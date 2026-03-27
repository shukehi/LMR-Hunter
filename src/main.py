"""
LMR-Hunter 主入口。

当前阶段：Observe Mode

数据流：
    Binance WS → BinanceGateway
                   ├─ on_liquidation → FeatureCalculator + DatabaseWriter
                   ├─ on_trade       → DatabaseWriter（批量）
                   ├─ on_depth       → FeatureCalculator（更新中间价）
                   └─ on_kline       → FeatureCalculator + DatabaseWriter

后台任务：
    - trade_flusher    每 TRADE_FLUSH_INTERVAL_SEC 秒排空成交队列
    - stats_reporter   每 60 秒打印统计摘要 + 写入心跳 + 运行告警检测
    - outcome_processor 每 OUTCOME_CHECK_SEC 秒回填 episode outcome
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import time

import uvloop
from dotenv import load_dotenv

from src.features.calculator import FeatureCalculator
from src.features.episode import EpisodeBuilder
from src.features.outcome import compute_outcome
from src.gateway.binance_ws import BinanceGateway
from src.gateway.models import Depth, Kline, Liquidation, Trade
from src.gateway.rest_client import fetch_klines
from src.monitor.alerts import AlertManager
from src.storage import DatabaseWriter, init_db
from src.utils.logger import setup_logger

# ── 配置加载 ──────────────────────────────────────────────────────────────────

load_dotenv("/opt/lmr-hunter/config/.env")

LOG_LEVEL      = os.getenv("LOG_LEVEL", "INFO")
DB_PATH        = os.getenv("DB_PATH", "/opt/lmr-hunter/data/lmr.db")
SYMBOL         = os.getenv("SYMBOL", "BTCUSDT")
LIQ_WINDOW_SEC = int(os.getenv("LIQ_WINDOW_SEC", "5"))

# Episode 模型配置
EPISODE_GAP_SEC              = int(os.getenv("EPISODE_GAP_SEC", "30"))
EPISODE_NOISE_THRESHOLD_USDT = float(os.getenv("EPISODE_NOISE_THRESHOLD_USDT", "50000"))
EPISODE_MAX_DURATION_SEC     = int(os.getenv("EPISODE_MAX_DURATION_SEC", "300"))

# Outcome 计算配置
OUTCOME_WINDOW_MS = 15 * 60_000
OUTCOME_CHECK_SEC = int(os.getenv("OUTCOME_CHECK_SEC", "60"))

# 成交队列与吞吐配置（阶段 A 运行态优化）
TRADE_BATCH_SIZE             = int(os.getenv("TRADE_BATCH_SIZE", "2000"))
TRADE_FLUSH_INTERVAL_SEC     = float(os.getenv("TRADE_FLUSH_INTERVAL_SEC", "1"))
TRADE_QUEUE_MAXLEN           = int(os.getenv("TRADE_QUEUE_MAXLEN", "50000"))
TRADE_FLUSH_MAX_BATCHES_PER_TICK = int(os.getenv("TRADE_FLUSH_MAX_BATCHES_PER_TICK", "20"))

LIQ_SIGNAL_SYMBOL = os.getenv("LIQ_SIGNAL_SYMBOL", SYMBOL)
LIQ_SIGNAL_SIDE   = os.getenv("LIQ_SIGNAL_SIDE", "SELL")

# 告警配置（可选）
TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID")
LATENCY_P95_ALERT_MS = float(os.getenv("LATENCY_P95_ALERT_MS", "400"))

logger = setup_logger("main", level=LOG_LEVEL)


# ── 全局状态 ──────────────────────────────────────────────────────────────────

_writer:          DatabaseWriter    | None = None
_calc:            FeatureCalculator | None = None
_gateway:         BinanceGateway    | None = None
_alerter:         AlertManager      | None = None
_episode_builder: EpisodeBuilder    | None = None


# ── 回调函数 ──────────────────────────────────────────────────────────────────

async def on_liquidation(liq: Liquidation) -> None:
    """强平事件：写入 DB；仅 BTCUSDT SELL 方向计入特征窗口与 episode 模型。"""
    if _writer is None or _calc is None:
        logger.error("on_liquidation: 组件未初始化，跳过事件 %s", liq)
        return

    await _writer.write_liquidation(liq)

    is_signal_liq = (liq.symbol == LIQ_SIGNAL_SYMBOL and liq.side == LIQ_SIGNAL_SIDE)
    if is_signal_liq:
        _calc.on_liquidation(liq)

    if not is_signal_liq:
        return

    snap = _calc.snapshot_at(liq.event_ts)
    logger.info(
        "[BTC强平] %.0f USDT | 窗口=%.0f | 加速率=%s | VWAP=%s | 价格=%s | 偏离=%s bps | 盘口延迟=%s",
        liq.notional,
        snap.liq_notional_window,
        f"{snap.liq_accel_ratio:.3f}" if snap.liq_accel_ratio is not None else "N/A",
        f"{snap.vwap_15m:.2f}"        if snap.vwap_15m is not None else "冷启动",
        f"{snap.mid_price:.2f}"       if snap.mid_price is not None else "N/A",
        f"{snap.deviation_bps:.1f}"   if snap.deviation_bps is not None else "N/A",
        f"{snap.depth_staleness_ms}ms" if snap.depth_staleness_ms is not None else "N/A",
    )

    if _episode_builder is not None:
        completed_ep = _episode_builder.feed(
            event_ts            = liq.event_ts,
            symbol              = liq.symbol,
            side                = liq.side,
            notional            = liq.notional,
            liq_notional_window = snap.liq_notional_window,
            liq_accel_ratio     = snap.liq_accel_ratio,
            mid_price           = snap.mid_price,
            deviation_bps       = snap.deviation_bps,
            vwap_15m            = snap.vwap_15m,
        )
        if completed_ep is not None:
            await _writer.write_episode(completed_ep)

    if snap.mid_price is not None:
        await _writer.write_signal(
            ts=snap.ts,
            symbol=LIQ_SIGNAL_SYMBOL,
            liq_notional_window=snap.liq_notional_window,
            liq_accel_ratio=snap.liq_accel_ratio,
            vwap_15m=snap.vwap_15m,
            mid_price=snap.mid_price,
            deviation_bps=snap.deviation_bps,
            signal_fired=False,
            notes="observe_mode_btc_sell",
        )


async def on_trade(trade: Trade) -> None:
    if _writer is None:
        return
    _writer.enqueue_trade(trade)


async def on_depth(depth: Depth) -> None:
    if _calc is None:
        return
    _calc.update_mid_price(depth.mid_price, depth.event_ts)


async def on_kline(kline: Kline) -> None:
    if _writer is None or _calc is None:
        return
    _calc.on_kline(kline)
    await _writer.write_kline(kline)


# ── 网关事件钩子（供告警使用）────────────────────────────────────────────────

async def on_gateway_disconnect() -> None:
    if _alerter:
        await _alerter.on_disconnect()


async def on_gateway_reconnect() -> None:
    if _alerter:
        await _alerter.on_reconnect()


# ── 后台任务 ──────────────────────────────────────────────────────────────────

async def trade_flusher(
    interval: float = TRADE_FLUSH_INTERVAL_SEC,
    max_batches: int = TRADE_FLUSH_MAX_BATCHES_PER_TICK,
) -> None:
    """
    每 interval 秒排空成交队列，单轮最多执行 max_batches 次 flush。

    若达到单轮上限后队列仍非空（QUEUE_BACKLOG），记录警告但不触发 DEGRADED：
    backlog 本身是回压信号，真正的样本损坏由 overflow_dropped/isolated/lost 反映。
    """
    last_backlog_warn_ts = 0.0

    while True:
        await asyncio.sleep(interval)
        if _writer is None:
            continue

        batches_done = 0
        while _writer.trade_queue_size > 0 and batches_done < max_batches:
            flushed = await _writer.flush_trades()
            batches_done += 1
            if flushed == 0:
                break   # 写入失败，停止本轮继续重试（避免死循环）

        # 达到单轮上限后队列仍有积压 → QUEUE_BACKLOG 警告（60s 防抖）
        if _writer.trade_queue_size > 0:
            now = time.monotonic()
            if now - last_backlog_warn_ts >= 60.0:
                last_backlog_warn_ts = now
                queue_size = _writer.trade_queue_size
                logger.warning(
                    "QUEUE_BACKLOG: 成交队列仍有 %d 条未写入"
                    "（已达单轮上限 %d 批 × %d 条 = %d 条/tick）",
                    queue_size, max_batches, TRADE_BATCH_SIZE,
                    max_batches * TRADE_BATCH_SIZE,
                )
                await _writer.write_risk_event(
                    component="storage.writer",
                    event_type="QUEUE_BACKLOG",
                    severity="WARN",
                    message=f"成交队列回压 {queue_size} 条，超过单轮排空能力",
                    context=json.dumps({
                        "trade_queue_size":      queue_size,
                        "max_batches_per_tick":  max_batches,
                        "trade_batch_size":      TRADE_BATCH_SIZE,
                    }),
                )


async def stats_reporter(interval: int = 60) -> None:
    """每 interval 秒：打印统计 + 写心跳 + 运行告警检测。"""
    while True:
        await asyncio.sleep(interval)
        if not _gateway or not _writer:
            continue

        s    = _gateway.stats
        lat  = _gateway.latency_percentiles()
        wc   = _writer.counts
        intg = _writer.integrity

        logger.info(
            "[统计] 消息=%d | 强平=%d | 成交=%d | K线=%d | 盘口=%d | "
            "解析错误=%d | 重连=%d | 写入错误=%d",
            s["msg_total"], s["liquidations"], s["trades"],
            s["klines"], s["depths"], s["parse_errors"],
            s["reconnects"], wc["errors"],
        )
        logger.info(
            "[延迟] P50=%.1fms | P95=%.1fms | P99=%.1fms",
            lat["p50"] or 0, lat["p95"] or 0, lat["p99"] or 0,
        )
        logger.info(
            "[完整性] 入队=%d | 写库=%d | 重试=%d | "
            "溢出丢弃=%d | 隔离=%d | 永久丢失=%d | 队列积压=%d | 状态=%s",
            intg["enqueued"], intg["written"], intg["retried"],
            intg["overflow_dropped"], intg["isolated"], intg["lost"],
            _writer.trade_queue_size,
            "DEGRADED" if _writer.is_degraded else "OK",
        )

        if _writer.is_degraded and _alerter:
            await _alerter.on_storage_degraded(intg["discarded"])

        # 心跳 notes 包含研究可用性相关字段
        notes = json.dumps({
            "trade_queue_size": _writer.trade_queue_size,
            "overflow_dropped": intg["overflow_dropped"],
            "isolated":         intg["isolated"],
            "lost":             intg["lost"],
            "episode_stats":    _episode_builder.stats if _episode_builder else {},
        })

        status = "DEGRADED" if _writer.is_degraded else "OK"
        await _writer.write_heartbeat(
            component="gateway",
            status=status,
            msg_total=s["msg_total"],
            liq_count=s["liquidations"],
            trade_count=s["trades"],
            latency_p50=lat["p50"],
            latency_p95=lat["p95"],
            latency_p99=lat["p99"],
            reconnects=s["reconnects"],
            notes=notes,
        )

        if _alerter:
            await _alerter.check_all(s, lat, wc)


async def outcome_processor() -> None:
    """
    后台任务：每 OUTCOME_CHECK_SEC 秒检查一次，为已完成的 episode 回填 outcome 记录。
    """
    await asyncio.sleep(OUTCOME_CHECK_SEC)
    while True:
        try:
            if _writer is not None:
                pending = await _writer.get_episodes_pending_outcome(
                    min_age_ms=OUTCOME_WINDOW_MS
                )
                for ep in pending:
                    trades = await _writer.get_trades_after_episode(
                        symbol   = ep["symbol"],
                        start_ts = ep["end_event_ts"],
                        end_ts   = ep["end_event_ts"] + OUTCOME_WINDOW_MS,
                    )
                    outcome = compute_outcome(
                        episode_id     = ep["episode_id"],
                        end_event_ts   = ep["end_event_ts"],
                        entry_price    = ep["min_mid_price"],
                        pre_event_vwap = ep["pre_event_vwap"],
                        trades         = trades,
                    )
                    await _writer.write_episode_outcome(outcome)
        except Exception as e:
            logger.error("outcome_processor 错误: %s", e)
        await asyncio.sleep(OUTCOME_CHECK_SEC)


# ── 主函数 ────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _writer, _calc, _gateway, _alerter, _episode_builder

    logger.info("LMR-Hunter 启动 | 模式=observe | symbol=%s", SYMBOL)
    logger.info(
        "成交队列配置 | batch=%d | interval=%.1fs | maxlen=%d | max_batches=%d",
        TRADE_BATCH_SIZE, TRADE_FLUSH_INTERVAL_SEC,
        TRADE_QUEUE_MAXLEN, TRADE_FLUSH_MAX_BATCHES_PER_TICK,
    )

    # 1. 初始化数据库
    logger.info("初始化数据库: %s", DB_PATH)
    conn = await init_db(DB_PATH)
    _writer = DatabaseWriter(
        conn,
        trade_batch_size   = TRADE_BATCH_SIZE,
        trade_queue_maxlen = TRADE_QUEUE_MAXLEN,
    )

    # 2. 初始化告警管理器
    _alerter = AlertManager(
        telegram_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID,
        latency_p95_threshold_ms=LATENCY_P95_ALERT_MS,
        write_error_threshold=5,
        disconnect_timeout_sec=120,
    )

    # 3. 初始化 episode 构建器
    _episode_builder = EpisodeBuilder(
        gap_ms               = EPISODE_GAP_SEC * 1000,
        noise_threshold_usdt = EPISODE_NOISE_THRESHOLD_USDT,
        max_duration_ms      = EPISODE_MAX_DURATION_SEC * 1000,
    )
    logger.info(
        "EpisodeBuilder 已初始化 | gap=%ds | 噪声门槛=%.0f USDT | 最大持续=%ds",
        EPISODE_GAP_SEC, EPISODE_NOISE_THRESHOLD_USDT, EPISODE_MAX_DURATION_SEC,
    )

    # 4. 通过 REST 拉取历史 K 线，预热 VWAP
    logger.info("引导 K 线数据...")
    _calc = FeatureCalculator(liq_window_sec=LIQ_WINDOW_SEC)
    boot_klines = await fetch_klines(SYMBOL, limit=16)
    if boot_klines:
        for k in boot_klines:
            _calc.on_kline(k)
        await _writer.write_klines_batch(boot_klines)
        snap = _calc.snapshot()
        logger.info(
            "K 线引导完成: %d 根 | VWAP=%.2f | 就绪=%s",
            snap.kline_count, snap.vwap_15m or 0, snap.is_vwap_ready,
        )
    else:
        logger.warning("K 线引导失败，VWAP 将在收到 15 根 K 线后可用")

    # 5. 初始化网关
    _gateway = BinanceGateway(
        on_liquidation=on_liquidation,
        on_trade=on_trade,
        on_depth=on_depth,
        on_kline=on_kline,
        on_disconnect=on_gateway_disconnect,
        on_reconnect=on_gateway_reconnect,
    )

    # 6. 创建后台任务（显式管理，供停机时 cancel）
    loop = asyncio.get_running_loop()

    flusher_task  = asyncio.create_task(
        trade_flusher(TRADE_FLUSH_INTERVAL_SEC, TRADE_FLUSH_MAX_BATCHES_PER_TICK),
        name="trade_flusher",
    )
    reporter_task = asyncio.create_task(stats_reporter(), name="stats_reporter")
    outcome_task  = asyncio.create_task(outcome_processor(), name="outcome_processor")
    gateway_task  = asyncio.create_task(_gateway.start(), name="gateway")

    bg_tasks = [flusher_task, reporter_task, outcome_task]
    _shutting_down = False

    # 7. 停机处理（闭包捕获任务引用）
    async def _shutdown(sig_name: str) -> None:
        nonlocal _shutting_down
        if _shutting_down:
            return
        _shutting_down = True
        logger.info("收到信号 %s，开始有序退出...", sig_name)

        # 步骤 1：停止网关（主动关闭 websocket，不再接收新数据）
        if _gateway:
            await _gateway.stop()

        # 步骤 2：取消周期性后台任务
        for task in bg_tasks:
            task.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)
        logger.info("后台任务已取消")

        # 步骤 3：取消 gateway 任务（stop() 应已触发退出，此处兜底）
        gateway_task.cancel()
        try:
            await gateway_task
        except (asyncio.CancelledError, Exception):
            pass

        # 步骤 4：flush active episode
        if _episode_builder and _writer:
            last_ep = _episode_builder.flush()
            if last_ep is not None:
                await _writer.write_episode(last_ep)
                logger.info("退出前 flush episode: %s", last_ep.episode_id)

        # 步骤 5：drain trade queue
        if _writer:
            total_flushed = 0
            while _writer.trade_queue_size > 0:
                n = await _writer.flush_trades()
                total_flushed += n
                if n == 0:
                    break
            logger.info("退出前 flush %d 条成交（队列剩余=%d）",
                        total_flushed, _writer.trade_queue_size)

        # 步骤 6：关闭数据库
        if conn:
            await conn.close()
            logger.info("数据库连接已关闭")

        logger.info("LMR-Hunter 有序退出完成")

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(_shutdown(s.name)),
        )

    # 8. 等待所有任务完成（停机时任务逐一 cancel/return）
    logger.info("所有组件已就绪，开始 Observe Mode...")
    all_tasks   = [gateway_task, flusher_task, reporter_task, outcome_task]
    task_names  = ["gateway", "trade_flusher", "stats_reporter", "outcome_processor"]
    results     = await asyncio.gather(*all_tasks, return_exceptions=True)

    for name, result in zip(task_names, results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            logger.critical("协程 %s 异常退出: %s", name, result, exc_info=result)

    logger.info("LMR-Hunter 已退出")


if __name__ == "__main__":
    uvloop.run(main())
