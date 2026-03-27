"""
LMR-Hunter 主入口。

当前阶段：Observe Mode（阶段 1）

数据流：
    Binance WS → BinanceGateway
                   ├─ on_liquidation → FeatureCalculator + DatabaseWriter
                   ├─ on_trade       → DatabaseWriter（批量）
                   ├─ on_depth       → FeatureCalculator（更新中间价）
                   └─ on_kline       → FeatureCalculator + DatabaseWriter

后台任务：
    - trade_flusher   每 5 秒将成交队列批量写入 SQLite
    - stats_reporter  每 60 秒打印统计摘要 + 写入心跳 + 运行告警检测
"""
from __future__ import annotations

import asyncio
import os
import signal

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
EPISODE_GAP_SEC             = int(os.getenv("EPISODE_GAP_SEC", "30"))
EPISODE_NOISE_THRESHOLD_USDT = float(os.getenv("EPISODE_NOISE_THRESHOLD_USDT", "50000"))
EPISODE_MAX_DURATION_SEC    = int(os.getenv("EPISODE_MAX_DURATION_SEC", "300"))

# Outcome 计算配置
OUTCOME_WINDOW_MS  = 15 * 60_000                                    # 观测窗口：15 分钟
OUTCOME_CHECK_SEC  = int(os.getenv("OUTCOME_CHECK_SEC", "60"))      # 后台检查间隔

LIQ_SIGNAL_SYMBOL = os.getenv("LIQ_SIGNAL_SYMBOL", SYMBOL)
LIQ_SIGNAL_SIDE   = os.getenv("LIQ_SIGNAL_SIDE", "SELL")

# 告警配置（可选）
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
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

    # 阶段 2：以强平事件的交易所时间戳为锚点，确保窗口计算与事件时间对齐
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

    # 阶段 3：喂入 episode 构建器，若有完整 episode 刚关闭则持久化
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
    # 阶段 2：传入交易所事件时间戳，供偏离率计算时校验盘口新鲜度
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

async def trade_flusher(interval: float = 5.0) -> None:
    while True:
        await asyncio.sleep(interval)
        if _writer:
            flushed = await _writer.flush_trades()
            if flushed:
                logger.debug("成交批量写入 %d 条", flushed)


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
            "[完整性] 入队=%d | 写库=%d | 重试=%d | 丢弃=%d | 状态=%s",
            intg["enqueued"], intg["written"],
            intg["retried"], intg["discarded"],
            "DEGRADED" if _writer.is_degraded else "OK",
        )

        # DEGRADED 状态触发告警（通过 AlertManager 公开接口，附带 10 分钟防抖）
        if _writer.is_degraded and _alerter:
            await _alerter.on_storage_degraded(intg["discarded"])

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
        )

        # 告警检测
        if _alerter:
            await _alerter.check_all(s, lat, wc)


async def outcome_processor() -> None:
    """
    后台任务：每 OUTCOME_CHECK_SEC 秒检查一次，为已完成的 episode 回填 outcome 记录。

    Episode 结束后至少 15 分钟（OUTCOME_WINDOW_MS）才计算 outcome，
    以确保 raw_trades 表中已有完整的价格路径数据。
    """
    await asyncio.sleep(OUTCOME_CHECK_SEC)  # 首次延迟，避免启动时立即扫描
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

    # 1. 初始化数据库
    logger.info("初始化数据库: %s", DB_PATH)
    conn = await init_db(DB_PATH)
    _writer = DatabaseWriter(conn)

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

    # 5. 初始化网关（含断线/重连钩子）
    _gateway = BinanceGateway(
        on_liquidation=on_liquidation,
        on_trade=on_trade,
        on_depth=on_depth,
        on_kline=on_kline,
        on_disconnect=on_gateway_disconnect,
        on_reconnect=on_gateway_reconnect,
    )

    # 6. 注册优雅退出信号
    loop = asyncio.get_running_loop()

    async def _shutdown(sig_name: str) -> None:
        logger.info("收到信号 %s，准备退出...", sig_name)
        if _gateway:
            await _gateway.stop()
        # 关闭前将未完成的 episode 持久化（避免丢失最后一次冲击的研究样本）
        if _episode_builder and _writer:
            last_ep = _episode_builder.flush()
            if last_ep is not None:
                await _writer.write_episode(last_ep)
                logger.info("退出前 flush episode: %s", last_ep.episode_id)
        if _writer:
            total_flushed = 0
            while _writer.trade_queue_size > 0:
                n = await _writer.flush_trades()
                total_flushed += n
                if n == 0:
                    break
            logger.info("退出前 flush %d 条成交", total_flushed)
        if conn:
            await conn.close()
            logger.info("数据库连接已关闭")

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(_shutdown(s.name)),
        )

    # 7. 启动所有协程
    logger.info("所有组件已就绪，开始 Observe Mode...")
    results = await asyncio.gather(
        _gateway.start(),
        trade_flusher(),
        stats_reporter(),
        outcome_processor(),
        return_exceptions=True,
    )

    # 检查是否有协程异常退出（return_exceptions=True 会吞掉异常，需手动检查）
    task_names = ["gateway", "trade_flusher", "stats_reporter", "outcome_processor"]
    for name, result in zip(task_names, results):
        if isinstance(result, Exception):
            logger.critical("协程 %s 异常退出: %s", name, result, exc_info=result)

    logger.info("LMR-Hunter 已退出")


if __name__ == "__main__":
    uvloop.run(main())
