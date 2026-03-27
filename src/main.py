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

LIQ_SIGNAL_SYMBOL = os.getenv("LIQ_SIGNAL_SYMBOL", SYMBOL)
LIQ_SIGNAL_SIDE   = os.getenv("LIQ_SIGNAL_SIDE", "SELL")

# 告警配置（可选）
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
LATENCY_P95_ALERT_MS = float(os.getenv("LATENCY_P95_ALERT_MS", "400"))

logger = setup_logger("main", level=LOG_LEVEL)


# ── 全局状态 ──────────────────────────────────────────────────────────────────

_writer:  DatabaseWriter    | None = None
_calc:    FeatureCalculator | None = None
_gateway: BinanceGateway    | None = None
_alerter: AlertManager      | None = None


# ── 回调函数 ──────────────────────────────────────────────────────────────────

async def on_liquidation(liq: Liquidation) -> None:
    """强平事件：写入 DB；仅 BTCUSDT SELL 方向计入特征窗口。"""
    assert _writer is not None and _calc is not None

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
    assert _writer is not None
    _writer.enqueue_trade(trade)


async def on_depth(depth: Depth) -> None:
    assert _calc is not None
    # 阶段 2：传入交易所事件时间戳，供偏离率计算时校验盘口新鲜度
    _calc.update_mid_price(depth.mid_price, depth.event_ts)


async def on_kline(kline: Kline) -> None:
    assert _writer is not None and _calc is not None
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

        # DEGRADED 状态触发告警
        if _writer.is_degraded and _alerter:
            from src.monitor.alerts import AlertLevel
            await _alerter._send(
                key="storage_degraded",
                level=AlertLevel.CRITICAL,
                title="存储层 DEGRADED",
                body=(
                    f"批量写入持续失败，已丢弃 {intg['discarded']} 条成交样本。"
                    f"研究数据已不完整，请检查磁盘和 SQLite 文件。"
                ),
            )

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


# ── 主函数 ────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _writer, _calc, _gateway, _alerter

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

    # 3. 通过 REST 拉取历史 K 线，预热 VWAP
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

    # 4. 初始化网关（含断线/重连钩子）
    _gateway = BinanceGateway(
        on_liquidation=on_liquidation,
        on_trade=on_trade,
        on_depth=on_depth,
        on_kline=on_kline,
        on_disconnect=on_gateway_disconnect,
        on_reconnect=on_gateway_reconnect,
    )

    # 5. 注册优雅退出信号
    loop = asyncio.get_running_loop()

    async def _shutdown(sig_name: str) -> None:
        logger.info("收到信号 %s，准备退出...", sig_name)
        if _gateway:
            await _gateway.stop()
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

    # 6. 启动所有协程
    logger.info("所有组件已就绪，开始 Observe Mode...")
    await asyncio.gather(
        _gateway.start(),
        trade_flusher(),
        stats_reporter(),
        return_exceptions=True,
    )

    logger.info("LMR-Hunter 已退出")


if __name__ == "__main__":
    uvloop.run(main())
