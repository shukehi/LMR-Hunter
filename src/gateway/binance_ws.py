"""
Binance Futures WebSocket 连接层。

订阅四个流：
  - !forceOrder@arr          强平事件流（全市场）
  - btcusdt@aggTrade         BTC 聚合成交流
  - btcusdt@depth20@100ms    BTC 盘口深度（20 档，100ms 更新）
  - btcusdt@kline_1m         BTC 1 分钟 K 线

设计原则：
  - 单一 combined stream 连接，减少握手开销
  - 指数退避重连，最大间隔 60s
  - 接收到消息后立即记录本地时间戳，供延迟分析使用
  - 解析错误只记录日志，不中断连接
  - 延迟样本保留最近 10000 条，用于 P50/P95/P99 统计
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable

import orjson
import websockets
from websockets.exceptions import ConnectionClosed

from src.gateway.models import Depth, Kline, Liquidation, Trade
from src.utils.logger import setup_logger

logger = setup_logger("gateway")

# Binance Futures combined stream 端点
_WS_BASE = "wss://fstream.binance.com/stream"
_STREAMS = [
    "!forceOrder@arr",
    "btcusdt@aggTrade",
    "btcusdt@depth20@100ms",
    "btcusdt@kline_1m",
]
_WS_URL = f"{_WS_BASE}?streams=" + "/".join(_STREAMS)

# 重连参数
_BACKOFF_INIT   = 1.0
_BACKOFF_MAX    = 60.0
_BACKOFF_FACTOR = 2.0

# 延迟样本窗口大小（用于 P50/P95/P99）
_LATENCY_WINDOW = 10_000

# 回调类型
OnLiquidation = Callable[[Liquidation], Awaitable[None]]
OnTrade       = Callable[[Trade],       Awaitable[None]]
OnDepth       = Callable[[Depth],       Awaitable[None]]
OnKline       = Callable[[Kline],       Awaitable[None]]
OnEvent       = Callable[[],            Awaitable[None]]   # 无参事件（断线/重连）


class BinanceGateway:
    """
    持久化 WebSocket 连接，自动重连，将原始消息解析为强类型模型
    并调用注册的回调函数。
    """

    def __init__(
        self,
        on_liquidation: OnLiquidation,
        on_trade:       OnTrade,
        on_depth:       OnDepth,
        on_kline:       OnKline,
        on_disconnect:  OnEvent | None = None,
        on_reconnect:   OnEvent | None = None,
    ) -> None:
        self._on_liquidation = on_liquidation
        self._on_trade       = on_trade
        self._on_depth       = on_depth
        self._on_kline       = on_kline
        self._on_disconnect  = on_disconnect
        self._on_reconnect   = on_reconnect
        self._running        = False

        # 消息计数
        self.stats: dict[str, int] = {
            "msg_total":    0,
            "liquidations": 0,
            "trades":       0,
            "depths":       0,
            "klines":       0,
            "parse_errors": 0,
            "reconnects":   0,
        }

        # 延迟样本（ms），用于定期计算分位数
        self._latency_samples: deque[float] = deque(maxlen=_LATENCY_WINDOW)

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """启动网关，永久运行直到调用 stop()。"""
        self._running = True
        backoff = _BACKOFF_INIT

        while self._running:
            try:
                logger.info("连接 Binance WebSocket (%d 个流)", len(_STREAMS))
                async with websockets.connect(
                    _WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    backoff = _BACKOFF_INIT
                    logger.info("WebSocket 已连接，开始接收数据")
                    if self._on_reconnect and self.stats["reconnects"] > 0:
                        await self._on_reconnect()
                    await self._listen(ws)

            except ConnectionClosed as e:
                logger.warning("WebSocket 断线: %s", e)
                if self._on_disconnect:
                    await self._on_disconnect()
            except OSError as e:
                logger.error("网络错误: %s", e)
                if self._on_disconnect:
                    await self._on_disconnect()
            except Exception as e:  # noqa: BLE001
                logger.error("未预期错误: %s", e, exc_info=True)
                if self._on_disconnect:
                    await self._on_disconnect()

            if not self._running:
                break

            self.stats["reconnects"] += 1
            logger.info("%.1fs 后重连（第 %d 次）...", backoff, self.stats["reconnects"])
            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)

    async def stop(self) -> None:
        self._running = False
        logger.info("网关已停止")

    def latency_percentiles(self) -> dict[str, float | None]:
        """返回最近样本的延迟分位数（ms）。"""
        if not self._latency_samples:
            return {"p50": None, "p95": None, "p99": None}
        sorted_samples = sorted(self._latency_samples)
        n = len(sorted_samples)

        def pct(p: float) -> float:
            idx = max(0, int(n * p / 100) - 1)
            return sorted_samples[idx]

        return {
            "p50": round(pct(50), 1),
            "p95": round(pct(95), 1),
            "p99": round(pct(99), 1),
        }

    # ── 内部消息循环 ──────────────────────────────────────────────────────────

    async def _listen(self, ws) -> None:
        async for raw in ws:
            recv_ts = int(time.time() * 1000)
            self.stats["msg_total"] += 1

            try:
                msg    = orjson.loads(raw)
                stream: str  = msg.get("stream", "")
                data:   dict = msg.get("data", {})

                if stream == "!forceOrder@arr":
                    await self._handle_liquidation(data, recv_ts)
                elif stream.endswith("@aggTrade"):
                    await self._handle_trade(data, recv_ts)
                elif "@depth" in stream:
                    await self._handle_depth(data, recv_ts)
                elif "@kline_" in stream:
                    await self._handle_kline(data, recv_ts)

            except Exception as e:  # noqa: BLE001
                self.stats["parse_errors"] += 1
                logger.debug("解析错误: %s | raw=%s", e, raw[:200])

    # ── 消息处理器 ────────────────────────────────────────────────────────────

    async def _handle_liquidation(self, data: dict, recv_ts: int) -> None:
        liq = Liquidation.from_raw(data, recv_ts)
        latency = recv_ts - liq.event_ts
        self._latency_samples.append(latency)
        self.stats["liquidations"] += 1
        logger.info(
            "[强平] %s %s %.4f BTC @ %.2f USDT = %.0f USDT | 延迟 %dms",
            liq.symbol, liq.side, liq.qty, liq.price, liq.notional, latency,
        )
        await self._on_liquidation(liq)

    async def _handle_trade(self, data: dict, recv_ts: int) -> None:
        trade = Trade.from_raw(data, recv_ts)
        latency = recv_ts - trade.trade_ts
        self._latency_samples.append(latency)
        self.stats["trades"] += 1
        await self._on_trade(trade)

    async def _handle_depth(self, data: dict, recv_ts: int) -> None:
        depth = Depth.from_raw(data, recv_ts)
        latency = recv_ts - depth.event_ts
        self._latency_samples.append(latency)
        self.stats["depths"] += 1
        await self._on_depth(depth)

    async def _handle_kline(self, data: dict, recv_ts: int) -> None:
        kline = Kline.from_raw(data, recv_ts)
        self.stats["klines"] += 1
        if kline.is_closed:
            logger.debug(
                "[K线] %s 收盘 @ %.2f | vol=%.4f",
                kline.symbol, kline.close, kline.volume,
            )
        await self._on_kline(kline)
