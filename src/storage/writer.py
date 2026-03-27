"""
异步写入接口。

设计原则：
  - 强平事件（低频、高价值）：直接写入，立即 commit
  - 成交数据（高频）：放入内存队列，由外部定时 flush 批量写入
  - K 线（低频）：UPSERT，用于 K 线更新期间覆盖未收盘 K 线
  - 心跳/风控：直接写入
  - 所有写入失败记录日志，不抛出异常（避免中断主循环）
"""
from __future__ import annotations

import asyncio
import time
from collections import deque

import aiosqlite

from src.gateway.models import Kline, Liquidation, Trade
from src.utils.logger import setup_logger

logger = setup_logger("storage.writer")


class DatabaseWriter:
    """封装所有对 SQLite 的写入操作。"""

    def __init__(self, conn: aiosqlite.Connection, trade_batch_size: int = 200) -> None:
        self._conn = conn
        self._trade_queue: deque[Trade] = deque()
        self._trade_batch_size = trade_batch_size
        self._write_counts = {
            "liquidations": 0,
            "trades": 0,
            "klines": 0,
            "signals": 0,
            "heartbeats": 0,
            "risk_events": 0,
            "errors": 0,
        }

    # ── 属性 ──────────────────────────────────────────────────────────────────

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._write_counts)

    @property
    def trade_queue_size(self) -> int:
        return len(self._trade_queue)

    # ── 强平事件 ──────────────────────────────────────────────────────────────

    async def write_liquidation(self, liq: Liquidation) -> None:
        try:
            await self._conn.execute(
                """
                INSERT INTO raw_liquidations
                    (recv_ts, event_ts, symbol, side, qty, price, notional, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (liq.recv_ts, liq.event_ts, liq.symbol, liq.side,
                 liq.qty, liq.price, liq.notional, liq.status),
            )
            await self._conn.commit()
            self._write_counts["liquidations"] += 1
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入强平事件失败: %s | %s", e, liq)

    # ── 成交数据（批量）─────────────────────────────────────────────────────────

    def enqueue_trade(self, trade: Trade) -> None:
        """将成交放入内存队列，不立即写库。"""
        self._trade_queue.append(trade)

    async def flush_trades(self) -> int:
        """将队列中的成交批量写入数据库，返回写入条数。"""
        if not self._trade_queue:
            return 0

        batch = []
        while self._trade_queue:
            t = self._trade_queue.popleft()
            batch.append((
                t.recv_ts, t.trade_ts, t.symbol,
                t.price, t.qty, int(t.is_buyer_maker),
            ))
            if len(batch) >= self._trade_batch_size:
                break

        try:
            await self._conn.executemany(
                """
                INSERT INTO raw_trades
                    (recv_ts, trade_ts, symbol, price, qty, is_buyer_maker)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            await self._conn.commit()
            self._write_counts["trades"] += len(batch)
            return len(batch)
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("批量写入成交失败: %s | batch_size=%d", e, len(batch))
            return 0

    # ── K 线 ──────────────────────────────────────────────────────────────────

    async def write_kline(self, kline: Kline) -> None:
        """UPSERT K 线（未收盘 K 线会被反复更新）。"""
        try:
            await self._conn.execute(
                """
                INSERT INTO raw_klines
                    (open_ts, close_ts, symbol, open, high, low, close, volume, is_closed, recv_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, open_ts) DO UPDATE SET
                    close_ts  = excluded.close_ts,
                    high      = excluded.high,
                    low       = excluded.low,
                    close     = excluded.close,
                    volume    = excluded.volume,
                    is_closed = excluded.is_closed,
                    recv_ts   = excluded.recv_ts
                """,
                (kline.open_ts, kline.close_ts, kline.symbol,
                 kline.open, kline.high, kline.low, kline.close,
                 kline.volume, int(kline.is_closed), kline.recv_ts),
            )
            await self._conn.commit()
            self._write_counts["klines"] += 1
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入 K 线失败: %s | %s", e, kline)

    async def write_klines_batch(self, klines: list[Kline]) -> None:
        """批量写入 K 线（用于 REST 引导阶段）。"""
        if not klines:
            return
        try:
            rows = [
                (k.open_ts, k.close_ts, k.symbol,
                 k.open, k.high, k.low, k.close,
                 k.volume, int(k.is_closed), k.recv_ts)
                for k in klines
            ]
            await self._conn.executemany(
                """
                INSERT INTO raw_klines
                    (open_ts, close_ts, symbol, open, high, low, close, volume, is_closed, recv_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, open_ts) DO UPDATE SET
                    close_ts  = excluded.close_ts,
                    high      = excluded.high,
                    low       = excluded.low,
                    close     = excluded.close,
                    volume    = excluded.volume,
                    is_closed = excluded.is_closed,
                    recv_ts   = excluded.recv_ts
                """,
                rows,
            )
            await self._conn.commit()
            self._write_counts["klines"] += len(klines)
            logger.info("引导写入 %d 根 K 线完成", len(klines))
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("批量写入 K 线失败: %s", e)

    # ── 心跳 ──────────────────────────────────────────────────────────────────

    async def write_heartbeat(
        self,
        component: str,
        status: str,
        msg_total: int,
        liq_count: int,
        trade_count: int,
        latency_p50: float | None,
        latency_p95: float | None,
        latency_p99: float | None,
        reconnects: int,
        notes: str | None = None,
    ) -> None:
        try:
            await self._conn.execute(
                """
                INSERT INTO service_heartbeats
                    (ts, component, status, msg_total, liq_count, trade_count,
                     latency_p50, latency_p95, latency_p99, reconnects, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(time.time() * 1000), component, status,
                 msg_total, liq_count, trade_count,
                 latency_p50, latency_p95, latency_p99, reconnects, notes),
            )
            await self._conn.commit()
            self._write_counts["heartbeats"] += 1
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入心跳失败: %s", e)

    # ── 风控事件 ──────────────────────────────────────────────────────────────

    async def write_risk_event(
        self,
        component: str,
        event_type: str,
        severity: str,
        message: str | None = None,
        context: str | None = None,
    ) -> None:
        try:
            await self._conn.execute(
                """
                INSERT INTO risk_events
                    (ts, component, event_type, severity, message, context)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(time.time() * 1000), component, event_type,
                 severity, message, context),
            )
            await self._conn.commit()
            self._write_counts["risk_events"] += 1
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入风控事件失败: %s", e)

    # ── 信号 ──────────────────────────────────────────────────────────────────

    async def write_signal(
        self,
        ts: int,
        symbol: str,
        liq_notional_window: float,
        liq_accel_ratio: float | None,
        vwap_15m: float | None,
        mid_price: float,
        deviation_bps: float | None,
        signal_fired: bool = False,
        entry_price: float | None = None,
        notes: str | None = None,
    ) -> int:
        """写入信号记录，返回新插入的 id。"""
        try:
            cursor = await self._conn.execute(
                """
                INSERT INTO signals
                    (ts, symbol, liq_notional_window, liq_accel_ratio,
                     vwap_15m, mid_price, deviation_bps, signal_fired,
                     entry_price, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, symbol, liq_notional_window, liq_accel_ratio,
                 vwap_15m, mid_price, deviation_bps,
                 int(signal_fired), entry_price, notes),
            )
            await self._conn.commit()
            self._write_counts["signals"] += 1
            return cursor.lastrowid or 0
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入信号失败: %s", e)
            return 0
