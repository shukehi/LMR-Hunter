"""
异步写入接口。

设计原则：
  - 强平事件（低频、高价值）：直接写入，立即 commit
  - 成交数据（高频）：放入内存队列，由外部定时 flush 批量写入
  - K 线（低频）：UPSERT，用于 K 线更新期间覆盖未收盘 K 线
  - 心跳/风控：直接写入

数据完整性保证（阶段 1 优化）：
  - flush_trades() 先复制 batch、成功后才从队列移除，失败不丢样本
  - 写入失败自动重试（指数退避，最多 3 次）
  - 全部重试失败后写入隔离文件，样本不静默丢失
  - 完整性计数器：enqueued / written / retried / discarded
  - 连续失败超阈值时写入 risk_events，状态升级为 DEGRADED
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from pathlib import Path

import aiosqlite

from src.gateway.models import Kline, Liquidation, Trade
from src.utils.logger import setup_logger

logger = setup_logger("storage.writer")

# 批量写入重试配置
_MAX_RETRIES    = 3
_RETRY_BACKOFF  = [0.5, 1.0]         # 每次重试前等待秒数（_MAX_RETRIES=3 次尝试，2 次退避）
_FAIL_THRESHOLD = 3                   # 连续失败超过此次数 → DEGRADED

# 隔离文件路径（写库彻底失败时的最后防线）
_ISOLATION_DIR = Path("/opt/lmr-hunter/data/isolation")


class DatabaseWriter:
    """封装所有对 SQLite 的写入操作，提供数据完整性保证。"""

    def __init__(
        self,
        conn: aiosqlite.Connection,
        trade_batch_size: int = 200,
        isolation_dir: Path = _ISOLATION_DIR,
    ) -> None:
        self._conn            = conn
        # maxlen 防止 DEGRADED 期间内存无限增长（BTC aggTrades 峰值 ~2000/min）
        # 10_000 条约 10MB，提供 5 分钟缓冲；超出时最旧数据被静默丢弃
        _QUEUE_MAXLEN         = 10_000
        self._trade_queue:    deque[Trade] = deque(maxlen=_QUEUE_MAXLEN)
        self._trade_batch_size = trade_batch_size
        self._isolation_dir   = isolation_dir

        # ── 完整性计数器 ──────────────────────────────────────────────────────
        self._integrity = {
            "enqueued":    0,   # 入队总笔数
            "written":     0,   # 成功写库笔数
            "retried":     0,   # 重试次数（一次 flush 多次重试算多次）
            "discarded":   0,   # 写入隔离文件的笔数（可追溯）
            "lost":        0,   # 隔离文件也失败的笔数（永久丢失）
        }

        # ── 原有写入计数 ─────────────────────────────────────────────────────
        self._write_counts = {
            "liquidations": 0,
            "trades":       0,
            "klines":       0,
            "signals":      0,
            "heartbeats":   0,
            "risk_events":  0,
            "errors":       0,
        }

        # 连续失败计数（用于触发 DEGRADED）
        self._consecutive_flush_failures = 0
        self._system_degraded = False

    # ── 属性 ──────────────────────────────────────────────────────────────────

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._write_counts)

    @property
    def integrity(self) -> dict[str, int]:
        return dict(self._integrity)

    @property
    def trade_queue_size(self) -> int:
        return len(self._trade_queue)

    @property
    def is_degraded(self) -> bool:
        return self._system_degraded

    def integrity_ok(self) -> bool:
        """True = 没有样本被丢弃。Shadow Mode 前置门槛之一。"""
        return self._integrity["discarded"] == 0

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

    # ── 成交数据（批量 + 完整性保证）────────────────────────────────────────────

    def enqueue_trade(self, trade: Trade) -> None:
        """将成交放入内存队列，递增入队计数。

        队列设有 maxlen 上限（10_000）防止 OOM。超出时最旧数据被 deque 自动丢弃，
        此时 enqueued > written + discarded，差值即为被回压丢弃的数量。
        """
        was_full = self._trade_queue.maxlen is not None and len(self._trade_queue) >= self._trade_queue.maxlen
        self._trade_queue.append(trade)
        self._integrity["enqueued"] += 1
        if was_full:
            # deque 的 maxlen 机制自动丢弃了最旧的元素
            self._integrity["discarded"] += 1
            logger.warning("成交队列已满（maxlen=%d），最旧样本被丢弃", self._trade_queue.maxlen)

    async def flush_trades(self) -> int:
        """
        批量写入成交数据，返回本次成功写入的条数。

        完整性保证：
          1. 先复制 batch，不立即从队列移除
          2. 写入成功后才移除队列头部对应数量
          3. 失败时重试（指数退避，最多 3 次）
          4. 全部重试失败：写入隔离文件，从队列移除（记为 discarded）
          5. 连续失败超过阈值：标记系统 DEGRADED，写 risk_events
        """
        if not self._trade_queue:
            return 0

        # 1. 复制 batch（不出队）
        batch_size = min(len(self._trade_queue), self._trade_batch_size)
        batch_trades = [self._trade_queue[i] for i in range(batch_size)]
        batch_rows = [
            (t.recv_ts, t.trade_ts, t.symbol, t.price, t.qty, int(t.is_buyer_maker))
            for t in batch_trades
        ]

        # 2. 带重试的写入
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                await self._conn.executemany(
                    """
                    INSERT INTO raw_trades
                        (recv_ts, trade_ts, symbol, price, qty, is_buyer_maker)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    batch_rows,
                )
                await self._conn.commit()

                # 3. 写入成功 → 从队列移除
                for _ in range(batch_size):
                    self._trade_queue.popleft()

                self._write_counts["trades"]  += batch_size
                self._integrity["written"]    += batch_size
                self._consecutive_flush_failures = 0
                if self._system_degraded:
                    logger.info("写库恢复正常，清除 DEGRADED 状态")
                    self._system_degraded = False
                return batch_size

            except Exception as e:
                last_exc = e
                if attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BACKOFF[attempt]
                    self._integrity["retried"] += 1
                    logger.warning(
                        "批量写入成交失败（第 %d/%d 次），%.1fs 后重试: %s",
                        attempt + 1, _MAX_RETRIES, wait, e,
                    )
                    await asyncio.sleep(wait)

        # 4. 全部重试失败 → 写隔离文件，从队列移除（discarded）
        self._write_counts["errors"] += 1
        self._integrity["discarded"] += batch_size
        self._consecutive_flush_failures += 1
        logger.error(
            "批量写入彻底失败（%d 次重试），%d 条成交写入隔离文件: %s",
            _MAX_RETRIES, batch_size, last_exc,
        )
        iso_ok = await self._write_isolation(batch_trades)

        # 从队列移除（无论隔离是否成功，均移除以防无限循环）
        # iso_ok=False 时数据永久丢失，_integrity["lost"] 已累计，上方 logger.critical 已记录
        for _ in range(batch_size):
            self._trade_queue.popleft()

        # 5. 连续失败超阈值 → DEGRADED
        if self._consecutive_flush_failures >= _FAIL_THRESHOLD:
            await self._mark_degraded()

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

    # ── 内部工具 ──────────────────────────────────────────────────────────────

    async def _write_isolation(self, trades: list[Trade]) -> bool:
        """
        将无法写入 SQLite 的成交数据写入 JSONL 隔离文件。
        文件名包含时间戳，每次调用生成独立文件，避免并发写入冲突。

        返回 True = 隔离成功；False = 隔离也失败（数据永久丢失）。

        注意：文件写入使用 asyncio.to_thread() 避免阻塞事件循环。
        """
        try:
            ts_str   = time.strftime("%Y%m%d_%H%M%S")
            iso_path = self._isolation_dir / f"trades_{ts_str}_{len(trades)}.jsonl"
            content = "\n".join(
                json.dumps({
                    "recv_ts":        t.recv_ts,
                    "trade_ts":       t.trade_ts,
                    "symbol":         t.symbol,
                    "price":          t.price,
                    "qty":            t.qty,
                    "is_buyer_maker": t.is_buyer_maker,
                })
                for t in trades
            )

            def _sync_write() -> None:
                self._isolation_dir.mkdir(parents=True, exist_ok=True)
                iso_path.write_text(content, encoding="utf-8")

            await asyncio.to_thread(_sync_write)
            logger.warning("隔离文件已写入: %s (%d 条)", iso_path, len(trades))
            return True
        except Exception as e:
            # 隔离文件也写不了 → 样本永久丢失，记录 CRITICAL
            self._integrity["lost"] = self._integrity.get("lost", 0) + len(trades)
            logger.critical(
                "隔离文件写入失败！%d 条成交数据永久丢失: %s",
                len(trades), e,
            )
            return False

    async def _mark_degraded(self) -> None:
        """将系统状态标记为 DEGRADED，写入 risk_events。"""
        if self._system_degraded:
            return
        self._system_degraded = True
        msg = (
            f"批量写入连续失败 {self._consecutive_flush_failures} 次，"
            f"已丢弃 {self._integrity['discarded']} 条成交样本。"
            f"系统进入 DEGRADED 状态。"
        )
        logger.critical("DEGRADED: %s", msg)
        await self.write_risk_event(
            component="storage.writer",
            event_type="WRITE_FAILURE_PERSISTENT",
            severity="CRITICAL",
            message=msg,
            context=json.dumps(self._integrity),
        )
