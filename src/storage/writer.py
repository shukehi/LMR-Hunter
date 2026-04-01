"""
异步写入接口。

设计原则：
  - 强平事件（低频、高价值）：直接写入，立即 commit
  - 成交数据（高频）：放入内存队列，由外部定时 flush 批量写入
  - K 线（低频）：UPSERT，用于 K 线更新期间覆盖未收盘 K 线
  - 心跳/风控：直接写入

数据完整性保证（阶段 A 运行态优化）：
  - flush_trades() 先复制 batch、成功后才从队列移除，失败不丢样本
  - 写入失败自动重试（指数退避，最多 3 次）
  - 全部重试失败后写入隔离文件（isolated），样本不静默丢失
  - 隔离文件也写失败 → 永久丢失（lost）
  - 队列超出 maxlen → 最旧样本被 deque 自动覆盖（overflow_dropped）
  - 完整性计数器：enqueued / written / retried / overflow_dropped / isolated / lost
    其中 discarded = overflow_dropped + isolated + lost（向后兼容只读字段）
  - 任何真实样本损坏（overflow_dropped/isolated/lost > 0）立即标记 DEGRADED
  - DEGRADED 在进程生命周期内不自动恢复
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from pathlib import Path

import aiosqlite

from src.features.episode import LiquidationEpisode
from src.features.outcome import EpisodeOutcome
from src.gateway.models import Depth, Kline, Liquidation, Trade
from src.utils.logger import setup_logger

logger = setup_logger("storage.writer")

# 批量写入重试配置
_MAX_RETRIES    = 3
_RETRY_BACKOFF  = [0.5, 1.0]         # 每次重试前等待秒数（_MAX_RETRIES=3 次尝试，2 次退避）
_FAIL_THRESHOLD = 3                   # 连续失败超过此次数 → 额外 DEGRADED 保障

# 隔离文件路径（写库彻底失败时的最后防线）
_ISOLATION_DIR = Path("/opt/lmr-hunter/data/isolation")


class DatabaseWriter:
    """封装所有对 SQLite 的写入操作，提供数据完整性保证。"""

    def __init__(
        self,
        conn: aiosqlite.Connection,
        trade_batch_size: int  = 2000,
        trade_queue_maxlen: int = 50_000,
        isolation_dir: Path    = _ISOLATION_DIR,
    ) -> None:
        self._conn               = conn
        self._trade_queue:       deque[Trade] = deque(maxlen=trade_queue_maxlen)
        self._trade_batch_size   = trade_batch_size
        self._isolation_dir      = isolation_dir
        self._last_depth_ts: int = 0   # 深度快照降采样：记录上次写入的 event_ts

        # ── 完整性计数器 ──────────────────────────────────────────────────────────
        # overflow_dropped : 队列溢出，最旧样本被 deque 自动覆盖（直接丢失）
        # isolated         : DB 写入彻底失败，已写入隔离文件（可追溯）
        # lost             : DB + 隔离文件均失败（永久丢失）
        # discarded        : 只读计算字段 = overflow_dropped + isolated + lost
        self._integrity: dict[str, int] = {
            "enqueued":         0,
            "written":          0,
            "retried":          0,
            "overflow_dropped": 0,
            "isolated":         0,
            "lost":             0,
        }

        # ── 原有写入计数 ─────────────────────────────────────────────────────────
        self._write_counts = {
            "liquidations": 0,
            "trades":       0,
            "klines":       0,
            "depths":       0,
            "signals":      0,
            "episodes":     0,
            "outcomes":     0,
            "heartbeats":   0,
            "risk_events":  0,
            "errors":       0,
        }

        # 连续失败计数（作为 DEGRADED 的额外触发保障）
        self._consecutive_flush_failures = 0
        self._system_degraded            = False
        # 首次溢出时由 enqueue_trade（同步）设置，由 flush_trades（异步）处理
        self._needs_overflow_risk_event  = False
        # BUG-5 修复：并发保护，防止 trade_flusher 与 _shutdown 同时执行 flush_trades
        # 导致 executemany 并发、重复写入和计数膨胀
        self._flushing: bool             = False

    # ── 属性 ──────────────────────────────────────────────────────────────────

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._write_counts)

    @property
    def integrity(self) -> dict[str, int]:
        """返回完整性计数字典，含向后兼容字段 discarded。"""
        d = dict(self._integrity)
        d["discarded"] = d["overflow_dropped"] + d["isolated"] + d["lost"]
        return d

    @property
    def trade_queue_size(self) -> int:
        return len(self._trade_queue)

    @property
    def is_degraded(self) -> bool:
        return self._system_degraded

    def integrity_ok(self) -> bool:
        """True = 没有任何真实样本损坏。Shadow Mode 前置门槛之一。"""
        i = self._integrity
        return i["overflow_dropped"] == 0 and i["isolated"] == 0 and i["lost"] == 0

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

        队列设有 maxlen 上限，超出时 deque 自动覆盖最旧样本（overflow_dropped）。
        首次溢出时同步标记 _needs_overflow_risk_event，由下一次 flush_trades 写入
        DEGRADED 风控事件（避免同步函数中调用 async 方法）。
        """
        was_full = (
            self._trade_queue.maxlen is not None
            and len(self._trade_queue) >= self._trade_queue.maxlen
        )
        self._trade_queue.append(trade)
        self._integrity["enqueued"] += 1

        if was_full:
            self._integrity["overflow_dropped"] += 1
            if not self._system_degraded and not self._needs_overflow_risk_event:
                # 首次溢出：设置标志，等下一次 flush_trades 异步写入 risk_event
                self._needs_overflow_risk_event = True
                logger.critical(
                    "成交队列溢出开始！maxlen=%d | overflow_dropped=%d | "
                    "系统将在下次 flush 时进入 DEGRADED",
                    self._trade_queue.maxlen,
                    self._integrity["overflow_dropped"],
                )
            else:
                logger.warning(
                    "成交队列持续溢出，累计 overflow_dropped=%d",
                    self._integrity["overflow_dropped"],
                )

    async def flush_trades(self) -> int:
        """
        批量写入成交数据，返回本次成功写入的条数。

        完整性保证：
          1. 检查并处理首次溢出风控事件
          2. 先复制 batch，不立即从队列移除
          3. 写入成功后才移除队列头部对应数量
          4. 失败时重试（指数退避，最多 3 次）
          5. 全部重试失败：写入隔离文件（isolated），从队列移除
          6. 隔离文件也失败：永久丢失（lost），DEGRADED
          7. 任何真实样本损坏 → 立即进入 DEGRADED（进程内不恢复）
        """
        # BUG-5 修复：并发保护（trade_flusher 每秒触发 vs _shutdown 的 drain 循环）
        if self._flushing:
            return 0
        self._flushing = True
        try:
            return await self._flush_trades_inner()
        finally:
            self._flushing = False

    async def _flush_trades_inner(self) -> int:
        """flush_trades 的实际逻辑，由持有 _flushing 锁的调用者执行。"""
        # 1. 处理首次溢出的风控事件（同步检测、异步写入）
        if self._needs_overflow_risk_event:
            self._needs_overflow_risk_event = False
            await self._ensure_degraded(
                f"成交队列溢出（maxlen={self._trade_queue.maxlen}，"
                f"overflow_dropped={self._integrity['overflow_dropped']}），"
                f"最旧样本已丢失，系统进入 DEGRADED",
                context=json.dumps(self.integrity),
            )

        if not self._trade_queue:
            return 0

        # 2. 复制 batch（不出队）
        batch_size   = min(len(self._trade_queue), self._trade_batch_size)
        batch_trades = [self._trade_queue[i] for i in range(batch_size)]
        batch_rows   = [
            (t.recv_ts, t.trade_ts, t.symbol, t.price, t.qty, int(t.is_buyer_maker), t.agg_trade_id)
            for t in batch_trades
        ]

        # 3. 带重试的写入
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                await self._conn.executemany(
                    """
                    INSERT INTO raw_trades
                        (recv_ts, trade_ts, symbol, price, qty, is_buyer_maker, agg_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_rows,
                )
                await self._conn.commit()

                # 写入成功 → 从队列移除
                for _ in range(batch_size):
                    self._trade_queue.popleft()

                self._write_counts["trades"] += batch_size
                self._integrity["written"]   += batch_size
                self._consecutive_flush_failures = 0
                # 注意：DEGRADED 一旦触发不在此处恢复
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

        # 4. 全部重试失败 → 写隔离文件
        self._write_counts["errors"]           += 1
        self._consecutive_flush_failures       += 1
        logger.error(
            "批量写入彻底失败（%d 次重试），%d 条成交尝试写入隔离文件: %s",
            _MAX_RETRIES, batch_size, last_exc,
        )
        iso_ok = await self._write_isolation(batch_trades)

        if iso_ok:
            self._integrity["isolated"] += batch_size
        # iso_ok=False → _write_isolation 内部已增 self._integrity["lost"]

        # 从队列移除（无论隔离是否成功，均移除以防无限循环）
        for _ in range(batch_size):
            self._trade_queue.popleft()

        # 5. 任何真实样本损坏 → 立即 DEGRADED（幂等）
        await self._ensure_degraded(
            f"批量写入彻底失败，{batch_size} 条成交"
            f"{'已写入隔离文件' if iso_ok else '永久丢失（隔离文件也失败）'}",
            context=json.dumps(self.integrity),
        )

        # 6. 连续失败阈值保障（通常已由上方 _ensure_degraded 覆盖）
        if self._consecutive_flush_failures >= _FAIL_THRESHOLD:
            await self._ensure_degraded(
                f"批量写入连续失败 {self._consecutive_flush_failures} 次",
                context=json.dumps(self.integrity),
            )

        return 0

    # ── 盘口深度快照 ────────────────────────────────────────────────────────────

    async def write_depth(self, depth: Depth) -> None:
        """写入盘口深度快照（1 秒降采样：同一 symbol 两次写入间隔 < 1000ms 则跳过）。"""
        if depth.event_ts - self._last_depth_ts < 1000:
            return
        self._last_depth_ts = depth.event_ts
        try:
            await self._conn.execute(
                """
                INSERT INTO raw_depth_snapshots (recv_ts, event_ts, symbol, bids_json, asks_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (depth.recv_ts, depth.event_ts, depth.symbol,
                 json.dumps(depth.bids), json.dumps(depth.asks)),
            )
            await self._conn.commit()
            self._write_counts["depths"] += 1
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入深度快照失败: %s", e)

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
        index_price: float | None = None,
        basis_bps: float | None = None,
        bid_depth_usdt: float | None = None,
        impact_ratio: float | None = None,
    ) -> int:
        """写入信号记录，返回新插入的 id。"""
        try:
            cursor = await self._conn.execute(
                """
                INSERT INTO signals
                    (ts, symbol, liq_notional_window, liq_accel_ratio,
                     vwap_15m, mid_price, deviation_bps, signal_fired,
                     entry_price, notes,
                     index_price, basis_bps, bid_depth_usdt, impact_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, symbol, liq_notional_window, liq_accel_ratio,
                 vwap_15m, mid_price, deviation_bps,
                 int(signal_fired), entry_price, notes,
                 index_price, basis_bps, bid_depth_usdt, impact_ratio),
            )
            await self._conn.commit()
            self._write_counts["signals"] += 1
            return cursor.lastrowid or 0
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入信号失败: %s", e)
            return 0

    # ── Episode（阶段 3）────────────────────────────────────────────────────────

    async def write_episode(self, episode: LiquidationEpisode) -> int:
        """
        写入一条 episode 研究样本，返回新插入的 id（失败时返回 0）。
        """
        try:
            cursor = await self._conn.execute(
                """
                INSERT INTO liquidation_episodes
                    (episode_id, symbol, side,
                     start_event_ts, end_event_ts, duration_ms,
                     liq_count, liq_notional_total, liq_peak_window,
                     liq_accel_ratio_peak, min_mid_price,
                     pre_event_vwap, max_deviation_bps,
                     pre_event_index_price, max_basis_bps, max_impact_ratio,
                     created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    episode.episode_id,  episode.symbol,  episode.side,
                    episode.start_event_ts, episode.end_event_ts, episode.duration_ms,
                    episode.liq_count,   episode.liq_notional_total, episode.liq_peak_window,
                    episode.liq_accel_ratio_peak, episode.min_mid_price,
                    episode.pre_event_vwap, episode.max_deviation_bps,
                    episode.pre_event_index_price, episode.max_basis_bps,
                    episode.max_impact_ratio,
                    int(time.time() * 1000),
                ),
            )
            await self._conn.commit()
            self._write_counts["episodes"] += 1
            logger.info(
                "[Episode] %s | %d 笔 | 总量=%.0f USDT | 峰值窗口=%.0f | 持续=%dms | "
                "基差=%s bps | 冲击率=%s | 偏离=%s bps",
                episode.episode_id,
                episode.liq_count,
                episode.liq_notional_total,
                episode.liq_peak_window,
                episode.duration_ms,
                f"{episode.max_basis_bps:.1f}" if episode.max_basis_bps is not None else "N/A",
                f"{episode.max_impact_ratio:.3f}" if episode.max_impact_ratio is not None else "N/A",
                f"{episode.max_deviation_bps:.1f}" if episode.max_deviation_bps is not None else "N/A",
            )
            return cursor.lastrowid or 0
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入 episode 失败: %s | %s", e, episode.episode_id)
            return 0

    # ── Episode Outcome（阶段 4）──────────────────────────────────────────────────

    async def get_episodes_pending_outcome(
        self, min_age_ms: int = 15 * 60_000
    ) -> list[dict]:
        """返回尚无 outcome 记录、且 end_event_ts 距今已超过 min_age_ms 的 episode 列表。"""
        cutoff = int(time.time() * 1000) - min_age_ms
        rows = await (await self._conn.execute(
            """
            SELECT e.episode_id, e.end_event_ts, e.min_mid_price, e.pre_event_vwap,
                   e.symbol, e.pre_event_index_price
            FROM   liquidation_episodes e
            LEFT   JOIN episode_outcomes o ON e.episode_id = o.episode_id
            WHERE  o.episode_id IS NULL
              AND  e.end_event_ts < ?
            ORDER  BY e.end_event_ts
            """,
            (cutoff,),
        )).fetchall()
        return [dict(r) for r in rows]

    async def get_trades_after_episode(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> list[tuple[int, float]]:
        """返回 episode 结束后 15 分钟内的成交序列 [(trade_ts, price), ...]，按 trade_ts 升序。

        start_ts 语义为 episode 的 end_event_ts（严格排他，即 > start_ts），
        end_ts 为观测窗口末端（含）。
        不使用 BETWEEN 以避免 trades[0] 误取到 episode 最后一笔成交（同一毫秒）。
        """
        rows = await (await self._conn.execute(
            """
            SELECT trade_ts, price FROM raw_trades
            WHERE  symbol = ? AND trade_ts > ? AND trade_ts <= ?
            ORDER  BY trade_ts, rowid
            """,
            (symbol, start_ts, end_ts),
        )).fetchall()
        return [(r[0], r[1]) for r in rows]

    async def write_episode_outcome(self, outcome: EpisodeOutcome) -> None:
        """写入 episode outcome 记录（使用 INSERT OR IGNORE 避免重复写入）。"""
        try:
            await self._conn.execute(
                """
                INSERT OR IGNORE INTO episode_outcomes
                    (episode_id, entry_price, price_at_episode_end,
                     price_at_1m, price_at_5m, price_at_15m,
                     min_price_0_5m, max_price_0_5m,
                     min_price_0_15m, max_price_0_15m,
                     mae_bps, mfe_bps,
                     rebound_to_vwap_ms, rebound_depth_bps,
                     trade_count_0_15m,
                     rebound_to_basis_zero_ms, basis_rebound_depth,
                     computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outcome.episode_id,          outcome.entry_price,
                    outcome.price_at_episode_end,
                    outcome.price_at_1m,          outcome.price_at_5m,  outcome.price_at_15m,
                    outcome.min_price_0_5m,       outcome.max_price_0_5m,
                    outcome.min_price_0_15m,      outcome.max_price_0_15m,
                    outcome.mae_bps,              outcome.mfe_bps,
                    outcome.rebound_to_vwap_ms,   outcome.rebound_depth_bps,
                    outcome.trade_count_0_15m,
                    outcome.rebound_to_basis_zero_ms, outcome.basis_rebound_depth,
                    int(time.time() * 1000),
                ),
            )
            await self._conn.commit()
            self._write_counts["outcomes"] += 1
            rebound_str = (
                f"{outcome.rebound_to_basis_zero_ms // 1000}s"
                if outcome.rebound_to_basis_zero_ms is not None else "未回归"
            )
            logger.info(
                "[Outcome] %s | MAE=%s bps | MFE=%s bps | 基差回归=%s | "
                "基差距离=%s bps | 成交=%d笔",
                outcome.episode_id,
                f"{outcome.mae_bps:.1f}" if outcome.mae_bps is not None else "N/A",
                f"{outcome.mfe_bps:.1f}" if outcome.mfe_bps is not None else "N/A",
                rebound_str,
                f"{outcome.basis_rebound_depth:.1f}" if outcome.basis_rebound_depth is not None else "N/A",
                outcome.trade_count_0_15m,
            )
        except Exception as e:
            self._write_counts["errors"] += 1
            logger.error("写入 outcome 失败: %s | %s", e, outcome.episode_id)

    # ── 内部工具 ──────────────────────────────────────────────────────────────

    async def _write_isolation(self, trades: list[Trade]) -> bool:
        """
        将无法写入 SQLite 的成交数据写入 JSONL 隔离文件。
        返回 True = 隔离成功（isolated）；False = 隔离也失败（lost）。
        """
        try:
            ts_str   = time.strftime("%Y%m%d_%H%M%S")
            iso_path = self._isolation_dir / f"trades_{ts_str}_{len(trades)}.jsonl"
            content  = "\n".join(
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
            self._integrity["lost"] += len(trades)
            logger.critical(
                "隔离文件写入失败！%d 条成交数据永久丢失: %s",
                len(trades), e,
            )
            return False

    async def _ensure_degraded(
        self, reason: str, context: str | None = None
    ) -> None:
        """将系统标记为 DEGRADED（幂等，进程生命周期内不可恢复）。"""
        if self._system_degraded:
            return
        self._system_degraded = True
        logger.critical("DEGRADED: %s", reason)
        await self.write_risk_event(
            component="storage.writer",
            event_type="WRITE_FAILURE_PERSISTENT",
            severity="CRITICAL",
            message=reason,
            context=context,
        )
