"""
阶段 5 验收测试：可交易口径研究报告。

覆盖以下逻辑：
  1.  classify_session — 正确划分四个时段（亚盘 / 欧盘 / 美盘 / 夜盘）
  2.  simulate_trade — 数据缺失时返回 filled=False
  3.  simulate_trade — entry_offset=0 时 mae_bps=0 可成交
  4.  simulate_trade — mae_bps > -entry_offset 时不成交
  5.  simulate_trade — 硬止损优先于止盈（保守原则）
  6.  simulate_trade — 止盈路径正确计算 PnL
  7.  simulate_trade — 时间止损路径（无反弹 + price_at_5m 可用）
  8.  simulate_trade — 时间止损路径（price_at_5m 缺失回退）
  9.  simulate_trade — 止盈 PnL 扣手续费正确
  10. simulate_trade — 硬止损 PnL 扣手续费正确
  11. aggregate_stats — 全未成交时 fill_rate=0
  12. aggregate_stats — 全部成交全部盈利时指标正确
  13. aggregate_stats — 混合结果期望值正确
  14. load_episodes   — 联表查询正常，不含无 outcome 的 episode
  15. load_episodes   — since_ts 过滤有效
  16. build_report    — 无数据时返回合理报告（不崩溃）
  17. build_report    — 有数据时报告包含核心关键词
  18. classify_session — 边界值：07:00、13:30、21:00 精确分段
  19. qualify_episode — 合格样本通过所有检查
  20. qualify_episode — mae_bps IS NULL 被拒绝
  21. qualify_episode — trade_count_0_15m 不足被拒绝
  22. qualify_episode — liq_notional_total 不足被拒绝
  23. qualify_episode — max_deviation_bps 偏浅被拒绝
  24. qualify_episode — liq_count 不足被拒绝
  25. qualify_episode — rebound_depth_bps 不足被拒绝
  26. filter_qualified — 独立性检查剔除间隔不足的 episode
  27. filter_qualified — 多条合格 episode 顺序输出正确
  28. filter_qualified — 不合格 episode 不影响独立性计时器
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiosqlite
import pytest
import pytest_asyncio

from src.analysis.research_report import (
    EpisodeRow,
    QualReject,
    SimParams,
    Stats,
    TradeResult,
    aggregate_stats,
    build_report,
    classify_session,
    filter_qualified,
    load_episodes,
    qualify_episode,
    simulate_trade,
)

# ── 固定基准时间戳 ─────────────────────────────────────────────────────────────
# 2026-03-27 15:00:00 UTC → 美盘（15*60 = 900 min, 13:30–21:00）
BASE_TS_US = 1_743_084_000_000  # 美盘
BASE_TS_EU = 1_743_066_600_000  # 2026-03-27 10:00 UTC → 欧盘
BASE_TS_AS = 1_743_040_200_000  # 2026-03-27 03:00 UTC → 亚盘
BASE_TS_NY = 1_743_109_200_000  # 2025-03-27 21:00 UTC → 夜盘（整点）


# ── 辅助函数 ───────────────────────────────────────────────────────────────────

def make_episode(
    episode_id:          str  = "BTCUSDT_1000000",
    start_event_ts:      int  = BASE_TS_US,
    end_event_ts:        int  = BASE_TS_US + 30_000,
    liq_count:           int  = 5,
    liq_notional_total:  float = 500_000.0,
    min_mid_price:       Optional[float] = 80_000.0,
    pre_event_vwap:      Optional[float] = 82_000.0,
    max_deviation_bps:   Optional[float] = -150.0,
    mae_bps:             Optional[float] = -10.0,
    mfe_bps:             Optional[float] = 50.0,
    rebound_to_vwap_ms:  Optional[int]   = 120_000,
    rebound_depth_bps:   Optional[float] = 250.0,
    price_at_5m:           Optional[float] = 81_500.0,
    entry_price:           Optional[float] = 80_000.0,
    price_at_episode_end:  Optional[float] = 80_100.0,
    trade_count_0_15m:     int  = 100,
) -> EpisodeRow:
    return EpisodeRow(
        episode_id           = episode_id,
        start_event_ts       = start_event_ts,
        end_event_ts         = end_event_ts,
        liq_count            = liq_count,
        liq_notional_total   = liq_notional_total,
        min_mid_price        = min_mid_price,
        pre_event_vwap       = pre_event_vwap,
        max_deviation_bps    = max_deviation_bps,
        mae_bps              = mae_bps,
        mfe_bps              = mfe_bps,
        rebound_to_vwap_ms   = rebound_to_vwap_ms,
        rebound_depth_bps    = rebound_depth_bps,
        price_at_5m          = price_at_5m,
        entry_price          = entry_price,
        price_at_episode_end = price_at_episode_end,
        trade_count_0_15m    = trade_count_0_15m,
    )


async def make_db() -> aiosqlite.Connection:
    """创建内存 SQLite，建立测试所需的最小 schema。"""
    db = await aiosqlite.connect(":memory:")
    await db.executescript("""
        CREATE TABLE liquidation_episodes (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id           TEXT NOT NULL UNIQUE,
            symbol               TEXT NOT NULL,
            side                 TEXT NOT NULL,
            start_event_ts       INTEGER NOT NULL,
            end_event_ts         INTEGER NOT NULL,
            duration_ms          INTEGER NOT NULL,
            liq_count            INTEGER NOT NULL,
            liq_notional_total   REAL NOT NULL,
            liq_peak_window      REAL NOT NULL,
            liq_accel_ratio_peak REAL,
            min_mid_price        REAL,
            pre_event_vwap       REAL,
            max_deviation_bps    REAL,
            created_at           INTEGER NOT NULL
        );
        CREATE TABLE episode_outcomes (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id           TEXT NOT NULL UNIQUE,
            entry_price          REAL,
            price_at_episode_end REAL,
            price_at_1m          REAL,
            price_at_5m          REAL,
            price_at_15m         REAL,
            min_price_0_5m       REAL,
            max_price_0_5m       REAL,
            min_price_0_15m      REAL,
            max_price_0_15m      REAL,
            mae_bps              REAL,
            mfe_bps              REAL,
            rebound_to_vwap_ms   INTEGER,
            rebound_depth_bps    REAL,
            trade_count_0_15m    INTEGER NOT NULL DEFAULT 0,
            computed_at          INTEGER NOT NULL
        );
        CREATE TABLE service_heartbeats (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        INTEGER NOT NULL,
            component TEXT,
            status    TEXT,
            msg_total INTEGER DEFAULT 0,
            liq_count INTEGER DEFAULT 0,
            trade_count INTEGER DEFAULT 0,
            latency_p50 REAL,
            latency_p95 REAL,
            latency_p99 REAL,
            reconnects INTEGER DEFAULT 0,
            notes     TEXT
        );
        CREATE TABLE risk_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts         INTEGER NOT NULL,
            component  TEXT,
            event_type TEXT,
            severity   TEXT,
            message    TEXT,
            context    TEXT
        );
    """)
    await db.commit()
    return db


async def insert_episode_with_outcome(
    db: aiosqlite.Connection,
    ep: EpisodeRow,
) -> None:
    await db.execute(
        """INSERT INTO liquidation_episodes
           (episode_id, symbol, side, start_event_ts, end_event_ts,
            duration_ms, liq_count, liq_notional_total, liq_peak_window,
            min_mid_price, pre_event_vwap, max_deviation_bps, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            ep.episode_id, "BTCUSDT", "SELL",
            ep.start_event_ts, ep.end_event_ts,
            ep.end_event_ts - ep.start_event_ts, 3,
            ep.liq_notional_total, ep.liq_notional_total * 0.5,
            ep.min_mid_price, ep.pre_event_vwap, ep.max_deviation_bps,
            ep.end_event_ts + 1000,
        ),
    )
    await db.execute(
        """INSERT INTO episode_outcomes
           (episode_id, entry_price, price_at_episode_end, price_at_5m,
            mae_bps, mfe_bps, rebound_to_vwap_ms, rebound_depth_bps,
            trade_count_0_15m, computed_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            ep.episode_id, ep.entry_price, ep.price_at_episode_end, ep.price_at_5m,
            ep.mae_bps, ep.mfe_bps, ep.rebound_to_vwap_ms, ep.rebound_depth_bps,
            100, ep.end_event_ts + 900_000,
        ),
    )
    await db.commit()


# ── 1. classify_session ───────────────────────────────────────────────────────

class TestClassifySession:

    def test_asian_session(self):
        """03:00 UTC → 亚盘。"""
        assert classify_session(BASE_TS_AS) == "亚盘"

    def test_europe_session(self):
        """10:00 UTC → 欧盘。"""
        assert classify_session(BASE_TS_EU) == "欧盘"

    def test_us_session(self):
        """15:00 UTC → 美盘。"""
        assert classify_session(BASE_TS_US) == "美盘"

    def test_night_session(self):
        """21:00 UTC 整点 → 夜盘。"""
        assert classify_session(BASE_TS_NY) == "夜盘"

    def test_boundary_midnight(self):
        """00:00 UTC → 亚盘（含起点）。"""
        # 找一个整点 00:00 UTC
        midnight_ms = (BASE_TS_US // 86_400_000) * 86_400_000
        assert classify_session(midnight_ms) == "亚盘"

    def test_boundary_07_00(self):
        """07:00 UTC 整点 → 欧盘（新段开始）。"""
        day_ms  = (BASE_TS_US // 86_400_000) * 86_400_000
        t_07_00 = day_ms + 7 * 3_600_000
        assert classify_session(t_07_00) == "欧盘"

    def test_boundary_13_29(self):
        """13:29 UTC → 欧盘（最后一分钟）。"""
        day_ms  = (BASE_TS_US // 86_400_000) * 86_400_000
        t_13_29 = day_ms + (13 * 60 + 29) * 60_000
        assert classify_session(t_13_29) == "欧盘"

    def test_boundary_13_30(self):
        """13:30 UTC → 美盘（新段开始）。"""
        day_ms  = (BASE_TS_US // 86_400_000) * 86_400_000
        t_13_30 = day_ms + (13 * 60 + 30) * 60_000
        assert classify_session(t_13_30) == "美盘"

    def test_boundary_20_59(self):
        """20:59 UTC → 美盘（最后一分钟）。"""
        day_ms  = (BASE_TS_US // 86_400_000) * 86_400_000
        t_20_59 = day_ms + (20 * 60 + 59) * 60_000
        assert classify_session(t_20_59) == "美盘"


# ── 2-10. simulate_trade ──────────────────────────────────────────────────────

class TestSimulateTrade:

    def _default_params(self, **kwargs) -> SimParams:
        base = SimParams(entry_offset_bps=0.0, hard_stop_bps=30.0,
                         time_stop_sec=300, maker_fee_bps=2.0, taker_fee_bps=4.0)
        for k, v in kwargs.items():
            setattr(base, k, v)
        return base

    def test_missing_mae_not_filled(self):
        """mae_bps 缺失时，返回 filled=False。"""
        ep = make_episode(mae_bps=None)
        r = simulate_trade(ep, self._default_params())
        assert not r.filled
        assert r.exit_reason is None
        assert r.pnl_bps is None

    def test_missing_entry_price_not_filled(self):
        """entry_price 缺失时，返回 filled=False。"""
        ep = make_episode(entry_price=None)
        r = simulate_trade(ep, self._default_params())
        assert not r.filled

    def test_offset_zero_mae_zero_filled(self):
        """entry_offset=0，mae_bps=0 时触达成交价，应 filled=True。"""
        ep = make_episode(mae_bps=0.0)
        r = simulate_trade(ep, self._default_params(entry_offset_bps=0.0))
        assert r.filled

    def test_mae_above_offset_not_filled(self):
        """mae_bps=-5，entry_offset=10 → 未触及挂单价，not filled。"""
        ep = make_episode(mae_bps=-5.0)
        r = simulate_trade(ep, self._default_params(entry_offset_bps=10.0))
        assert not r.filled

    def test_hard_stop_priority_over_take_profit(self):
        """硬止损触发时，即使也有止盈，保守原则取硬止损。"""
        ep = make_episode(
            mae_bps            = -50.0,   # 触发 entry_offset=0 成交，也触发 hard_stop=30
            rebound_to_vwap_ms = 60_000,  # 1 分钟内也反弹了
        )
        params = self._default_params(entry_offset_bps=0.0, hard_stop_bps=30.0)
        r = simulate_trade(ep, params)
        assert r.filled
        assert r.exit_reason == "HARD_STOP"

    def test_take_profit_path(self):
        """mae 在 offset 附近（止损未触发），且有反弹 → TAKE_PROFIT。"""
        ep = make_episode(
            mae_bps            = -5.0,    # 轻微下探，entry_offset=0 → filled
            rebound_to_vwap_ms = 120_000, # 2 分钟内反弹到 VWAP
            rebound_depth_bps  = 250.0,
        )
        params = self._default_params(entry_offset_bps=0.0, hard_stop_bps=30.0)
        r = simulate_trade(ep, params)
        assert r.filled
        assert r.exit_reason == "TAKE_PROFIT"

    def test_time_stop_path(self):
        """无反弹，且止损未触发 → TIME_STOP，使用 price_at_5m。"""
        ep = make_episode(
            mae_bps            = -5.0,
            rebound_to_vwap_ms = None,     # 未反弹
            price_at_5m        = 80_400.0, # 5min 后小幅上涨
            entry_price        = 80_000.0,
        )
        params = self._default_params(entry_offset_bps=0.0, hard_stop_bps=30.0)
        r = simulate_trade(ep, params)
        assert r.filled
        assert r.exit_reason == "TIME_STOP"
        # fill_price = 80000, price_at_5m = 80400 → +50 bps - 2 - 4 = +44 bps
        assert r.pnl_bps == pytest.approx(44.0, abs=0.5)

    def test_time_stop_fallback_no_price_at_5m(self):
        """price_at_5m 缺失时，时间止损回退到 -(maker+taker fee)。"""
        ep = make_episode(
            mae_bps            = -5.0,
            rebound_to_vwap_ms = None,
            price_at_5m        = None,
        )
        params = self._default_params(entry_offset_bps=0.0)
        r = simulate_trade(ep, params)
        assert r.filled
        assert r.exit_reason == "TIME_STOP"
        assert r.pnl_bps == pytest.approx(-(params.maker_fee_bps + params.taker_fee_bps), abs=0.1)

    def test_take_profit_pnl_net_of_fees(self):
        """TAKE_PROFIT PnL = rebound_depth_bps + entry_offset - 2×maker_fee。"""
        ep = make_episode(
            mae_bps            = -3.0,
            rebound_to_vwap_ms = 90_000,
            rebound_depth_bps  = 200.0,
            entry_price        = 80_000.0,
        )
        params = self._default_params(
            entry_offset_bps=0.0, hard_stop_bps=30.0,
            maker_fee_bps=2.0, taker_fee_bps=4.0
        )
        r = simulate_trade(ep, params)
        assert r.exit_reason == "TAKE_PROFIT"
        # 200 + 0 - 2*2 = 196
        assert r.pnl_bps == pytest.approx(196.0, abs=0.1)

    def test_hard_stop_pnl_net_of_fees(self):
        """HARD_STOP PnL = -(hard_stop + maker_fee + taker_fee)。"""
        ep = make_episode(mae_bps=-50.0)
        params = self._default_params(
            entry_offset_bps=0.0, hard_stop_bps=30.0,
            maker_fee_bps=2.0, taker_fee_bps=4.0
        )
        r = simulate_trade(ep, params)
        assert r.exit_reason == "HARD_STOP"
        # -(30 + 2 + 4) = -36
        assert r.pnl_bps == pytest.approx(-36.0, abs=0.1)

    def test_rebound_outside_time_window_becomes_time_stop(self):
        """反弹延迟超出 time_stop_sec × 1000ms → 不算止盈，走时间止损。"""
        ep = make_episode(
            mae_bps            = -5.0,
            rebound_to_vwap_ms = 400_000,  # 6min 以上，超出 5min
            price_at_5m        = 80_200.0,
            entry_price        = 80_000.0,
        )
        params = self._default_params(entry_offset_bps=0.0, time_stop_sec=300)
        r = simulate_trade(ep, params)
        assert r.exit_reason == "TIME_STOP"


# ── 11-13. aggregate_stats ────────────────────────────────────────────────────

class TestAggregateStats:

    def test_all_not_filled(self):
        """全部未成交 → fill_rate=0，无其他有意义数值。"""
        eps = [make_episode(mae_bps=None) for _ in range(5)]
        params = SimParams()
        results = [simulate_trade(ep, params) for ep in eps]
        stats = aggregate_stats(results, eps)
        assert stats.fill_rate == 0.0
        assert stats.n_filled == 0

    def test_all_win(self):
        """全部成交全部止盈 → win_rate=1。"""
        eps = [
            make_episode(
                episode_id        = f"ep_{i}",
                mae_bps           = -3.0,
                rebound_to_vwap_ms= 60_000,
                rebound_depth_bps = 200.0,
            )
            for i in range(4)
        ]
        params = SimParams(entry_offset_bps=0.0, hard_stop_bps=30.0, maker_fee_bps=2.0)
        results = [simulate_trade(ep, params) for ep in eps]
        stats = aggregate_stats(results, eps)
        assert stats.fill_rate == 1.0
        assert stats.win_rate  == 1.0
        assert stats.n_take_profit == 4
        assert stats.expectancy > 0

    def test_mixed_results_expectancy(self):
        """混合结果：3 盈利 + 1 亏损，期望值应为加权均值。"""
        win_ep  = make_episode(
            mae_bps=-3.0, rebound_to_vwap_ms=60_000,
            rebound_depth_bps=200.0
        )
        loss_ep = make_episode(
            episode_id="loss", mae_bps=-50.0, rebound_to_vwap_ms=None
        )
        eps = [win_ep, win_ep, win_ep, loss_ep]
        params = SimParams(
            entry_offset_bps=0.0, hard_stop_bps=30.0,
            maker_fee_bps=2.0, taker_fee_bps=4.0
        )
        results = [simulate_trade(ep, params) for ep in eps]
        stats = aggregate_stats(results, eps)
        assert stats.fill_rate == 1.0
        assert 0 < stats.win_rate < 1
        # win=196 bps × 3, loss=-36 bps × 1 → expectancy=(196*3-36)/4=138
        assert stats.expectancy == pytest.approx(138.0, abs=1.0)


# ── 14-15. load_episodes ──────────────────────────────────────────────────────

class TestLoadEpisodes:

    @pytest.mark.asyncio
    async def test_loads_joined_episodes(self):
        """只返回同时有 episode 和 outcome 记录的条目。"""
        db = await make_db()
        try:
            ep = make_episode(episode_id="BTCUSDT_1000")
            await insert_episode_with_outcome(db, ep)

            # 插入一个没有 outcome 的 episode（不应被返回）
            await db.execute(
                """INSERT INTO liquidation_episodes
                   (episode_id, symbol, side, start_event_ts, end_event_ts,
                    duration_ms, liq_count, liq_notional_total, liq_peak_window, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                ("BTCUSDT_orphan", "BTCUSDT", "SELL",
                 BASE_TS_US + 999_999, BASE_TS_US + 1_029_999,
                 30_000, 2, 300_000.0, 150_000.0, BASE_TS_US + 2_000_000),
            )
            await db.commit()

            rows = await load_episodes(db)
            assert len(rows) == 1
            assert rows[0].episode_id == "BTCUSDT_1000"
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_since_ts_filter(self):
        """since_ts 过滤：早于窗口的 episode 不被返回。"""
        db = await make_db()
        try:
            old_ep = make_episode(
                episode_id    = "ep_old",
                start_event_ts= BASE_TS_US - 100_000,
                end_event_ts  = BASE_TS_US - 70_000,
            )
            new_ep = make_episode(
                episode_id    = "ep_new",
                start_event_ts= BASE_TS_US + 100_000,
                end_event_ts  = BASE_TS_US + 130_000,
            )
            await insert_episode_with_outcome(db, old_ep)
            await insert_episode_with_outcome(db, new_ep)

            rows = await load_episodes(db, since_ts=BASE_TS_US)
            assert len(rows) == 1
            assert rows[0].episode_id == "ep_new"
        finally:
            await db.close()


# ── 16-17. build_report ──────────────────────────────────────────────────────

class TestBuildReport:

    @pytest.mark.asyncio
    async def test_no_episodes_does_not_crash(self):
        """无 episode 数据时，build_report 返回包含警告的字符串，不崩溃。"""
        db = await make_db()
        try:
            # 插入一条心跳，避免 UNKNOWN
            await db.execute(
                "INSERT INTO service_heartbeats (ts, component, status, notes) VALUES (?,?,?,?)",
                (BASE_TS_US, "gateway", "OK",
                 '{"overflow_dropped":0,"isolated":0,"lost":0,"trade_queue_size":0,"episode_stats":{}}'),
            )
            await db.commit()
            report = await build_report(db, since_ts=0)
            assert isinstance(report, str)
            assert len(report) > 50
            assert "暂无有效 episode 数据" in report or "TRUSTED" in report
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_report_with_data_contains_key_sections(self):
        """有数据时报告包含所有核心章节关键词。"""
        db = await make_db()
        try:
            # 心跳
            await db.execute(
                "INSERT INTO service_heartbeats (ts, component, status, notes) VALUES (?,?,?,?)",
                (BASE_TS_US, "gateway", "OK",
                 '{"overflow_dropped":0,"isolated":0,"lost":0,"trade_queue_size":0,"episode_stats":{}}'),
            )
            # 3 条不同时段的 episodes
            eps = [
                make_episode(episode_id="ep_us1", start_event_ts=BASE_TS_US,
                             mae_bps=-5.0, rebound_to_vwap_ms=90_000,
                             rebound_depth_bps=200.0),
                make_episode(episode_id="ep_eu1", start_event_ts=BASE_TS_EU,
                             mae_bps=-5.0, rebound_to_vwap_ms=None,
                             price_at_5m=80_100.0),
                make_episode(episode_id="ep_as1", start_event_ts=BASE_TS_AS,
                             mae_bps=-80.0, rebound_to_vwap_ms=None,
                             price_at_5m=79_500.0),
            ]
            for ep in eps:
                await insert_episode_with_outcome(db, ep)
            await db.commit()

            report = await build_report(db, since_ts=0)

            # 核心章节
            assert "【一】数据概况" in report
            assert "【二】样本合格性过滤" in report
            assert "【三】全局可交易模拟" in report
            assert "【四】按盘口分组统计" in report
            assert "【五】参数敏感性分析" in report
            assert "【六】回弹质量分布" in report
            # 不得只有混合统计（必须有分组）
            assert "美盘" in report
            assert "欧盘" in report
            # 期望净收益应出现
            assert "期望净收益" in report
            # 成交率应出现
            assert "成交率" in report
            # 合格性章节必须存在
            assert "【二】样本合格性过滤" in report
        finally:
            await db.close()


# ── 19-28. qualify_episode / filter_qualified ─────────────────────────────────

class TestQualifyEpisode:
    """单条 episode 的静态合格性检查。"""

    def test_fully_qualified_returns_empty(self):
        """满足所有条件的 episode 返回空原因列表。"""
        ep = make_episode()   # 所有默认值均满足条件
        assert qualify_episode(ep) == []

    def test_mae_bps_null_rejected(self):
        """mae_bps IS NULL → 被拒绝。"""
        ep = make_episode(mae_bps=None)
        reasons = qualify_episode(ep)
        assert any("mae_bps" in r for r in reasons)

    def test_trade_count_low_rejected(self):
        """trade_count_0_15m < 30 → 被拒绝。"""
        ep = make_episode(trade_count_0_15m=10)
        reasons = qualify_episode(ep)
        assert any("trade_count_0_15m" in r for r in reasons)

    def test_low_notional_rejected(self):
        """liq_notional_total < 100,000 → 被拒绝。"""
        ep = make_episode(liq_notional_total=50_000.0)
        reasons = qualify_episode(ep)
        assert any("liq_notional_total" in r for r in reasons)

    def test_shallow_deviation_rejected(self):
        """max_deviation_bps > -10（偏离过浅）→ 被拒绝。"""
        ep = make_episode(max_deviation_bps=-5.0)
        reasons = qualify_episode(ep)
        assert any("max_deviation_bps" in r for r in reasons)

    def test_low_liq_count_rejected(self):
        """liq_count < 3 → 被拒绝。"""
        ep = make_episode(liq_count=2)
        reasons = qualify_episode(ep)
        assert any("liq_count" in r for r in reasons)

    def test_low_rebound_depth_rejected(self):
        """rebound_depth_bps < 10 → 被拒绝。"""
        ep = make_episode(rebound_depth_bps=5.0)
        reasons = qualify_episode(ep)
        assert any("rebound_depth_bps" in r for r in reasons)

    def test_multiple_failures_all_reported(self):
        """多个字段同时不合格时，所有原因均被记录。"""
        ep = make_episode(mae_bps=None, liq_count=1, liq_notional_total=1_000.0)
        reasons = qualify_episode(ep)
        assert len(reasons) >= 3

    def test_boundary_notional_exact(self):
        """liq_notional_total == 100,000 → 正好在边界上，视为合格。"""
        ep = make_episode(liq_notional_total=100_000.0)
        assert qualify_episode(ep) == []

    def test_boundary_deviation_exact(self):
        """max_deviation_bps == -10 → 正好在边界上，视为合格。"""
        ep = make_episode(max_deviation_bps=-10.0)
        assert qualify_episode(ep) == []


class TestFilterQualified:
    """filter_qualified 的独立性检查和序列逻辑。"""

    def test_all_qualified_no_gap_issue(self):
        """时间间隔充足的多条 episode，全部通过。"""
        eps = [
            make_episode(
                episode_id     = f"ep_{i}",
                start_event_ts = BASE_TS_US + i * 2_000_000,   # 间隔 2000s
                end_event_ts   = BASE_TS_US + i * 2_000_000 + 30_000,
            )
            for i in range(3)
        ]
        qualified, rejected = filter_qualified(eps)
        assert len(qualified) == 3
        assert len(rejected)  == 0

    def test_independence_violation_rejected(self):
        """两条 episode 间隔 < 900s → 第二条因独立性被拒绝。"""
        ep1 = make_episode(
            episode_id     = "ep_1",
            start_event_ts = BASE_TS_US,
            end_event_ts   = BASE_TS_US + 30_000,
        )
        ep2 = make_episode(
            episode_id     = "ep_2",
            start_event_ts = BASE_TS_US + 300_000,   # 仅 300s 后
            end_event_ts   = BASE_TS_US + 330_000,
        )
        qualified, rejected = filter_qualified([ep1, ep2])
        assert len(qualified) == 1
        assert qualified[0].episode_id == "ep_1"
        assert len(rejected) == 1
        assert "independence" in rejected[0].reasons[0]

    def test_disqualified_episode_does_not_block_next(self):
        """
        不合格的 episode 不占用独立性计时器：
        ep1（合格）→ ep2（信号弱，静态不合格）→ ep3（合格，与 ep1 间隔足够）
        ep3 应通过，独立性以 ep1 的 end_event_ts 为基准。
        """
        ep1 = make_episode(
            episode_id     = "ep_1",
            start_event_ts = BASE_TS_US,
            end_event_ts   = BASE_TS_US + 30_000,
        )
        ep2 = make_episode(
            episode_id        = "ep_2",
            start_event_ts    = BASE_TS_US + 300_000,   # 5 分钟后
            end_event_ts      = BASE_TS_US + 330_000,
            liq_notional_total= 1_000.0,                # 静态不合格
        )
        ep3 = make_episode(
            episode_id     = "ep_3",
            start_event_ts = BASE_TS_US + 1_800_000,    # 30 分钟后（对 ep1 而言 >15min）
            end_event_ts   = BASE_TS_US + 1_830_000,
        )
        qualified, rejected = filter_qualified([ep1, ep2, ep3])
        assert len(qualified) == 2
        assert [q.episode_id for q in qualified] == ["ep_1", "ep_3"]
        assert rejected[0].episode_id == "ep_2"
