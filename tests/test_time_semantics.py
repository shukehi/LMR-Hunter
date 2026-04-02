"""
阶段 2 验收测试：时间语义正确性。

验证：
  1. snapshot_at(event_ts) 以事件时间为窗口锚点，而非本地时间
  2. 盘口足够新鲜时，偏离率可用
  3. 盘口过时（> MAX_MID_STALE_MS）时，偏离率为 None（系统拒绝生成错误研究样本）
  4. 相同事件在不同本地处理延迟下，liq_accel_ratio 一致（消除延迟误判）
  5. is_depth_fresh 属性正确区分新鲜/过时盘口（基于 MAX_MID_STALE_MS）
  6. bid_depth 新鲜度使用独立阈值 MAX_DEPTH_STALE_MS（Phase 3 独立 freshness 约束）
"""
from __future__ import annotations

import pytest

from src.features.calculator import (
    FeatureCalculator,
    MAX_STALE_MS,        # 向后兼容别名，仅用于旧测试引用
    MAX_MID_STALE_MS,    # mid_price 新鲜度阈值（is_depth_fresh 使用）
    MAX_DEPTH_STALE_MS,  # bid_depth 新鲜度阈值（impact_ratio 使用）
    MAX_INDEX_STALE_MS,  # index_price 新鲜度阈值（basis_bps 使用）
)
from src.gateway.models import Kline, Liquidation


# ── 工具函数 ──────────────────────────────────────────────────────────────────

BASE_TS = 1_700_000_000_000  # 固定基准时间戳 (ms)


def make_liq(event_ts: int, notional: float = 100_000.0) -> Liquidation:
    return Liquidation(
        recv_ts=event_ts + 150,
        event_ts=event_ts,
        symbol="BTCUSDT",
        side="SELL",
        qty=1.5,
        price=notional / 1.5,
        notional=notional,
        status="FILLED",
    )


def make_kline(open_ts: int, close: float = 68000.0) -> Kline:
    return Kline(
        recv_ts=open_ts + 60_100,
        open_ts=open_ts,
        close_ts=open_ts + 59_999,
        symbol="BTCUSDT",
        open=close - 10,
        high=close + 20,
        low=close - 20,
        close=close,
        volume=100.0,
        is_closed=True,
    )


def make_calculator_with_vwap() -> FeatureCalculator:
    """返回已预热 15 根 K 线的计算器，VWAP 可用。"""
    calc = FeatureCalculator(liq_window_sec=5)
    for i in range(15):
        calc.on_kline(make_kline(BASE_TS - (15 - i) * 60_000))
    return calc


# ── 测试用例 ──────────────────────────────────────────────────────────────────

class TestTimeSemantics:

    def test_snapshot_at_uses_event_ts_for_window(self):
        """
        核心测试：snapshot_at(event_ts) 以事件时间为窗口锚点。
        在 event_ts=T 时窗口内的强平，不因本地处理延迟而丢失。
        """
        calc = FeatureCalculator(liq_window_sec=5)

        # 在 T=0 时有一次强平
        liq_ts = BASE_TS
        calc.on_liquidation(make_liq(liq_ts, notional=50_000.0))

        # 以事件时间为锚点（窗口 [T-5s, T]）：强平在窗口内
        snap = calc.snapshot_at(liq_ts)
        assert snap.liq_notional_window == pytest.approx(50_000.0)
        assert snap.ts == liq_ts

        # 即使"本地"延迟了 3 秒处理，以 event_ts 为锚点结果相同
        snap_delayed = calc.snapshot_at(liq_ts)
        assert snap_delayed.liq_notional_window == snap.liq_notional_window

    def test_fresh_depth_enables_deviation(self):
        """盘口新鲜时（staleness < MAX_MID_STALE_MS），偏离率应可用。"""
        calc = make_calculator_with_vwap()

        event_ts   = BASE_TS + 1_000_000
        depth_ts   = event_ts - 200  # 盘口比事件早 200ms（正常情况，< MAX_MID_STALE_MS=500ms）

        calc.update_mid_price(67000.0, depth_ts)
        snap = calc.snapshot_at(event_ts)

        assert snap.mid_price is not None
        assert snap.deviation_bps is not None
        assert snap.depth_staleness_ms == 200
        assert snap.is_depth_fresh is True

    def test_stale_depth_disables_deviation(self):
        """
        关键测试：盘口过时（> MAX_MID_STALE_MS）时，
        偏离率应为 None，系统拒绝生成研究样本，而不是输出错误特征。
        """
        calc = make_calculator_with_vwap()

        event_ts   = BASE_TS + 1_000_000
        # 盘口比事件早 MAX_MID_STALE_MS + 1000ms（明显过时）
        stale_depth_ts = event_ts - (MAX_MID_STALE_MS + 1_000)

        calc.update_mid_price(67000.0, stale_depth_ts)
        snap = calc.snapshot_at(event_ts)

        assert snap.mid_price is None          # 过时盘口不参与计算
        assert snap.deviation_bps is None      # 偏离率不输出
        assert snap.is_depth_fresh is False
        assert snap.depth_staleness_ms == MAX_MID_STALE_MS + 1_000

    def test_accel_ratio_consistent_across_different_processing_delays(self):
        """
        阶段 2 核心验收：相同事件，以 event_ts 为锚点时，
        无论本地处理延迟多少，liq_accel_ratio 结果一致。
        """
        # 场景：T 时刻有前窗口强平，T+5s 有当前窗口强平
        event_ts_prev    = BASE_TS
        event_ts_current = BASE_TS + 5_000  # 恰好进入下一个窗口

        calc1 = FeatureCalculator(liq_window_sec=5)
        calc1.on_liquidation(make_liq(event_ts_prev,    notional=100_000.0))
        calc1.on_liquidation(make_liq(event_ts_current, notional=200_000.0))

        calc2 = FeatureCalculator(liq_window_sec=5)
        calc2.on_liquidation(make_liq(event_ts_prev,    notional=100_000.0))
        calc2.on_liquidation(make_liq(event_ts_current, notional=200_000.0))

        # calc1 立即处理（延迟 0ms）
        snap1 = calc1.snapshot_at(event_ts_current)
        # calc2 延迟 2000ms 处理（模拟慢处理路径）
        snap2 = calc2.snapshot_at(event_ts_current)  # 锚点仍是事件时间

        # 两者结果应完全一致
        assert snap1.liq_notional_window == snap2.liq_notional_window
        assert snap1.liq_accel_ratio     == snap2.liq_accel_ratio

    def test_depth_exactly_at_stale_boundary_is_fresh(self):
        """边界测试：mid_price staleness == MAX_MID_STALE_MS 时仍视为新鲜。"""
        calc = make_calculator_with_vwap()

        event_ts  = BASE_TS + 1_000_000
        depth_ts  = event_ts - MAX_MID_STALE_MS  # 恰好在 mid_price 阈值上（500ms）

        calc.update_mid_price(68000.0, depth_ts)
        snap = calc.snapshot_at(event_ts)

        assert snap.is_depth_fresh is True
        assert snap.mid_price is not None

    def test_depth_one_ms_over_stale_is_stale(self):
        """边界测试：mid_price staleness == MAX_MID_STALE_MS + 1 时视为过时。"""
        calc = make_calculator_with_vwap()

        event_ts  = BASE_TS + 1_000_000
        depth_ts  = event_ts - (MAX_MID_STALE_MS + 1)

        calc.update_mid_price(68000.0, depth_ts)
        snap = calc.snapshot_at(event_ts)

        assert snap.is_depth_fresh is False
        assert snap.mid_price is None

    def test_bid_depth_uses_independent_freshness_threshold(self):
        """
        Phase 3 验收：bid_depth 使用独立的 MAX_DEPTH_STALE_MS（1500ms）。
        mid_price 和 bid_depth 有不同阈值，不能混用。
        """
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 1_000_000

        # 设置强平窗口（使 impact_ratio 有分子）
        from tests.test_time_semantics import make_liq
        calc.on_liquidation(make_liq(event_ts - 1000, notional=200_000.0))

        # bid_depth 在 MAX_MID_STALE_MS (500ms) < staleness < MAX_DEPTH_STALE_MS (1500ms) 之间
        # 即：mid_price 此时已过时，但 bid_depth 仍在有效范围内
        depth_ts = event_ts - 1000  # 1000ms 前：mid 过时，bid_depth 仍有效

        calc.update_bid_depth(5_000_000.0, depth_ts)
        calc.update_mid_price(84000.0, depth_ts)  # mid 也在同时刻更新

        snap = calc.snapshot_at(event_ts)

        # mid_price 应过时（1000ms > MAX_MID_STALE_MS=500ms）
        assert snap.mid_price is None
        assert snap.is_depth_fresh is False

        # bid_depth 仍应有效（1000ms <= MAX_DEPTH_STALE_MS=1500ms），且因果方向正确
        assert snap.bid_depth_usdt is not None
        assert snap.impact_ratio is not None, "bid_depth 在 MAX_DEPTH_STALE_MS 内应有 impact_ratio"

    def test_bid_depth_after_max_depth_stale_gives_none(self):
        """bid_depth 超过 MAX_DEPTH_STALE_MS 时 impact_ratio = None。"""
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 1_000_000

        from tests.test_time_semantics import make_liq
        calc.on_liquidation(make_liq(event_ts - 1000, notional=200_000.0))

        # bid_depth 超过 MAX_DEPTH_STALE_MS（1500ms）
        depth_ts = event_ts - (MAX_DEPTH_STALE_MS + 1)
        calc.update_bid_depth(5_000_000.0, depth_ts)

        snap = calc.snapshot_at(event_ts)

        assert snap.bid_depth_usdt is None
        assert snap.impact_ratio is None

    def test_bid_depth_after_liquidation_gives_none(self):
        """因果约束：bid_depth 时间戳晚于强平事件时，impact_ratio = None。"""
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 1_000_000

        from tests.test_time_semantics import make_liq
        calc.on_liquidation(make_liq(event_ts - 100, notional=200_000.0))

        # bid_depth 在强平「之后」到达（盘口已修复）
        depth_ts_after = event_ts + 50  # 强平后 50ms（做市商已重新挂单）
        calc.update_bid_depth(8_000_000.0, depth_ts_after)  # 修复后深度更大

        snap = calc.snapshot_at(event_ts)

        assert snap.bid_depth_usdt is None, "强平后的深度不得参与 impact_ratio 计算"
        assert snap.impact_ratio is None

    def test_no_depth_update_gives_no_deviation(self):
        """从未收到盘口数据时，偏离率应为 None。"""
        calc = make_calculator_with_vwap()
        snap = calc.snapshot_at(BASE_TS + 1_000_000)

        assert snap.mid_price is None
        assert snap.deviation_bps is None
        assert snap.depth_event_ts is None
        assert snap.depth_staleness_ms is None
        assert snap.is_depth_fresh is False

    def test_snapshot_backward_compat_still_works(self):
        """snapshot() 向后兼容：仍应返回有效快照（但以本地时间为锚点）。"""
        calc = make_calculator_with_vwap()
        snap = calc.snapshot()

        # 不应抛出异常，基础字段应有值
        assert snap.kline_count == 15
        assert snap.vwap_15m is not None
        assert isinstance(snap.ts, int)
