"""
Phase 4 验收测试：Outcome 基差语义修复。

验证 rebound_to_basis_zero_ms 的核心语义：
  - 旧（错误）：perp_price >= pre_event_index_price（固定价格阈值）
  - 新（正确）：basis_bps(T) = (perp(T) - index(T)) / index(T) × 10000 >= -5

四个验收场景：
  A. 市场整体下移 + basis 收敛
     → 新路径应判定为"已修复"，旧路径因 perp 永远未回到初始 index 而误判"未修复"

  B. perp 独立下跌，basis 进一步扩大
     → 新路径和旧路径均应判定为"未修复"

  C. perp 快速弹回，basis 恢复
     → 新路径应找到修复点，且时间戳应比旧路径早（旧路径用固定价格门槛较高）

  D. 无 index_prices 降级路径
     → 降级到旧的固定价格近似，语义差但兼容

测试数据设计约束：
  - index_prices 必须在对应 trade 时间戳前 <= _INDEX_ALIGN_MAX_MS (2000ms) 内
  - 使用 1000ms 间隔，确保每笔成交都能找到有效 index
"""
from __future__ import annotations

import pytest

from src.features.outcome import compute_outcome, _BASIS_ZERO_BPS, _INDEX_ALIGN_MAX_MS

BASE_TS = 1_700_000_000_000  # 固定基准时间戳 (ms)


class TestPhase4OutcomeSemantics:
    """Phase 4 核心验收：rebound_to_basis_zero_ms 语义正确性。"""

    def test_scenario_a_market_wide_drop_basis_converges(self):
        """
        场景 A：市场整体下移，perp 和 index 同步下跌，basis 收敛到 0。

        市场背景：
          - 强平冲击后，perp 和 index 同步下跌 (market 整体 risk-off)
          - perp 从 83000 跌到 82500，index 同步从 83000 跌到 82500
          - basis 最终收敛（perp == index → basis = 0 bps）
          - perp (82500) 永远不会回到 pre_event_index_price (83000)

        预期：
          - 新路径（实时 basis）：✅ 找到修复（basis = 0 >= -5）
          - 旧路径（固定价格）：❌ perp 82500 < index 83000，永远找不到
        """
        end_event_ts = BASE_TS
        entry_price  = 82000.0          # 理论入场价（最低盘口中价）
        pre_event_index_price = 83000.0 # 首笔强平时的现货指数价

        # 成交序列：perp 和 index 同步下跌，basis 逐渐收敛
        # 关键：trade(+120_000) 时 perp=82500, index=82500 → basis=0 bps
        trades = [
            (BASE_TS +  60_000, 82800.0),   # perp=82800, index(见下)=82800 → basis≈0
            (BASE_TS + 120_000, 82500.0),   # perp=82500, index=82500 → basis=0 bps ✓
            (BASE_TS + 180_000, 82500.0),   # perp 稳定
        ]

        # index_prices：每笔 trade 前 1000ms 内有对应数据（满足 _INDEX_ALIGN_MAX_MS=2000ms）
        index_prices = [
            (BASE_TS +  59_000, 82800.0),   # 对应 trade@60_000，gap=1000ms ✓
            (BASE_TS + 119_000, 82500.0),   # 对应 trade@120_000，gap=1000ms ✓ → basis=0
            (BASE_TS + 179_000, 82500.0),   # 对应 trade@180_000，gap=1000ms ✓
        ]

        # ── 新路径（实时 basis）──────────────────────────────────────────────────
        outcome_new = compute_outcome(
            episode_id             = "test_A_new",
            end_event_ts           = end_event_ts,
            entry_price            = entry_price,
            pre_event_vwap         = None,
            trades                 = trades,
            pre_event_index_price  = pre_event_index_price,
            index_prices           = index_prices,
        )
        assert outcome_new.rebound_to_basis_zero_ms is not None, "新路径应判断为修复"
        # 第一个 basis >= -5 的时刻是 trade@60_000（basis ≈ 0）
        assert outcome_new.rebound_to_basis_zero_ms == 60_000, (
            f"修复时刻应在 60_000ms，实际 {outcome_new.rebound_to_basis_zero_ms}"
        )

        # ── 旧路径（固定价格，降级兼容）────────────────────────────────────────
        # perp 最高 82800 < pre_event_index_price 83000 → 旧路径找不到修复
        outcome_old = compute_outcome(
            episode_id             = "test_A_old",
            end_event_ts           = end_event_ts,
            entry_price            = entry_price,
            pre_event_vwap         = None,
            trades                 = trades,
            pre_event_index_price  = pre_event_index_price,
            index_prices           = [],  # 空 → 强制走旧路径
        )
        assert outcome_old.rebound_to_basis_zero_ms is None, (
            "旧路径（固定价格）：perp 未达到 pre_event_index_price，应为 None"
        )

    def test_scenario_b_perp_drops_further_basis_widens(self):
        """
        场景 B：perp 继续下跌，basis 进一步扩大（做空压力未释放）。

        预期：新路径和旧路径均找不到修复点。
        """
        end_event_ts = BASE_TS
        entry_price  = 82000.0
        pre_event_index_price = 83000.0

        # perp 持续下跌，index 相对稳定，basis 越来越负
        trades = [
            (BASE_TS +  60_000, 82000.0),  # perp=82000, index=82900 → basis≈-10.9 bps
            (BASE_TS + 120_000, 81500.0),  # perp=81500, index=82800 → basis≈-15.7 bps
            (BASE_TS + 180_000, 81000.0),  # perp=81000, index=82700 → basis≈-20.6 bps
        ]
        index_prices = [
            (BASE_TS +  59_000, 82900.0),
            (BASE_TS + 119_000, 82800.0),
            (BASE_TS + 179_000, 82700.0),
        ]

        outcome_new = compute_outcome(
            episode_id            = "test_B_new",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = pre_event_index_price,
            index_prices          = index_prices,
        )
        assert outcome_new.rebound_to_basis_zero_ms is None, "basis 扩大，新路径不应找到修复"

        outcome_old = compute_outcome(
            episode_id            = "test_B_old",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = pre_event_index_price,
            index_prices          = [],
        )
        assert outcome_old.rebound_to_basis_zero_ms is None, "basis 扩大，旧路径不应找到修复"

    def test_scenario_c_perp_rebounds_basis_recovers(self):
        """
        场景 C：典型修复场景 — perp 快速弹回，basis 从 -50bps 收敛到 0。

        背景：
          - 强平冲击将 perp 打到 82500（index 仍在 83000）→ basis = -60.6 bps
          - 套利者买入 perp + 卖现货，basis 快速修复
          - trade@120_000：perp=82960, index=83000 → basis≈-4.8 bps ≥ -5 → 修复！

        预期：
          - 新路径：在 120_000ms 找到修复（basis ≈ -4.8 bps ≥ -5 bps）
          - basis_rebound_depth：从 entry(82500) 到 index(83000) 的距离 ≈ 60.6 bps
        """
        end_event_ts = BASE_TS
        entry_price  = 82500.0
        pre_event_index_price = 83000.0

        trades = [
            (BASE_TS +  60_000, 82600.0),   # perp=82600, index=82980 → basis≈-45.8 bps
            (BASE_TS + 120_000, 82960.0),   # perp=82960, index=83000 → basis≈-4.8 bps ≥ -5 ✓
            (BASE_TS + 180_000, 83010.0),   # 已过修复点
        ]
        index_prices = [
            (BASE_TS +  59_000, 82980.0),   # 对应 trade@60_000
            (BASE_TS + 119_000, 83000.0),   # 对应 trade@120_000 → basis=(82960-83000)/83000×10000=-4.82
            (BASE_TS + 179_000, 83000.0),   # 对应 trade@180_000
        ]

        outcome = compute_outcome(
            episode_id            = "test_C",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = pre_event_index_price,
            index_prices          = index_prices,
        )

        assert outcome.rebound_to_basis_zero_ms is not None, "经典修复场景应找到修复点"
        assert outcome.rebound_to_basis_zero_ms == 120_000, (
            f"修复时刻应在 120_000ms，实际 {outcome.rebound_to_basis_zero_ms}"
        )

        # 验证 basis_rebound_depth（从 entry 到 pre_event_index 的理论止盈空间）
        expected_depth = round((pre_event_index_price - entry_price) / entry_price * 10_000, 1)
        assert outcome.basis_rebound_depth == pytest.approx(expected_depth, abs=0.1), (
            f"basis_rebound_depth 应为 {expected_depth} bps，实际 {outcome.basis_rebound_depth}"
        )

    def test_scenario_d_degraded_fallback_no_index_prices(self):
        """
        场景 D：降级兼容路径 — 无 index_prices 时回退到旧的固定价格近似。

        预期：当 index_prices=[] 且 pre_event_index_price 可用时，
              使用 perp >= pre_event_index_price 作为修复条件。
        """
        end_event_ts = BASE_TS
        entry_price  = 82500.0
        pre_event_index_price = 83000.0

        trades = [
            (BASE_TS +  60_000, 82800.0),   # 未达到 pre_event_index_price
            (BASE_TS + 120_000, 83000.0),   # 恰好达到 → 旧路径修复
            (BASE_TS + 180_000, 83100.0),
        ]

        outcome = compute_outcome(
            episode_id            = "test_D_fallback",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = pre_event_index_price,
            index_prices          = [],   # 空 → 触发降级路径
        )

        # 降级路径：perp(120_000) = 83000 >= pre_event_index_price=83000 → 修复
        assert outcome.rebound_to_basis_zero_ms is not None, "降级路径应找到修复"
        assert outcome.rebound_to_basis_zero_ms == 120_000

    def test_index_prices_stale_beyond_limit_returns_none(self):
        """
        边界测试：index_price 距 trade 超过 _INDEX_ALIGN_MAX_MS 时，视为数据过旧，
        该时间点跳过，不参与 basis 计算。

        若所有点均超时，则 rebound_to_basis_zero_ms = None（数据不足，不猜测）。
        """
        end_event_ts = BASE_TS
        entry_price  = 82500.0
        pre_event_index_price = 83000.0

        trades = [
            (BASE_TS + 60_000, 82950.0),  # basis 很接近 0，但 index 数据过旧
        ]

        # index_price 距 trade 超过 2000ms（3000ms gap → 过旧）
        index_prices = [
            (BASE_TS + 60_000 - (_INDEX_ALIGN_MAX_MS + 1_000), 82950.0),
        ]

        outcome = compute_outcome(
            episode_id            = "test_stale_index",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = pre_event_index_price,
            index_prices          = index_prices,
        )

        # index 过旧，跳过该点；无其他数据 → 无法判定修复
        assert outcome.rebound_to_basis_zero_ms is None, (
            "index_price 过旧应跳过，不应凑合判断修复"
        )

    def test_basis_exactly_at_threshold_counts_as_recovered(self):
        """
        边界测试：basis_bps == _BASIS_ZERO_BPS（即 -5.0）时，视为已修复（>= 阈值）。
        """
        end_event_ts = BASE_TS
        entry_price  = 82500.0

        # 设计 perp 和 index 使 basis 恰好等于 -5.0 bps
        # basis = (perp - index) / index × 10000 = -5.0
        # → perp = index × (1 - 5/10000) = index × 0.9995
        index_val = 83000.0
        perp_val  = round(index_val * (1 - 5 / 10_000), 2)  # 82958.5

        trades = [
            (BASE_TS + 60_000, perp_val),
        ]
        index_prices = [
            (BASE_TS + 59_000, index_val),  # 1000ms 前，在容忍范围内
        ]

        outcome = compute_outcome(
            episode_id            = "test_threshold",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = index_val,
            index_prices          = index_prices,
        )

        # basis = -5.0 bps >= -5.0 bps → 修复
        assert outcome.rebound_to_basis_zero_ms is not None, (
            f"basis == {_BASIS_ZERO_BPS} bps 应视为已修复（>= 阈值）"
        )
        assert outcome.rebound_to_basis_zero_ms == 60_000

    def test_basis_one_bps_below_threshold_not_recovered(self):
        """
        边界测试：basis_bps < _BASIS_ZERO_BPS（-5.1 bps）时，不视为修复。
        """
        end_event_ts = BASE_TS
        entry_price  = 82500.0

        # basis = -5.1 bps → perp = index × (1 - 5.1/10000)
        index_val = 83000.0
        perp_val  = round(index_val * (1 - 5.1 / 10_000), 2)  # 82957.67

        trades = [
            (BASE_TS + 60_000, perp_val),
        ]
        index_prices = [
            (BASE_TS + 59_000, index_val),
        ]

        outcome = compute_outcome(
            episode_id            = "test_below_threshold",
            end_event_ts          = end_event_ts,
            entry_price           = entry_price,
            pre_event_vwap        = None,
            trades                = trades,
            pre_event_index_price = index_val,
            index_prices          = index_prices,
        )

        assert outcome.rebound_to_basis_zero_ms is None, (
            f"basis < {_BASIS_ZERO_BPS} bps 不应视为修复"
        )
