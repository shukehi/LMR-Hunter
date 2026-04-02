"""
Phase 5 验收测试：信号时序修复。

验证四层信号的正确时序关系：
  L1（前置预警）：过去 180s 内是否出现 taker_sell_ratio 峰值 > 65%
  L2（核心触发）：当前 basis_bps < -30
  L4（入场确认）：taker_buy_ratio_rising()

核心物理逻辑：
  - taker_sell spike 出现在强平「之前」（T-3min），不是同步条件
  - 等 basis 最深时，卖压峰值已过；要求两者同时成立会永远不触发
  - 修复后：L1 检测历史窗口最大值，L2 检测当前值，解除时序耦合

验收场景：
  1. 前 3 分钟有卖压 spike，当前 basis 负值宽，买方回升 → 触发信号
  2. 前 3 分钟无卖压 spike，即使 basis 很负 → 不触发（缺 L1）
  3. 卖压有，basis 负，买方未回升 → 不触发（缺 L4）
  4. taker_sell_peak 在各子窗口的峰值正确定位
  5. taker_buy_rising 的斜率判断正确
  6. trade_flow 保留 180s（不因 liq_window=5s 而被过早清理）
"""
from __future__ import annotations

import pytest

from src.features.calculator import (
    FeatureCalculator,
    _TAKER_FLOW_MAX_LOOKBACK_MS,
    _TAKER_SELL_SUBWIN_MS,
    _TAKER_BUY_RECENT_MS,
    _TAKER_BUY_PRIOR_MS,
)
from src.gateway.models import Liquidation, Trade

BASE_TS = 1_700_000_000_000  # 固定基准时间戳 (ms)


def make_liq(event_ts: int, notional: float = 100_000.0) -> Liquidation:
    return Liquidation(
        recv_ts=event_ts + 50,
        event_ts=event_ts,
        symbol="BTCUSDT",
        side="SELL",
        qty=1.5,
        price=notional / 1.5,
        notional=notional,
        status="FILLED",
    )


def make_trade(
    trade_ts: int,
    price: float = 83000.0,
    qty: float = 1.0,
    is_buyer_maker: bool = True,  # True = Taker Sell（默认）
) -> Trade:
    return Trade(
        recv_ts=trade_ts + 5,
        trade_ts=trade_ts,
        symbol="BTCUSDT",
        price=price,
        qty=qty,
        is_buyer_maker=is_buyer_maker,
        agg_trade_id=0,
    )


def feed_sell_trades(calc: FeatureCalculator, start_ts: int, count: int,
                     interval_ms: int = 100, notional_per: float = 10_000.0) -> None:
    """批量喂入主动卖成交（Taker Sell）。"""
    qty = notional_per / 83000.0
    for i in range(count):
        ts = start_ts + i * interval_ms
        calc.on_trade_flow(make_trade(ts, price=83000.0, qty=qty, is_buyer_maker=True))


def feed_buy_trades(calc: FeatureCalculator, start_ts: int, count: int,
                    interval_ms: int = 100, notional_per: float = 10_000.0) -> None:
    """批量喂入主动买成交（Taker Buy）。"""
    qty = notional_per / 82500.0
    for i in range(count):
        ts = start_ts + i * interval_ms
        calc.on_trade_flow(make_trade(ts, price=82500.0, qty=qty, is_buyer_maker=False))


class TestTakerSellPeakInWindow:
    """taker_sell_peak_180s：前置卖压峰值检测。"""

    def test_no_trades_returns_none(self):
        """无任何成交数据时，峰值为 None（冷启动）。"""
        calc = FeatureCalculator(liq_window_sec=5)
        snap = calc.snapshot_at(BASE_TS)
        assert snap.taker_sell_peak_180s is None

    def test_pure_sell_pressure_returns_high_peak(self):
        """
        某个 30s 子窗口内全是 Taker Sell → 峰值应为 1.0。
        """
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000  # 足够晚，使 180s 窗口完整

        # 在 event_ts 前 60s 喂入纯 Taker Sell
        sell_start = event_ts - 60_000
        feed_sell_trades(calc, sell_start, count=30, interval_ms=1_000)

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_sell_peak_180s is not None
        assert snap.taker_sell_peak_180s == pytest.approx(1.0, abs=1e-6)

    def test_mixed_window_computes_correct_ratio(self):
        """
        30s 子窗口内 50% Taker Sell + 50% Taker Buy → 峰值约 0.5。
        """
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        win_start = event_ts - 30_000  # 最近的 30s 子窗口
        # 前 15s 全是 sell，后 15s 全是 buy，各 15 笔，金额相同
        for i in range(15):
            calc.on_trade_flow(make_trade(win_start + i * 1_000, is_buyer_maker=True))
        for i in range(15):
            calc.on_trade_flow(make_trade(win_start + 15_000 + i * 1_000, is_buyer_maker=False))

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_sell_peak_180s is not None
        assert 0.45 < snap.taker_sell_peak_180s < 0.55  # ≈ 0.5

    def test_peak_is_maximum_across_subwindows(self):
        """
        多个子窗口，峰值取最大值。
        第 1 个子窗口（最早）全是 sell → 峰值 1.0；
        第 6 个子窗口（最近）全是 buy → 该窗口 sell_ratio = 0；
        整体峰值应为 1.0（来自第 1 个子窗口）。
        """
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 第 1 个子窗口 [T-180s, T-150s]：全 sell
        sell_start = event_ts - 180_000
        feed_sell_trades(calc, sell_start, count=10, interval_ms=2_000)

        # 第 6 个子窗口 [T-30s, T]：全 buy
        buy_start = event_ts - 30_000
        feed_buy_trades(calc, buy_start, count=10, interval_ms=2_000)

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_sell_peak_180s is not None
        assert snap.taker_sell_peak_180s == pytest.approx(1.0, abs=1e-6), (
            "峰值应来自最早子窗口的纯卖压"
        )

    def test_empty_subwindow_is_skipped(self):
        """无成交的子窗口跳过，不影响其他子窗口的峰值。"""
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 只在第 3 个子窗口 [T-120s, T-90s] 放成交，全是 sell
        win3_start = event_ts - 120_000
        feed_sell_trades(calc, win3_start, count=5, interval_ms=2_000)

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_sell_peak_180s is not None
        assert snap.taker_sell_peak_180s == pytest.approx(1.0, abs=1e-6)


class TestTakerBuyRatioRising:
    """taker_buy_rising：买方力量回升确认。"""

    def test_no_trades_returns_false(self):
        """无成交数据时，保守返回 False。"""
        calc = FeatureCalculator(liq_window_sec=5)
        snap = calc.snapshot_at(BASE_TS)
        assert snap.taker_buy_rising is False

    def test_buy_rising_returns_true(self):
        """
        近期（0-10s）买方比例 > 前期（10-30s）买方比例 → True。

        设计：前期全是 sell，近期全是 buy。
        """
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 前期 [T-30s, T-10s]：全是 Taker Sell
        prior_start = event_ts - 30_000
        feed_sell_trades(calc, prior_start, count=20, interval_ms=1_000)  # 20 笔 sell in 20s

        # 近期 [T-10s, T]：全是 Taker Buy
        recent_start = event_ts - 10_000
        feed_buy_trades(calc, recent_start, count=10, interval_ms=1_000)

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_buy_rising is True, "近期全买 vs 前期全卖，买方应判定为回升"

    def test_buy_not_rising_returns_false(self):
        """
        近期（0-10s）买方比例 <= 前期（10-30s）买方比例 → False。

        设计：前期全是 buy，近期全是 sell（买方力量在衰减）。
        """
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 前期 [T-30s, T-10s]：全是 Taker Buy
        prior_start = event_ts - 30_000
        feed_buy_trades(calc, prior_start, count=20, interval_ms=1_000)

        # 近期 [T-10s, T]：全是 Taker Sell
        recent_start = event_ts - 10_000
        feed_sell_trades(calc, recent_start, count=10, interval_ms=1_000)

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_buy_rising is False, "近期全卖 vs 前期全买，买方不应判定为回升"

    def test_missing_prior_window_returns_false(self):
        """前期无数据时，保守返回 False（不够数据 → 不确认入场）。"""
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 只在近期放买单，前期无数据
        recent_start = event_ts - 5_000
        feed_buy_trades(calc, recent_start, count=5, interval_ms=1_000)

        snap = calc.snapshot_at(event_ts)
        assert snap.taker_buy_rising is False, "前期无数据应保守返回 False"


class TestTradeFlowRetention:
    """
    trade_flow 数据保留时间验证。

    Phase 5 关键约束：
      - trade_flow 必须保留 180s（_TAKER_FLOW_MAX_LOOKBACK_MS）
      - 不能因 liq_window_sec=5 就把 180s 前的数据清掉
      - 清理时机：snapshot_at 调用时统一清理超过 180s 的数据
    """

    def test_trades_within_180s_are_retained(self):
        """180s 以内的历史成交不被清理，可参与峰值计算。"""
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 在 T-170s 喂入全 sell（距当前锚点 170s，在 180s 保留窗口内）
        old_ts = event_ts - 170_000
        feed_sell_trades(calc, old_ts, count=5, interval_ms=1_000)

        snap = calc.snapshot_at(event_ts)
        # 170s 前的 sell 数据应仍在，峰值应为 1.0
        assert snap.taker_sell_peak_180s is not None, "170s 前的数据应在 180s 窗口内被保留"
        assert snap.taker_sell_peak_180s == pytest.approx(1.0, abs=1e-6)

    def test_trades_older_than_180s_are_purged(self):
        """超过 180s 的成交数据被清理，不参与峰值计算。"""
        calc = FeatureCalculator(liq_window_sec=5)
        event_ts = BASE_TS + 200_000

        # 在 T-190s 喂入全 sell（超过 180s → 应被清理）
        old_ts = event_ts - 190_000
        feed_sell_trades(calc, old_ts, count=5, interval_ms=1_000)

        snap = calc.snapshot_at(event_ts)
        # 190s 前的数据超出 _TAKER_FLOW_MAX_LOOKBACK_MS → 应被清理 → 峰值为 None
        assert snap.taker_sell_peak_180s is None, (
            "190s 前的数据超出 180s 保留窗口，应被清理"
        )

    def test_liq_window_cleanup_does_not_affect_180s_history(self):
        """
        调用 snapshot_at 多次（模拟 liq_window 的 5s 滚动），
        不会错误清理 180s 以内但超过 5s 的历史数据。
        """
        calc = FeatureCalculator(liq_window_sec=5)

        # T-100s: 喂入 sell（在 5s 窗口外，但在 180s 窗口内）
        sell_ts = BASE_TS + 100_000
        feed_sell_trades(calc, sell_ts, count=5, interval_ms=1_000)

        # T-100s+60s = T-40s: 再 snapshot（liq_window_sec=5s，只保留 5s 内的强平）
        # 但 trade_flow 的清理应用 180s 边界，不用 5s
        mid_snap = calc.snapshot_at(BASE_TS + 160_000)

        # T-100s+100s = T+0s 时的锚点
        event_ts = BASE_TS + 200_000
        snap = calc.snapshot_at(event_ts)

        # T-100s 的 sell 距 event_ts = 100s，在 180s 窗口内，应仍可见
        assert snap.taker_sell_peak_180s is not None, (
            "100s 前的 sell 在 180s 保留窗口内，不应被 liq_window=5s 清理"
        )


class TestSignalTimingIntegration:
    """
    信号时序集成测试：验证四层条件的正确协作。
    """

    def _make_calc_with_sell_spike(self, event_ts: int) -> FeatureCalculator:
        """
        构建已有前置卖压 spike 的计算器：
        T-120s 至 T-90s 内全是 Taker Sell（峰值 ≈ 1.0 在某个 30s 子窗口）
        T-10s 至 T 全是 Taker Buy（近期买方回升）
        """
        calc = FeatureCalculator(liq_window_sec=5)

        # L1：前期卖压（T-120s 到 T-90s）
        sell_start = event_ts - 120_000
        feed_sell_trades(calc, sell_start, count=30, interval_ms=1_000)

        # L4：近期买方回升（T-30s 到 T-10s：前期 = sell，T-10s 到 T：近期 = buy）
        prior_sell_start = event_ts - 30_000
        feed_sell_trades(calc, prior_sell_start, count=20, interval_ms=1_000)  # 前期 sell

        recent_buy_start = event_ts - 10_000
        feed_buy_trades(calc, recent_buy_start, count=10, interval_ms=1_000)   # 近期 buy

        return calc

    def test_full_signal_fires_when_all_conditions_met(self):
        """
        L1（历史卖压 spike）+ L2（当前 basis 负）+ L4（买方回升）→ 信号触发。
        """
        event_ts = BASE_TS + 300_000
        calc = self._make_calc_with_sell_spike(event_ts)

        # 设置 basis（basis 负值，L2 满足）
        calc.update_index_price(83000.0, event_ts - 500)   # index 价格
        calc.update_mid_price(82500.0, event_ts - 100)     # mid < index → basis 负

        snap = calc.snapshot_at(event_ts)

        # 验证各层状态
        l1 = snap.taker_sell_peak_180s is not None and snap.taker_sell_peak_180s > 0.65
        l2 = snap.basis_bps is not None and snap.basis_bps < -30
        l4 = snap.taker_buy_rising

        # basis = (82500 - 83000) / 83000 × 10000 ≈ -60.2 bps → L2 满足
        assert snap.basis_bps is not None and snap.basis_bps < -30, (
            f"L2 未满足，basis={snap.basis_bps}"
        )
        assert l1, f"L1 未满足，taker_sell_peak={snap.taker_sell_peak_180s}"
        assert l4, "L4 未满足，买方未回升"

        signal_fired = l1 and l2 and l4
        assert signal_fired, "所有条件满足，信号应触发"

    def test_signal_blocked_when_no_sell_spike(self):
        """
        前 3 分钟无卖压 spike（L1 缺失），即使 basis 负值很大也不触发。

        场景：basis 很负（-100 bps），买方也在回升，但前期无卖压 → 不触发。
        （可能是正常的 funding 偏差，不是强平驱动的冲击）
        """
        event_ts = BASE_TS + 300_000
        calc = FeatureCalculator(liq_window_sec=5)

        # 只在近期放买单（L4 满足），无历史卖压
        recent_buy_start = event_ts - 10_000
        feed_buy_trades(calc, recent_buy_start, count=10, interval_ms=1_000)

        # 添加少量 sell 在前期（比例远低于 65%）
        # 注意：必须用 feed_sell_trades 保证 notional = 10_000/笔，
        # 与 buy 的 notional 一致，才能正确计算比例
        prior_start = event_ts - 30_000
        feed_sell_trades(calc, prior_start, count=3, interval_ms=1_000)
        # 结果：sell notional = 3 × 10000 = 30000，buy notional = 10 × 10000 = 100000
        # sell_ratio in window = 30000/130000 ≈ 0.23 < 0.65 → L1 不满足

        # basis 很负
        calc.update_index_price(83000.0, event_ts - 500)
        calc.update_mid_price(82170.0, event_ts - 100)  # basis ≈ -100 bps

        snap = calc.snapshot_at(event_ts)

        l1 = snap.taker_sell_peak_180s is not None and snap.taker_sell_peak_180s > 0.65
        l2 = snap.basis_bps is not None and snap.basis_bps < -30

        assert l2, f"L2 应满足（basis={snap.basis_bps}），测试前提有误"
        assert not l1, (
            f"L1 不应满足（无异常卖压），但 taker_sell_peak={snap.taker_sell_peak_180s}"
        )

        signal_fired = l1 and l2 and snap.taker_buy_rising
        assert not signal_fired, "缺少 L1 卖压前置条件，信号不应触发"

    def test_signal_blocked_when_buy_not_rising(self):
        """
        卖压有（L1），basis 负（L2），但买方未回升（L4 缺失）→ 不触发。

        场景：强平仍在持续，卖压还未消退。
        """
        event_ts = BASE_TS + 300_000
        calc = FeatureCalculator(liq_window_sec=5)

        # L1：历史卖压 spike
        sell_start = event_ts - 120_000
        feed_sell_trades(calc, sell_start, count=30, interval_ms=1_000)

        # L4 不满足：近期也是卖（强平仍持续）
        recent_sell_start = event_ts - 30_000
        feed_sell_trades(calc, recent_sell_start, count=30, interval_ms=1_000)

        # basis 负
        calc.update_index_price(83000.0, event_ts - 500)
        calc.update_mid_price(82500.0, event_ts - 100)

        snap = calc.snapshot_at(event_ts)

        l1 = snap.taker_sell_peak_180s is not None and snap.taker_sell_peak_180s > 0.65
        l2 = snap.basis_bps is not None and snap.basis_bps < -30
        l4 = snap.taker_buy_rising

        assert l1, "L1 应满足"
        assert l2, "L2 应满足"
        assert not l4, "L4 不应满足（近期全是卖单，买方未回升）"

        signal_fired = l1 and l2 and l4
        assert not signal_fired, "缺少 L4 入场确认，信号不应触发"
