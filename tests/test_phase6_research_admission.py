"""
Phase 6 验收测试：研究准入去 VWAP 依赖。

核心变更：
  旧准入条件（错误）：pre_event_vwap IS NOT NULL 是硬门槛
  新准入条件（正确）：pre_event_index_price IS NOT NULL 是硬门槛

物理逻辑：
  - VWAP 是过去 15 根 K 线的成交量加权均价，是统计量，与基差因果链无关
  - pre_event_index_price 是首笔强平时的现货指数价格，是套利者的公允价值锚点
  - 一个样本要进入「基差修复」策略的研究集，必须有 index_price，不需要 VWAP

验收场景：
  1. pre_event_index_price 完整，pre_event_vwap = NULL → 样本通过准入
  2. pre_event_index_price = NULL → 样本被拒绝（无论 VWAP 是否完整）
  3. 两者均完整 → 样本通过准入（向后兼容）
  4. 两者均为 NULL → 样本被拒绝
"""
from __future__ import annotations

import pytest
from typing import Optional

from src.analysis.research_report import EpisodeRow, qualify_episode

BASE_TS = 1_700_000_000_000  # 固定基准时间戳 (ms)


def make_episode(
    pre_event_vwap: Optional[float] = 82_000.0,
    pre_event_index_price: Optional[float] = 82_000.0,
    **overrides,
) -> EpisodeRow:
    """构造一个「除测试字段外其他均合格」的 EpisodeRow。"""
    defaults = dict(
        episode_id            = "BTCUSDT_test",
        start_event_ts        = BASE_TS,
        end_event_ts          = BASE_TS + 30_000,
        liq_count             = 5,
        liq_notional_total    = 500_000.0,
        min_mid_price         = 80_000.0,
        pre_event_vwap        = pre_event_vwap,
        max_deviation_bps     = -150.0,
        mae_bps               = -10.0,
        mfe_bps               = 50.0,
        rebound_to_vwap_ms    = 120_000,
        rebound_depth_bps     = 250.0,
        price_at_5m           = 81_500.0,
        entry_price           = 80_000.0,
        price_at_episode_end  = 80_100.0,
        trade_count_0_15m     = 100,
        pre_event_index_price = pre_event_index_price,
        max_basis_bps         = None,
        basis_rebound_depth   = None,
    )
    defaults.update(overrides)
    return EpisodeRow(**defaults)


class TestPhase6ResearchAdmission:
    """Phase 6 核心验收：研究准入锚点从 VWAP 迁移到 Index Price。"""

    def test_index_present_vwap_null_passes(self):
        """
        核心场景：pre_event_index_price 完整，pre_event_vwap = NULL → 通过准入。

        这是 Phase 6 的核心修复：VWAP 缺失不再拦截有效样本。
        现实中，冷启动时（<15 根 K 线）VWAP 不可用，但 index_price 立即可用。
        """
        ep = make_episode(
            pre_event_vwap        = None,   # VWAP 缺失（冷启动或历史数据）
            pre_event_index_price = 83_000.0,  # index 完整
        )
        reasons = qualify_episode(ep)

        # 不应因 VWAP 被拒
        assert "pre_event_vwap IS NULL" not in " ".join(reasons), (
            "VWAP 缺失不应成为拒绝理由（Phase 6 已移除此门槛）"
        )
        # 不应因 index_price 被拒（它是完整的）
        assert "pre_event_index_price IS NULL" not in " ".join(reasons), (
            "index_price 完整，不应被拒绝"
        )
        assert reasons == [], f"所有条件满足，不应有任何拒绝理由，实际：{reasons}"

    def test_index_null_fails_regardless_of_vwap(self):
        """
        pre_event_index_price = NULL → 必须被拒绝，即使 VWAP 完整。

        物理语义：没有 index_price，就无法计算 basis，样本对基差策略无研究价值。
        """
        ep = make_episode(
            pre_event_vwap        = 82_000.0,  # VWAP 完整
            pre_event_index_price = None,       # index 缺失
        )
        reasons = qualify_episode(ep)

        assert any("pre_event_index_price" in r for r in reasons), (
            "index_price 缺失时必须被拒绝，即使 VWAP 完整"
        )

    def test_both_present_passes(self):
        """两者均完整 → 向后兼容，样本通过。"""
        ep = make_episode(
            pre_event_vwap        = 82_000.0,
            pre_event_index_price = 82_000.0,
        )
        reasons = qualify_episode(ep)
        assert reasons == [], f"两者均完整，不应被拒绝，实际：{reasons}"

    def test_both_null_fails(self):
        """两者均为 NULL → 样本被拒绝（index_price 缺失原因）。"""
        ep = make_episode(
            pre_event_vwap        = None,
            pre_event_index_price = None,
        )
        reasons = qualify_episode(ep)

        assert any("pre_event_index_price" in r for r in reasons), (
            "两者均为 NULL，应因 index_price 缺失被拒"
        )
        # 但不应再出现 VWAP 相关的拒绝理由
        assert "pre_event_vwap IS NULL" not in " ".join(reasons), (
            "VWAP 缺失不应出现在拒绝理由中（Phase 6 已移除此门槛）"
        )

    def test_legacy_episode_without_index_is_rejected(self):
        """
        历史回归场景：Phase 1 实施前采集的 episode 没有 index_price，
        这些样本在基差策略研究中无意义，应被拒绝（不用于参数标定）。

        不是 bug，是正确的设计：避免用无基差锚点的历史数据污染研究集。
        """
        ep = make_episode(
            pre_event_vwap        = 82_000.0,  # 旧系统记录了 VWAP
            pre_event_index_price = None,       # 但没有 index（Phase 1 之前）
        )
        reasons = qualify_episode(ep)

        assert len(reasons) >= 1, "Phase 1 之前的 episode 应被拒绝"
        assert any("pre_event_index_price" in r for r in reasons), (
            "拒绝原因应明确指出 index_price 缺失"
        )
