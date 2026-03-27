"""
阶段 3 验收测试：Episode 模型。

验收标准（来自 optimization-plan.md 阶段 3）：
  1. 一次连续踩踏只生成一个 episode 级样本
  2. episode 可回放到原始强平事件集合（通过 start/end_event_ts + symbol + side 范围查询）
  3. 低于噪声门槛的序列不生成 episode
  4. 聚合字段（peak、min_price、deviation）正确跟踪整个 episode

额外验收：
  5. 相邻事件间隔 > gap_ms 时拆分为两个独立 episode
  6. episode 总持续 > max_duration_ms 时强制关闭
  7. 统计计数（emitted / discarded / active）正确维护
  8. flush() 在系统关闭时正确关闭未完成的 episode
  9. pre_event_vwap 仅使用首笔事件的 VWAP（不被后续更新覆盖）
  10. episode_id 格式可追溯（"{symbol}_{start_event_ts}"）
"""
import pytest
from src.features.episode import EpisodeBuilder, LiquidationEpisode

# 固定基准时间戳（ms），用于使测试不依赖系统时钟
BASE_TS = 1_700_000_000_000


def _feed(
    builder:  EpisodeBuilder,
    offset:   int,                     # 相对 BASE_TS 的偏移量 (ms)
    notional: float        = 100_000.0,
    window:   float        = 100_000.0,
    accel:    float | None = None,
    mid:      float | None = 85_000.0,
    dev:      float | None = -50.0,
    vwap:     float | None = 86_000.0,
    symbol:   str          = "BTCUSDT",
    side:     str          = "SELL",
) -> LiquidationEpisode | None:
    """辅助函数：向 builder 喂入一笔强平事件，返回已完成的 episode（如有）。"""
    return builder.feed(
        event_ts            = BASE_TS + offset,
        symbol              = symbol,
        side                = side,
        notional            = notional,
        liq_notional_window = window,
        liq_accel_ratio     = accel,
        mid_price           = mid,
        deviation_bps       = dev,
        vwap_15m            = vwap,
    )


# ── 1. 基础行为 ────────────────────────────────────────────────────────────────

def test_single_liquidation_returns_none():
    """单笔强平不立即产生 episode（需等到 gap 超时或 flush）。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    result = _feed(builder, offset=0)
    assert result is None


def test_cascade_produces_one_episode():
    """连续踩踏（事件间隔 < gap_ms）只产生一个 episode，liq_count 等于喂入笔数。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    # 5 笔强平，每隔 5 秒（远小于 30 秒 gap）
    for i in range(5):
        result = _feed(builder, offset=i * 5_000)
        assert result is None  # 还未关闭，应返回 None
    ep = builder.flush()
    assert ep is not None
    assert isinstance(ep, LiquidationEpisode)
    assert ep.liq_count == 5
    assert ep.liq_notional_total == pytest.approx(500_000.0)


def test_flush_on_empty_builder_returns_none():
    """空 builder 调用 flush() 应返回 None。"""
    builder = EpisodeBuilder()
    assert builder.flush() is None


# ── 2. 终止条件 ────────────────────────────────────────────────────────────────

def test_gap_separates_into_two_episodes():
    """两波冲击之间间隔超过 gap_ms，产生两个独立 episode。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)

    # 第一波：3 笔，总量 300_000 USDT
    for i in range(3):
        _feed(builder, offset=i * 5_000)

    # 第二波开头触发 gap（40_000ms > 30_000ms）→ 第一波应作为完整 episode 返回
    first_ep = _feed(builder, offset=40_000)
    assert first_ep is not None, "第二波首笔应触发第一波的关闭"
    assert first_ep.liq_count == 3
    assert first_ep.liq_notional_total == pytest.approx(300_000.0)

    # 继续第二波（再喂 2 笔）
    _feed(builder, offset=45_000)
    _feed(builder, offset=50_000)

    second_ep = builder.flush()
    assert second_ep is not None
    assert second_ep.liq_count == 3  # 包含触发关闭的首笔 + 2 笔


def test_max_duration_closes_episode():
    """episode 超过 max_duration_ms 时自动关闭，关闭后开始新 episode。"""
    builder = EpisodeBuilder(
        gap_ms           = 30_000,
        noise_threshold_usdt = 50_000,
        max_duration_ms  = 60_000,     # 60 秒
    )
    _feed(builder, offset=0)           # t=0
    _feed(builder, offset=30_000)      # t=30s（duration=30s < 60s，不触发）
    closed = _feed(builder, offset=70_000)  # t=70s（duration=70s > 60s → 关闭旧 episode）
    assert closed is not None
    assert closed.start_event_ts == BASE_TS
    assert closed.end_event_ts   == BASE_TS + 30_000   # 末笔是 t=30s
    assert closed.duration_ms    == 30_000

    # 新 episode 已从 t=70s 开始
    remaining = builder.flush()
    assert remaining is not None
    assert remaining.start_event_ts == BASE_TS + 70_000
    assert remaining.liq_count == 1


# ── 3. 噪声过滤 ────────────────────────────────────────────────────────────────

def test_below_noise_threshold_not_emitted():
    """总名义价值低于噪声门槛时，flush() 返回 None，discarded 计数递增。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=500_000)
    _feed(builder, offset=0, notional=100_000.0)  # 100k < 500k
    ep = builder.flush()
    assert ep is None
    assert builder.stats["discarded"] == 1
    assert builder.stats["emitted"]   == 0


# ── 4. 聚合字段正确性 ─────────────────────────────────────────────────────────

def test_peak_window_tracks_maximum():
    """liq_peak_window 应为 episode 期间所有事件的最大窗口值。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    _feed(builder, offset=0,      window=100_000.0)
    _feed(builder, offset=5_000,  window=250_000.0)   # 峰值
    _feed(builder, offset=10_000, window=150_000.0)
    ep = builder.flush()
    assert ep is not None
    assert ep.liq_peak_window == pytest.approx(250_000.0)


def test_min_mid_price_tracks_lowest():
    """min_mid_price 应为 episode 期间盘口中间价的最低值（最深跌点）。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    _feed(builder, offset=0,      mid=85_000.0)
    _feed(builder, offset=5_000,  mid=82_000.0)   # 最低
    _feed(builder, offset=10_000, mid=83_500.0)
    ep = builder.flush()
    assert ep is not None
    assert ep.min_mid_price == pytest.approx(82_000.0)


def test_max_deviation_bps_most_negative():
    """max_deviation_bps 应为 episode 期间数值最小（最大超跌）的偏离率。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    _feed(builder, offset=0,      dev=-30.0)
    _feed(builder, offset=5_000,  dev=-120.0)   # 最大超跌
    _feed(builder, offset=10_000, dev=-80.0)
    ep = builder.flush()
    assert ep is not None
    assert ep.max_deviation_bps == pytest.approx(-120.0)


def test_pre_event_vwap_uses_first_event_only():
    """pre_event_vwap 应锁定为首笔强平时的 VWAP，后续 VWAP 不覆盖。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    _feed(builder, offset=0,      vwap=86_000.0)   # 首笔
    _feed(builder, offset=5_000,  vwap=85_500.0)
    _feed(builder, offset=10_000, vwap=85_000.0)
    ep = builder.flush()
    assert ep is not None
    assert ep.pre_event_vwap == pytest.approx(86_000.0)


# ── 5. 可追溯性 ────────────────────────────────────────────────────────────────

def test_episode_id_format_and_traceability():
    """
    episode_id = "{symbol}_{start_event_ts}"，与 start/end_event_ts 可构成范围查询：
    SELECT * FROM raw_liquidations
    WHERE event_ts BETWEEN start_event_ts AND end_event_ts
      AND symbol = symbol AND side = side
    """
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=50_000)
    _feed(builder, offset=0)
    _feed(builder, offset=5_000)
    _feed(builder, offset=10_000)
    ep = builder.flush()
    assert ep is not None
    assert ep.start_event_ts == BASE_TS
    assert ep.end_event_ts   == BASE_TS + 10_000
    assert ep.episode_id     == f"BTCUSDT_{BASE_TS}"
    # duration 可由 start/end 重新计算，无信息损失
    assert ep.duration_ms == ep.end_event_ts - ep.start_event_ts


# ── 6. 统计计数 ────────────────────────────────────────────────────────────────

def test_stats_tracking():
    """stats() 正确维护 emitted / discarded / active 计数。"""
    builder = EpisodeBuilder(gap_ms=30_000, noise_threshold_usdt=300_000)

    # episode 1：100k < 300k → 丢弃
    _feed(builder, offset=0, notional=100_000.0)
    builder.flush()

    # episode 2：两笔各 200k = 400k > 300k → 发出
    _feed(builder, offset=100_000, notional=200_000.0)
    _feed(builder, offset=105_000, notional=200_000.0)

    assert builder.stats["active"] == 1   # 当前仍有活跃 episode
    builder.flush()

    assert builder.stats["emitted"]   == 1
    assert builder.stats["discarded"] == 1
    assert builder.stats["active"]    == 0
