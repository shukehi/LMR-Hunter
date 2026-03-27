"""
Episode 模型：将连续强平事件聚合为一个研究样本。

设计原则（阶段 3）：
  - 研究单位是 episode（一次连续冲击），而非单笔强平
  - 相邻两笔强平的间隔超过 gap_ms，或 episode 总持续超过 max_duration_ms → 关闭当前 episode
  - 总名义价值低于 noise_threshold_usdt → 视为噪声，不生成研究样本（discarded）
  - 所有时间戳均为交易所 event_ts (ms)，与阶段 2 时间语义保持一致

主接口：
  - EpisodeBuilder.feed(...)  — 每次收到强平事件时调用
    返回 None（episode 仍在进行）或 LiquidationEpisode（刚完成的 episode）
  - EpisodeBuilder.flush()   — 系统关闭时调用，强制关闭并返回当前未完成的 episode

可追溯性：
  - 通过 episode 的 start_event_ts / end_event_ts + symbol + side，可在 raw_liquidations 表中
    精确回放该 episode 包含的所有原始强平事件，无需额外关联字段
"""
from __future__ import annotations

from dataclasses import dataclass


# ── 公开数据模型 ───────────────────────────────────────────────────────────────

@dataclass(slots=True)
class LiquidationEpisode:
    """
    一次强平 episode 的完整研究样本。

    字段说明：
      - max_deviation_bps: episode 期间观测到的最大负偏离（数值最小，通常为负）
        例如 -200.0 表示价格曾低于 VWAP 达 200bps，比 -100.0 代表更深的超跌
      - pre_event_vwap: 首笔强平时的 VWAP，作为事前基准（不受后续强平冲击影响）
      - episode 可追溯性：
        SELECT * FROM raw_liquidations
        WHERE event_ts BETWEEN start_event_ts AND end_event_ts
          AND symbol = symbol AND side = side
    """
    episode_id:           str           # "{symbol}_{start_event_ts}"
    symbol:               str
    side:                 str           # SELL=多头被强平, BUY=空头被强平
    start_event_ts:       int           # 首笔强平的交易所时间戳 (ms)
    end_event_ts:         int           # 末笔强平的交易所时间戳 (ms)
    duration_ms:          int           # end_event_ts - start_event_ts
    liq_count:            int           # 本 episode 包含的强平笔数
    liq_notional_total:   float         # 所有强平的名义价值之和 (USDT)
    liq_peak_window:      float         # episode 期间最高滚动窗口值 (USDT)
    liq_accel_ratio_peak: float | None  # episode 期间最高加速率（None = 无可用数据）
    min_mid_price:        float | None  # episode 期间盘口中间价最低值（最深跌点）
    pre_event_vwap:       float | None  # 首笔强平时的 VWAP（None = 冷启动期间）
    max_deviation_bps:    float | None  # episode 期间最大负偏离 bps（数值最小）


# ── 内部状态（不对外暴露）────────────────────────────────────────────────────────

class _ActiveEpisode:
    """episode 在未关闭时的可变聚合状态。不应由外部代码直接使用。"""

    __slots__ = (
        "symbol", "side",
        "start_event_ts", "last_event_ts",
        "liq_count", "liq_notional_total",
        "liq_peak_window", "liq_accel_ratio_peak",
        "min_mid_price", "pre_event_vwap",
        "max_deviation_bps",
    )

    def __init__(self, symbol: str, side: str, start_event_ts: int) -> None:
        self.symbol               = symbol
        self.side                 = side
        self.start_event_ts       = start_event_ts
        self.last_event_ts        = start_event_ts
        self.liq_count            = 0
        self.liq_notional_total   = 0.0
        self.liq_peak_window      = 0.0
        self.liq_accel_ratio_peak: float | None = None
        self.min_mid_price:        float | None = None
        self.pre_event_vwap:       float | None = None   # 首笔强平时的 VWAP，之后不再更新
        self.max_deviation_bps:    float | None = None   # 跟踪最大超跌（最小的 bps 值）

    def update(
        self,
        event_ts:            int,
        notional:            float,
        liq_notional_window: float,
        liq_accel_ratio:     float | None,
        mid_price:           float | None,
        deviation_bps:       float | None,
        vwap_15m:            float | None,
    ) -> None:
        """将一笔强平事件合并入当前 episode。"""
        self.last_event_ts       = event_ts
        self.liq_count          += 1
        self.liq_notional_total += notional

        # 峰值滚动窗口
        if liq_notional_window > self.liq_peak_window:
            self.liq_peak_window = liq_notional_window

        # 峰值加速率
        if liq_accel_ratio is not None:
            if self.liq_accel_ratio_peak is None or liq_accel_ratio > self.liq_accel_ratio_peak:
                self.liq_accel_ratio_peak = liq_accel_ratio

        # 最低盘口中间价
        if mid_price is not None:
            if self.min_mid_price is None or mid_price < self.min_mid_price:
                self.min_mid_price = mid_price

        # 最大负偏离（数值最小的 deviation_bps）
        if deviation_bps is not None:
            if self.max_deviation_bps is None or deviation_bps < self.max_deviation_bps:
                self.max_deviation_bps = deviation_bps

        # pre_event_vwap 只记录首笔事件时的 VWAP（liq_count 从 0 递增后为 1 即为首笔）
        if self.liq_count == 1 and vwap_15m is not None:
            self.pre_event_vwap = vwap_15m

    def to_episode(self) -> LiquidationEpisode:
        return LiquidationEpisode(
            episode_id           = f"{self.symbol}_{self.start_event_ts}",
            symbol               = self.symbol,
            side                 = self.side,
            start_event_ts       = self.start_event_ts,
            end_event_ts         = self.last_event_ts,
            duration_ms          = self.last_event_ts - self.start_event_ts,
            liq_count            = self.liq_count,
            liq_notional_total   = round(self.liq_notional_total, 2),
            liq_peak_window      = round(self.liq_peak_window, 2),
            liq_accel_ratio_peak = self.liq_accel_ratio_peak,
            min_mid_price        = self.min_mid_price,
            pre_event_vwap       = self.pre_event_vwap,
            max_deviation_bps    = self.max_deviation_bps,
        )


# ── 公开构建器 ─────────────────────────────────────────────────────────────────

class EpisodeBuilder:
    """
    将逐笔强平事件聚合为 episode（一次连续冲击 = 一条研究样本）。

    终止条件（满足任一即关闭当前 episode）：
      1. 两笔事件间隔 > gap_ms（默认 30 秒）
      2. episode 总持续时间 > max_duration_ms（默认 300 秒）

    过滤条件：
      - 总名义价值 < noise_threshold_usdt → 视为噪声，不生成样本（discarded）

    使用方式::

        builder = EpisodeBuilder()
        # 在 on_liquidation 回调中调用：
        completed = builder.feed(
            event_ts=liq.event_ts, symbol=liq.symbol, side=liq.side,
            notional=liq.notional, liq_notional_window=snap.liq_notional_window,
            liq_accel_ratio=snap.liq_accel_ratio, mid_price=snap.mid_price,
            deviation_bps=snap.deviation_bps, vwap_15m=snap.vwap_15m,
        )
        if completed is not None:
            await writer.write_episode(completed)
        # 在系统关闭时调用：
        last_ep = builder.flush()
        if last_ep:
            await writer.write_episode(last_ep)
    """

    def __init__(
        self,
        gap_ms:               int   = 30_000,    # 30 秒静默 → episode 结束
        noise_threshold_usdt: float = 50_000.0,  # 低于此门槛视为噪声
        max_duration_ms:      int   = 300_000,   # 单 episode 最长持续 5 分钟
    ) -> None:
        self._gap_ms             = gap_ms
        self._noise_threshold    = noise_threshold_usdt
        self._max_duration_ms    = max_duration_ms
        self._active:            _ActiveEpisode | None = None
        self._episodes_emitted   = 0
        self._episodes_discarded = 0

    # ── 属性 ──────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, int]:
        """返回 episode 统计（供 stats_reporter 记录）。"""
        return {
            "emitted":   self._episodes_emitted,
            "discarded": self._episodes_discarded,
            "active":    1 if self._active is not None else 0,
        }

    @property
    def has_active(self) -> bool:
        return self._active is not None

    # ── 主接口 ────────────────────────────────────────────────────────────────

    def feed(
        self,
        event_ts:            int,
        symbol:              str,
        side:                str,
        notional:            float,
        liq_notional_window: float,
        liq_accel_ratio:     float | None,
        mid_price:           float | None,
        deviation_bps:       float | None,
        vwap_15m:            float | None,
    ) -> LiquidationEpisode | None:
        """
        喂入一笔强平事件。

        返回值：
          - None  — 当前 episode 仍在进行中，或刚开始新 episode
          - LiquidationEpisode — 因此次事件触发了 gap/duration 终止条件，
            前一个 episode 刚关闭，返回其聚合结果（当前事件已进入新 episode）
        """
        completed: LiquidationEpisode | None = None

        if self._active is not None:
            gap_ms      = event_ts - self._active.last_event_ts
            duration_ms = event_ts - self._active.start_event_ts

            if gap_ms >= self._gap_ms or duration_ms >= self._max_duration_ms:
                # 终止条件满足 → 关闭当前 episode，开启新 episode
                completed    = self._close()
                self._active = _ActiveEpisode(symbol=symbol, side=side, start_event_ts=event_ts)
            # 否则：继续延伸当前 episode（self._active 不变）
        else:
            # 首次事件 → 开启新 episode
            self._active = _ActiveEpisode(symbol=symbol, side=side, start_event_ts=event_ts)

        # 将当前事件合并入活跃 episode（无论是新开还是继续）
        self._active.update(
            event_ts            = event_ts,
            notional            = notional,
            liq_notional_window = liq_notional_window,
            liq_accel_ratio     = liq_accel_ratio,
            mid_price           = mid_price,
            deviation_bps       = deviation_bps,
            vwap_15m            = vwap_15m,
        )

        return completed

    def flush(self) -> LiquidationEpisode | None:
        """
        强制关闭当前活跃 episode（在系统关闭时调用）。
        低于噪声门槛的 episode 同样被丢弃。
        """
        if self._active is None:
            return None
        return self._close()

    # ── 内部 ──────────────────────────────────────────────────────────────────

    def _close(self) -> LiquidationEpisode | None:
        """关闭 self._active，返回聚合结果（或 None 若低于噪声门槛）。"""
        ep           = self._active
        self._active = None
        if ep is None:
            return None
        if ep.liq_notional_total < self._noise_threshold:
            self._episodes_discarded += 1
            return None
        self._episodes_emitted += 1
        return ep.to_episode()
