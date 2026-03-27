"""
特征计算引擎（纯内存，无 I/O）。

计算的特征：
  1. liq_notional_window  — 滑动时间窗口内的强平总名义价值
  2. liq_accel_ratio      — 加速率：当前窗口 vs 前一窗口（> 1.0 = 加速，策略应阻断）
  3. vwap_15m             — 基于最近 15 根已收盘 1m K 线的 VWAP
  4. deviation_bps        — (mid_price - vwap_15m) / vwap_15m × 10000

设计原则（阶段 2 更新）：
  - 所有窗口计算以 exchange event_ts 为锚点，而非本地 time.time()
  - mid_price 缓存携带时间戳，偏离率计算前校验"新鲜度"
  - 若盘口距事件时间超过 MAX_STALE_MS，偏离率标记为不可用（返回 None）
  - 冷启动（K 线不足 15 根时）vwap 返回 None，明确标注不可用
  - snapshot_at(event_ts_ms) 是主接口；snapshot() 仅供无锚点时的兜底使用
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

from src.gateway.models import Kline, Liquidation

# 盘口"新鲜度"阈值：若最新 depth event_ts 与强平 event_ts 之差超过此值，
# 认为盘口数据过时，不输出偏离率研究样本（避免将链路延迟误判为市场结构）
MAX_STALE_MS: int = 5_000  # 5 秒


@dataclass(slots=True)
class FeatureSnapshot:
    """
    单次特征计算的完整快照，供 main.py 记录和判断信号。
    None 表示该特征当前不可用（数据不足或盘口过时）。
    """
    ts:                   int           # 锚点事件时间戳 (exchange event_ts, ms)
    liq_notional_window:  float         # 当前窗口强平总量 (USDT)
    liq_accel_ratio:      float | None  # 加速率（None = 前窗口无数据）
    vwap_15m:             float | None  # VWAP（None = K 线不足 15 根）
    mid_price:            float | None  # 当前盘口中间价
    deviation_bps:        float | None  # 偏离率（None = VWAP 不可用或盘口过时）
    kline_count:          int           # 当前持有的已收盘 K 线数量

    # 阶段 2 新增：盘口时间戳及新鲜度，供下游归因分析
    depth_event_ts:       int | None    # 最近一次 depth 更新的交易所时间戳 (ms)
    depth_staleness_ms:   int | None    # 盘口相对锚点的时间差（正 = 盘口更旧）

    @property
    def is_vwap_ready(self) -> bool:
        return self.vwap_15m is not None

    @property
    def is_accelerating(self) -> bool:
        """True = 强平在加速，策略衰减层应阻断入场。"""
        return self.liq_accel_ratio is not None and self.liq_accel_ratio > 1.0

    @property
    def is_depth_fresh(self) -> bool:
        """True = 盘口足够新鲜，偏离率可信。"""
        return self.depth_staleness_ms is not None and self.depth_staleness_ms <= MAX_STALE_MS


class FeatureCalculator:
    """
    维护滑动窗口状态，提供按需计算接口。

    使用方式：
        calc = FeatureCalculator(liq_window_sec=5)
        # 喂入 K 线（包括冷启动历史数据）
        calc.on_kline(kline)
        # 喂入强平事件
        calc.on_liquidation(liq)
        # 更新最新盘口价格（携带交易所事件时间戳）
        calc.update_mid_price(mid_price, event_ts)
        # 以强平事件时间为锚点获取特征快照
        snapshot = calc.snapshot_at(liq.event_ts)
    """

    def __init__(self, liq_window_sec: int = 5) -> None:
        self._liq_window_ms = liq_window_sec * 1000

        # 强平窗口：deque of (event_ts_ms, notional)
        # 保留足够长的历史以支持双窗口（当前 + 前一个）
        self._liq_events: deque[tuple[int, float]] = deque()

        # BUG-9 修复：高水位标记，驱逐只基于见过的最大时间戳
        # 防止乱序事件导致较小 now_ms 的调用错误驱逐仍需要的历史数据
        self._max_liq_ts: int = 0

        # K 线队列：只保留最近 15 根已收盘 K 线
        self._klines: deque[Kline] = deque(maxlen=15)

        # 带时间戳的盘口中间价（由 depth 回调更新）
        # 阶段 2：同时存储 depth event_ts（交易所时间），用于新鲜度校验
        self._mid_price:    float | None = None
        self._mid_price_ts: int   | None = None   # depth event_ts (ms)

        # 缓存的 VWAP（避免每次都重算）
        self._vwap_cache: float | None = None
        self._vwap_dirty: bool = True  # K 线更新后设为 True

    # ── 数据喂入接口 ──────────────────────────────────────────────────────────

    def on_liquidation(self, liq: Liquidation) -> None:
        """记录一个强平事件（使用交易所 event_ts）。"""
        self._liq_events.append((liq.event_ts, liq.notional))

    def on_kline(self, kline: Kline) -> None:
        """更新 K 线队列（只接受已收盘的 K 线，进行中的不计入 VWAP）。"""
        if not kline.is_closed:
            return
        # 避免重复（重连后可能收到重复数据）
        if self._klines and self._klines[-1].open_ts == kline.open_ts:
            self._klines[-1] = kline  # 覆盖（防御性处理）
        else:
            self._klines.append(kline)
        self._vwap_dirty = True

    def update_mid_price(self, mid_price: float, event_ts: int) -> None:
        """
        更新盘口中间价。

        阶段 2：必须传入 depth 的交易所事件时间戳（event_ts），
        而非本地收到时间，以便后续计算偏离率时校验新鲜度。

        防护：空盘口（bids/asks 为空）会导致 mid_price=0.0，
        0.0 不是有效价格，拒绝更新以避免 deviation_bps=-10000bps 污染研究数据。
        """
        if mid_price <= 0:
            return
        self._mid_price    = mid_price
        self._mid_price_ts = event_ts

    # ── 计算接口 ──────────────────────────────────────────────────────────────

    def snapshot_at(self, event_ts_ms: int) -> FeatureSnapshot:
        """
        以指定的交易所事件时间为锚点，计算完整特征快照。

        这是阶段 2 之后的主接口。传入强平事件的 event_ts_ms，
        所有时间窗口均以此锚点为基准，消除链路延迟的影响。

        参数：
            event_ts_ms: 强平/信号事件的交易所时间戳 (ms)

        返回：
            FeatureSnapshot，其中 ts 为 event_ts_ms
        """
        liq_current, liq_prev = self._liquidation_windows(event_ts_ms)
        accel_ratio = self._acceleration_ratio(liq_current, liq_prev)
        vwap = self._vwap()

        # 盘口新鲜度校验
        staleness_ms: int | None = None
        effective_mid: float | None = None

        if self._mid_price is not None and self._mid_price_ts is not None:
            staleness_ms   = abs(event_ts_ms - self._mid_price_ts)
            effective_mid  = self._mid_price if staleness_ms <= MAX_STALE_MS else None
        elif self._mid_price is not None:
            # 旧版路径兼容：有 mid_price 但无时间戳（不应出现，但防御）
            effective_mid = self._mid_price

        deviation = self._deviation(effective_mid, vwap)

        return FeatureSnapshot(
            ts=event_ts_ms,
            liq_notional_window=liq_current,
            liq_accel_ratio=accel_ratio,
            vwap_15m=vwap,
            mid_price=effective_mid,
            deviation_bps=deviation,
            kline_count=len(self._klines),
            depth_event_ts=self._mid_price_ts,
            depth_staleness_ms=staleness_ms,
        )

    def snapshot(self) -> FeatureSnapshot:
        """
        以本地当前时间为锚点返回特征快照（兜底接口）。

        注意：此接口会将本地延迟混入时间窗口，仅用于：
          - 无明确事件锚点的场景（如 stats_reporter 的周期性监控）
          - 已废弃路径的向后兼容

        生产信号路径应使用 snapshot_at(liq.event_ts)。
        """
        return self.snapshot_at(int(time.time() * 1000))

    # ── 内部计算 ──────────────────────────────────────────────────────────────

    def _liquidation_windows(self, now_ms: int) -> tuple[float, float]:
        """
        返回 (当前窗口总量, 前一窗口总量)。

        当前窗口：[now_ms - window_ms, now_ms]
        前一窗口：[now_ms - 2*window_ms, now_ms - window_ms]

        阶段 2：now_ms 为传入的事件锚点，而非 time.time()
        """
        current_start = now_ms - self._liq_window_ms
        prev_start    = now_ms - 2 * self._liq_window_ms

        # BUG-9 修复：只在高水位时才驱逐，避免乱序事件的小 now_ms
        # 把已经被更大时间戳驱逐的数据再次错误地"恢复需要"
        self._max_liq_ts = max(self._max_liq_ts, now_ms)
        evict_before = self._max_liq_ts - 2 * self._liq_window_ms
        while self._liq_events and self._liq_events[0][0] < evict_before:
            self._liq_events.popleft()

        current_total = 0.0
        prev_total    = 0.0

        for ts, notional in self._liq_events:
            if ts >= current_start:
                current_total += notional
            elif ts >= prev_start:
                prev_total += notional

        return current_total, prev_total

    @staticmethod
    def _acceleration_ratio(current: float, prev: float) -> float | None:
        """
        计算加速率。

        - 前窗口为 0 → 无法计算，返回 None（不阻断，因为没有基准）
        - current / prev > 1.0 → 加速（策略衰减层应阻断）
        - current / prev ≤ 1.0 → 减速或持平（符合入场条件）
        """
        if prev <= 0:
            return None
        return round(current / prev, 3)

    def _vwap(self) -> float | None:
        """
        计算基于最近 15 根已收盘 K 线的 VWAP。

        VWAP = Σ(典型价格 × 成交量) / Σ(成交量)
        典型价格 = (high + low + close) / 3

        K 线不足 15 根时返回 None（冷启动期间）。
        """
        if len(self._klines) < 15:
            return None

        if not self._vwap_dirty and self._vwap_cache is not None:
            return self._vwap_cache

        total_vol = 0.0
        total_tp_vol = 0.0
        for k in self._klines:
            tp = k.typical_price
            total_tp_vol += tp * k.volume
            total_vol += k.volume

        if total_vol <= 0:
            return None

        self._vwap_cache = round(total_tp_vol / total_vol, 2)
        self._vwap_dirty = False
        return self._vwap_cache

    @staticmethod
    def _deviation(mid_price: float | None, vwap: float | None) -> float | None:
        """
        计算当前价格相对 VWAP 的偏离率（单位 bps）。

        负值 = 价格低于 VWAP（潜在的超跌修复机会）
        正值 = 价格高于 VWAP

        传入 None 时（盘口过时或 VWAP 不可用）返回 None，
        系统不输出研究样本，而不是输出错误数据。
        """
        if mid_price is None or vwap is None or vwap <= 0:
            return None
        return round((mid_price - vwap) / vwap * 10_000, 1)
