"""
特征计算引擎（纯内存，无 I/O）。

计算的特征：
  1. liq_notional_window  — 滑动时间窗口内的强平总名义价值
  2. liq_accel_ratio      — 加速率：当前窗口 vs 前一窗口（> 1.0 = 加速，策略应阻断）
  3. vwap_15m             — 基于最近 15 根已收盘 1m K 线的 VWAP
  4. deviation_bps        — (mid_price - vwap_15m) / vwap_15m × 10000

设计原则：
  - 所有状态保存在内存中，无数据库读取
  - 输出是纯数据类，不触发任何副作用
  - 冷启动（K 线不足 15 根时）返回 None，明确标注不可用
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

from src.gateway.models import Kline, Liquidation


@dataclass(slots=True)
class FeatureSnapshot:
    """
    单次特征计算的完整快照，供 main.py 记录和判断信号。
    None 表示该特征当前不可用（数据不足）。
    """
    ts:                   int           # 计算时的本地时间戳 (ms)
    liq_notional_window:  float         # 当前窗口强平总量 (USDT)
    liq_accel_ratio:      float | None  # 加速率（None = 前窗口无数据）
    vwap_15m:             float | None  # VWAP（None = K 线不足 15 根）
    mid_price:            float | None  # 当前盘口中间价
    deviation_bps:        float | None  # 偏离率（None = VWAP 不可用）
    kline_count:          int           # 当前持有的已收盘 K 线数量

    @property
    def is_vwap_ready(self) -> bool:
        return self.vwap_15m is not None

    @property
    def is_accelerating(self) -> bool:
        """True = 强平在加速，策略衰减层应阻断入场。"""
        return self.liq_accel_ratio is not None and self.liq_accel_ratio > 1.0


class FeatureCalculator:
    """
    维护滑动窗口状态，提供按需计算接口。

    使用方式：
        calc = FeatureCalculator(liq_window_sec=5)
        # 喂入 K 线（包括冷启动历史数据）
        calc.on_kline(kline)
        # 喂入强平事件
        calc.on_liquidation(liq)
        # 更新最新盘口价格
        calc.update_mid_price(mid_price)
        # 获取当前特征快照
        snapshot = calc.snapshot()
    """

    def __init__(self, liq_window_sec: int = 5) -> None:
        self._liq_window_ms = liq_window_sec * 1000

        # 强平窗口：deque of (event_ts_ms, notional)
        # 保留足够长的历史以支持双窗口（当前 + 前一个）
        self._liq_events: deque[tuple[int, float]] = deque()

        # K 线队列：只保留最近 15 根已收盘 K 线
        self._klines: deque[Kline] = deque(maxlen=15)

        # 当前盘口中间价（由 depth 回调更新）
        self._mid_price: float | None = None

        # 缓存的 VWAP（避免每次都重算）
        self._vwap_cache: float | None = None
        self._vwap_dirty: bool = True  # K 线更新后设为 True

    # ── 数据喂入接口 ──────────────────────────────────────────────────────────

    def on_liquidation(self, liq: Liquidation) -> None:
        """记录一个强平事件。"""
        self._liq_events.append((liq.event_ts, liq.notional))

    def on_kline(self, kline: Kline) -> None:
        """更新 K 线队列（只接受已收盘的 K 线，进行中的不计入 VWAP）。"""
        if not kline.is_closed:
            return
        # 避免重复（重连后可能收到重复数据）
        if self._klines and self._klines[-1].open_ts == kline.open_ts:
            self._klines[-1] = kline  # 覆盖（理论上已收盘不会变，但防御性处理）
        else:
            self._klines.append(kline)
        self._vwap_dirty = True

    def update_mid_price(self, mid_price: float) -> None:
        self._mid_price = mid_price

    # ── 计算接口 ──────────────────────────────────────────────────────────────

    def snapshot(self) -> FeatureSnapshot:
        """计算并返回当前时刻的完整特征快照。"""
        now_ms = int(time.time() * 1000)

        liq_current, liq_prev = self._liquidation_windows(now_ms)
        accel_ratio = self._acceleration_ratio(liq_current, liq_prev)
        vwap = self._vwap()
        deviation = self._deviation(self._mid_price, vwap)

        return FeatureSnapshot(
            ts=now_ms,
            liq_notional_window=liq_current,
            liq_accel_ratio=accel_ratio,
            vwap_15m=vwap,
            mid_price=self._mid_price,
            deviation_bps=deviation,
            kline_count=len(self._klines),
        )

    # ── 内部计算 ──────────────────────────────────────────────────────────────

    def _liquidation_windows(self, now_ms: int) -> tuple[float, float]:
        """
        返回 (当前窗口总量, 前一窗口总量)。

        当前窗口：[now - window_ms, now]
        前一窗口：[now - 2*window_ms, now - window_ms]
        """
        current_start = now_ms - self._liq_window_ms
        prev_start    = now_ms - 2 * self._liq_window_ms

        # 清除超出双窗口范围的旧数据
        while self._liq_events and self._liq_events[0][0] < prev_start:
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
        """
        if mid_price is None or vwap is None or vwap <= 0:
            return None
        return round((mid_price - vwap) / vwap * 10_000, 1)
