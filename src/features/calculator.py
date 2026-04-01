"""
特征计算引擎（纯内存，无 I/O）。

计算的特征（微观结构重构后）：
  核心特征（基于第一性原理推导链）:
    1. basis_bps        — (perp_mid_price - index_price) / index_price × 10000
    2. impact_ratio     — liq_notional_window / bid_depth_usdt
    3. taker_buy_ratio  — 窗口内主动买成交占比（订单流失衡指标）

  辅助特征（保留向后兼容）:
    4. liq_notional_window  — 滑动时间窗口内的强平总名义价值
    5. liq_accel_ratio      — 加速率：当前窗口 vs 前一窗口
    6. vwap_15m             — [DEPRECATED] 基于最近 15 根已收盘 1m K 线的 VWAP
    7. deviation_bps        — [DEPRECATED] (mid_price - vwap_15m) / vwap_15m × 10000

设计原则：
  - 所有窗口计算以 exchange event_ts 为锚点
  - mid_price / index_price / bid_depth 缓存均携带时间戳，计算前校验新鲜度
  - 若数据距事件时间超过 MAX_STALE_MS，对应特征标记为不可用（返回 None）
  - snapshot_at(event_ts_ms) 是主接口
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

from src.gateway.models import Kline, Liquidation, Trade

# 数据"新鲜度"阈值 (ms)
MAX_STALE_MS: int = 5_000  # 5 秒


@dataclass(slots=True)
class FeatureSnapshot:
    """
    单次特征计算的完整快照，供 main.py 记录和判断信号。
    None 表示该特征当前不可用（数据不足或数据过时）。
    """
    ts:                   int           # 锚点事件时间戳 (exchange event_ts, ms)

    # ── 核心微观结构特征 ──────────────────────────────────────────────────────
    index_price:          float | None  # 现货指数价格（公允价值锚点）
    basis_bps:            float | None  # 期现基差 (bps)，负值 = 永续低于现货
    bid_depth_usdt:       float | None  # 当前买盘总深度 (USDT)
    impact_ratio:         float | None  # 冲击比例 = liq_notional_window / bid_depth_usdt
    taker_buy_ratio:      float | None  # 窗口内 Taker Buy 占比 (0-1)
    index_staleness_ms:   int | None    # index_price 与锚点的时间差

    # ── 强平窗口特征 ─────────────────────────────────────────────────────────
    liq_notional_window:  float         # 当前窗口强平总量 (USDT)
    liq_accel_ratio:      float | None  # 加速率（None = 前窗口无数据）

    # ── 盘口特征 ─────────────────────────────────────────────────────────────
    mid_price:            float | None  # 当前盘口中间价
    depth_event_ts:       int | None    # 最近一次 depth 更新的交易所时间戳 (ms)
    depth_staleness_ms:   int | None    # 盘口相对锚点的时间差

    # ── [DEPRECATED] VWAP 相关（保留向后兼容，不再用于信号判断）──────────────
    vwap_15m:             float | None  # VWAP（None = K 线不足 15 根）
    deviation_bps:        float | None  # VWAP 偏离率
    kline_count:          int           # 当前持有的已收盘 K 线数量

    @property
    def is_vwap_ready(self) -> bool:
        """[DEPRECATED] VWAP 仅供参考，不作为信号依据。"""
        return self.vwap_15m is not None

    @property
    def is_accelerating(self) -> bool:
        """True = 强平在加速，策略衰减层应阻断入场。"""
        return self.liq_accel_ratio is not None and self.liq_accel_ratio > 1.0

    @property
    def is_depth_fresh(self) -> bool:
        """True = 盘口足够新鲜，基差和冲击比例可信。"""
        return self.depth_staleness_ms is not None and self.depth_staleness_ms <= MAX_STALE_MS

    @property
    def is_index_fresh(self) -> bool:
        """True = 指数价格足够新鲜，基差可信。"""
        return self.index_staleness_ms is not None and self.index_staleness_ms <= MAX_STALE_MS

    @property
    def has_taker_buy_confirmation(self) -> bool:
        """True = 主动买盘在出现（套利者或抄底资金正在入场）。"""
        return self.taker_buy_ratio is not None and self.taker_buy_ratio > 0.3


class FeatureCalculator:
    """
    维护滑动窗口状态，提供按需计算接口。

    使用方式：
        calc = FeatureCalculator(liq_window_sec=5)
        # 喂入数据
        calc.on_kline(kline)                                  # K 线
        calc.on_liquidation(liq)                              # 强平
        calc.update_mid_price(mid_price, event_ts)            # 盘口中价
        calc.update_bid_depth(bid_depth_usdt, event_ts)       # 买盘深度
        calc.update_index_price(index_price, event_ts)        # 指数价格
        calc.on_trade_flow(trade)                             # 成交订单流
        # 以强平事件时间为锚点获取特征快照
        snapshot = calc.snapshot_at(liq.event_ts)
    """

    def __init__(self, liq_window_sec: int = 5) -> None:
        self._liq_window_ms = liq_window_sec * 1000

        # 强平窗口：deque of (event_ts_ms, notional)
        self._liq_events: deque[tuple[int, float]] = deque()
        self._max_liq_ts: int = 0  # BUG-9 修复：高水位标记

        # K 线队列：只保留最近 15 根已收盘 K 线
        self._klines: deque[Kline] = deque(maxlen=15)

        # 带时间戳的盘口中间价
        self._mid_price:    float | None = None
        self._mid_price_ts: int   | None = None

        # ── 微观结构重构新增 ──────────────────────────────────────────────────
        # 现货指数价格（由 markPrice@1s 流更新）
        self._index_price:    float | None = None
        self._index_price_ts: int   | None = None

        # 买盘深度（由 depth 回调提取）
        self._bid_depth_usdt:    float | None = None
        self._bid_depth_usdt_ts: int   | None = None

        # 成交订单流（Taker Buy / Taker Sell 分滑动窗口统计）
        # deque of (event_ts_ms, notional, is_taker_buy)
        self._trade_flow: deque[tuple[int, float, bool]] = deque()

        # 缓存的 VWAP（向后兼容）
        self._vwap_cache: float | None = None
        self._vwap_dirty: bool = True

    # ── 数据喂入接口 ──────────────────────────────────────────────────────────

    def on_liquidation(self, liq: Liquidation) -> None:
        """记录一个强平事件（使用交易所 event_ts）。"""
        self._liq_events.append((liq.event_ts, liq.notional))

    def on_kline(self, kline: Kline) -> None:
        """更新 K 线队列（只接受已收盘的 K 线，进行中的不计入 VWAP）。"""
        if not kline.is_closed:
            return
        if self._klines and self._klines[-1].open_ts == kline.open_ts:
            self._klines[-1] = kline
        else:
            self._klines.append(kline)
        self._vwap_dirty = True

    def update_mid_price(self, mid_price: float, event_ts: int) -> None:
        """更新盘口中间价。0.0 不是有效价格，拒绝更新。"""
        if mid_price <= 0:
            return
        self._mid_price    = mid_price
        self._mid_price_ts = event_ts

    def update_index_price(self, index_price: float, event_ts: int) -> None:
        """
        更新现货指数价格（由 markPrice@1s 流触发）。

        这是基差计算 (Basis_Bps) 的核心输入，替代了已废弃的 VWAP 作为公允价值锚点。
        """
        if index_price <= 0:
            return
        self._index_price    = index_price
        self._index_price_ts = event_ts

    def update_bid_depth(self, bid_depth_usdt: float, event_ts: int) -> None:
        """
        更新买盘总深度 (USDT)（由 depth 回调提取 Depth.bid_depth_usdt）。

        这是冲击比例 (Impact Ratio) 计算的核心输入。
        """
        if bid_depth_usdt <= 0:
            return
        self._bid_depth_usdt    = bid_depth_usdt
        self._bid_depth_usdt_ts = event_ts

    def on_trade_flow(self, trade: Trade) -> None:
        """
        记录一笔成交的订单流方向（Taker Buy vs Taker Sell）。

        用于衰减层的主动买盘确认：不只看强平是否停歇，
        还要看是否有主动买入（套利者/抄底者入场）。
        """
        notional = trade.price * trade.qty
        # is_buyer_maker=True → 主动卖（Taker Sell）
        # is_buyer_maker=False → 主动买（Taker Buy）
        is_taker_buy = not trade.is_buyer_maker
        self._trade_flow.append((trade.trade_ts, notional, is_taker_buy))

    # ── 计算接口 ──────────────────────────────────────────────────────────────

    def snapshot_at(self, event_ts_ms: int) -> FeatureSnapshot:
        """
        以指定的交易所事件时间为锚点，计算完整特征快照。

        这是主接口。传入强平事件的 event_ts_ms，
        所有时间窗口均以此锚点为基准，消除链路延迟的影响。
        """
        liq_current, liq_prev = self._liquidation_windows(event_ts_ms)
        accel_ratio = self._acceleration_ratio(liq_current, liq_prev)
        vwap = self._vwap()

        # ── 盘口新鲜度 ───────────────────────────────────────────────────────
        depth_staleness_ms: int | None = None
        effective_mid: float | None = None

        if self._mid_price is not None and self._mid_price_ts is not None:
            depth_staleness_ms = abs(event_ts_ms - self._mid_price_ts)
            effective_mid = self._mid_price if depth_staleness_ms <= MAX_STALE_MS else None
        elif self._mid_price is not None:
            effective_mid = self._mid_price

        # ── 指数价格新鲜度 ───────────────────────────────────────────────────
        index_staleness_ms: int | None = None
        effective_index: float | None = None

        if self._index_price is not None and self._index_price_ts is not None:
            index_staleness_ms = abs(event_ts_ms - self._index_price_ts)
            effective_index = self._index_price if index_staleness_ms <= MAX_STALE_MS else None

        # ── 基差 (Basis) ─────────────────────────────────────────────────────
        basis_bps = self._basis_bps(effective_mid, effective_index)

        # ── 买盘深度 & 冲击比例 ──────────────────────────────────────────────
        effective_bid_depth: float | None = None
        if self._bid_depth_usdt is not None and self._bid_depth_usdt_ts is not None:
            bid_staleness = abs(event_ts_ms - self._bid_depth_usdt_ts)
            if bid_staleness <= MAX_STALE_MS:
                effective_bid_depth = self._bid_depth_usdt

        impact_ratio = self._impact_ratio(liq_current, effective_bid_depth)

        # ── Taker Buy 占比 ───────────────────────────────────────────────────
        taker_buy_ratio = self._taker_buy_ratio(event_ts_ms)

        # ── [DEPRECATED] VWAP 偏离率 ─────────────────────────────────────────
        deviation = self._deviation(effective_mid, vwap)

        return FeatureSnapshot(
            ts=event_ts_ms,
            # 核心微观结构
            index_price=effective_index,
            basis_bps=basis_bps,
            bid_depth_usdt=effective_bid_depth,
            impact_ratio=impact_ratio,
            taker_buy_ratio=taker_buy_ratio,
            index_staleness_ms=index_staleness_ms,
            # 强平窗口
            liq_notional_window=liq_current,
            liq_accel_ratio=accel_ratio,
            # 盘口
            mid_price=effective_mid,
            depth_event_ts=self._mid_price_ts,
            depth_staleness_ms=depth_staleness_ms,
            # DEPRECATED VWAP
            vwap_15m=vwap,
            deviation_bps=deviation,
            kline_count=len(self._klines),
        )

    def snapshot(self) -> FeatureSnapshot:
        """
        以本地当前时间为锚点返回特征快照（兜底接口）。

        注意：此接口会将本地延迟混入时间窗口，仅用于：
          - 无明确事件锚点的场景（如 stats_reporter 的周期性监控）
        生产信号路径应使用 snapshot_at(liq.event_ts)。
        """
        return self.snapshot_at(int(time.time() * 1000))

    # ── 内部计算 ──────────────────────────────────────────────────────────────

    def _liquidation_windows(self, now_ms: int) -> tuple[float, float]:
        """返回 (当前窗口总量, 前一窗口总量)。"""
        current_start = now_ms - self._liq_window_ms
        prev_start    = now_ms - 2 * self._liq_window_ms

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
        """计算加速率。prev 为 0 → None（无法计算）。"""
        if prev <= 0:
            return None
        return round(current / prev, 3)

    @staticmethod
    def _basis_bps(mid_price: float | None, index_price: float | None) -> float | None:
        """
        计算期现基差 (Basis_Bps)。

        Basis_Bps = (Perp_Mid_Price - Index_Price) / Index_Price × 10000

        负值 = 永续低于现货（强平砸出的坑，套利空间）
        正值 = 永续高于现货
        """
        if mid_price is None or index_price is None or index_price <= 0:
            return None
        return round((mid_price - index_price) / index_price * 10_000, 1)

    @staticmethod
    def _impact_ratio(liq_notional: float, bid_depth: float | None) -> float | None:
        """
        计算冲击比例 (Impact Ratio)。

        Impact Ratio = 强平金额 / 买盘深度

        > 1.0 = 强平已击穿当前挂单防线，真实流动性真空
        """
        if bid_depth is None or bid_depth <= 0:
            return None
        if liq_notional <= 0:
            return 0.0
        return round(liq_notional / bid_depth, 3)

    def _taker_buy_ratio(self, now_ms: int) -> float | None:
        """
        计算滑动窗口内的 Taker Buy 成交额占比。

        用于微观衰减确认：必须看到主动买盘入场（而非仅强平暂停），
        才认为修复动力已出现。
        """
        cutoff = now_ms - self._liq_window_ms

        # 清理过期数据
        while self._trade_flow and self._trade_flow[0][0] < cutoff:
            self._trade_flow.popleft()

        if not self._trade_flow:
            return None

        total_buy = 0.0
        total_all = 0.0
        for ts, notional, is_buy in self._trade_flow:
            if ts >= cutoff:
                total_all += notional
                if is_buy:
                    total_buy += notional

        if total_all <= 0:
            return None
        return round(total_buy / total_all, 3)

    def _vwap(self) -> float | None:
        """
        [DEPRECATED] 基于最近 15 根已收盘 K 线的 VWAP。

        策略文档 §12.1 已冻结废弃 VWAP。保留此方法仅供向后兼容和参考对比。
        信号判断和研究结论不应依赖此值。
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
        [DEPRECATED] 计算当前价格相对 VWAP 的偏离率 (bps)。

        策略文档 §12.1 已冻结废弃 VWAP 偏离率。
        保留此方法仅供向后兼容。信号判断应使用 basis_bps。
        """
        if mid_price is None or vwap is None or vwap <= 0:
            return None
        return round((mid_price - vwap) / vwap * 10_000, 1)

