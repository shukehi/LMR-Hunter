"""
特征计算引擎（纯内存，无 I/O）。

计算的特征（微观结构重构后）：
  核心特征（基于第一性原理推导链）:
    1. basis_bps             — (perp_mid_price - index_price) / index_price × 10000
    2. impact_ratio          — liq_notional_window / bid_depth_usdt
    3. taker_buy_ratio       — 窗口内主动买成交占比（订单流失衡指标）
    4. taker_sell_peak_180s  — 过去 180s 内 30s 子窗口 taker_sell_ratio 的峰值（前置预警）
    5. taker_buy_rising      — 近 10s 买方比例 > 前 10-30s 买方比例（入场确认）

  辅助特征（保留向后兼容）:
    6. liq_notional_window  — 滑动时间窗口内的强平总名义价值
    7. liq_accel_ratio      — 加速率：当前窗口 vs 前一窗口
    8. vwap_15m             — [DEPRECATED] 基于最近 15 根已收盘 1m K 线的 VWAP
    9. deviation_bps        — [DEPRECATED] (mid_price - vwap_15m) / vwap_15m × 10000

设计原则：
  - 所有窗口计算以 exchange event_ts 为锚点
  - mid_price / index_price / bid_depth 缓存均携带时间戳，计算前校验新鲜度
  - 各特征有独立 freshness 阈值（MAX_INDEX/MID/DEPTH_STALE_MS），不共用单一上界
  - bid_depth 额外施加因果方向约束：depth.event_ts 必须 <= liq.event_ts
  - 若数据超出对应阈值，该特征标记为不可用（返回 None）
  - snapshot_at(event_ts_ms) 是主接口

Phase 5 信号时序设计：
  trade_flow 保留 _TAKER_FLOW_MAX_LOOKBACK_MS（180s）的历史数据，
  供 taker_sell_peak_in_window 检测前置卖压 spike。
  taker_buy_ratio 仍使用 liq_window_ms（5s）窗口，
  taker_buy_rising 使用 [0-10s] vs [10-30s] 的比较。
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

from src.gateway.models import Kline, Liquidation, Trade

# ── 数据新鲜度阈值（各特征独立，对齐其物理因果语义）──────────────────────────────────
#
# 设计原则（第一性原理）：
#   每个特征的 freshness 预算由其数据源的推送频率和因果语义共同决定，
#   不允许用统一阈值掩盖不同特征的不同时效要求。
#
# MAX_INDEX_STALE_MS = 2000ms
#   依据：markPrice@1s 流最大间隔 1s + 1s 网络/处理裕量
#   因果语义：index_price 表示当前公允价值，2s 内仍可信
#
# MAX_MID_STALE_MS = 500ms
#   依据：depth 更新频率 ~100ms，mid_price 应近实时
#   因果语义：mid_price 用于盘口基差计算，允许 500ms 误差
#
# MAX_DEPTH_STALE_MS = 1500ms（过渡方案）
#   依据：当前 depth stream 1s 降采样，1s + 500ms 处理裕量
#   因果语义：impact_ratio 要求使用强平「之前」的深度
#   长期目标：depth stream 升频至 ≤100ms 后，此值收紧至 100ms
#   注意：此阈值仅限制「多旧的深度可用」，不改变「必须早于强平」的因果约束
#
# MAX_STALE_MS 保留为向后兼容别名，等于最宽松的 MAX_INDEX_STALE_MS
MAX_INDEX_STALE_MS: int = 2_000   # index_price：2s（markPrice@1s 流）
MAX_MID_STALE_MS:   int = 500     # mid_price：500ms（盘口近实时）
MAX_DEPTH_STALE_MS: int = 1_500   # bid_depth：1.5s（过渡方案，待 depth 升频后收紧至 100ms）
MAX_STALE_MS:       int = MAX_INDEX_STALE_MS  # 向后兼容别名，勿用于新代码

# ── Phase 5：信号时序 — trade_flow 保留窗口配置 ───────────────────────────────────
#
# taker_sell_peak_in_window 需要回溯 180s（前置预警窗口），
# 因此 trade_flow 必须至少保留 180s 的数据。
# cleanup 不再在 _taker_buy_ratio 内部执行，改为在 snapshot_at 顶部统一清理。
_TAKER_FLOW_MAX_LOOKBACK_MS: int = 180_000  # trade_flow 最大保留时间（决定内存上界）
_TAKER_SELL_SUBWIN_MS:       int = 30_000   # taker_sell_peak 子窗口大小（30s）
_TAKER_BUY_RECENT_MS:        int = 10_000   # taker_buy_rising 近期窗口（0-10s）
_TAKER_BUY_PRIOR_MS:         int = 30_000   # taker_buy_rising 对比窗口（10-30s）


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

    # ── Phase 5：信号时序特征 ──────────────────────────────────────────────────
    taker_sell_peak_180s: float | None  # 前置预警：过去 180s 内 30s 子窗口 taker_sell_ratio 峰值
    #   None = 180s 内无任何成交数据（冷启动或数据断流）
    #   用于 L1：`taker_sell_peak_180s > 0.65` → 前置卖压已出现
    taker_buy_rising:     bool          # 入场确认：近 10s 买方比例 > 前 10-30s 买方比例
    #   False = 买方力量未回升（强平仍在持续 or 数据不足）
    #   用于 L4：`taker_buy_rising=True` → 套利/抄底资金正在入场

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
        """True = 盘口中价足够新鲜（≤ MAX_MID_STALE_MS）。"""
        return self.depth_staleness_ms is not None and self.depth_staleness_ms <= MAX_MID_STALE_MS

    @property
    def is_index_fresh(self) -> bool:
        """True = 指数价格足够新鲜（≤ MAX_INDEX_STALE_MS），基差可信。"""
        return self.index_staleness_ms is not None and self.index_staleness_ms <= MAX_INDEX_STALE_MS

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

        时间因果约束（第一性原理）：
          - mid_price：≤ MAX_MID_STALE_MS (500ms) 内的快照
          - index_price：≤ MAX_INDEX_STALE_MS (2000ms) 内（markPrice@1s 推送频率）
          - bid_depth：双重约束：
              1. depth.event_ts <= liq.event_ts（因果方向，绝对约束）
              2. liq.event_ts - depth.event_ts <= MAX_DEPTH_STALE_MS (1500ms，过渡方案)
          - Taker Buy（pre-event）：[event_ts - window, event_ts] 窗口，
            反映强平发生时的买卖格局，不含事后修复资金。
            注意：这与策略文档 §7.4 描述的「事后确认」语义不同：
              pre-event Taker Buy  → 记录研究样本中「强平前市场状态」
              post-event Taker Buy → 未来 Shadow Mode 下单时的入场时机确认
            Observe Mode 仅需前者用于统计研究。
        """
        # 统一清理 trade_flow（保留最近 _TAKER_FLOW_MAX_LOOKBACK_MS=180s 数据）
        # 必须在所有 taker_* 方法调用前执行，确保一致的窗口边界
        self._cleanup_trade_flow(event_ts_ms)

        liq_current, liq_prev = self._liquidation_windows(event_ts_ms)
        accel_ratio = self._acceleration_ratio(liq_current, liq_prev)
        vwap = self._vwap()

        # ── 盘口新鲜度 ───────────────────────────────────────────────────────
        depth_staleness_ms: int | None = None
        effective_mid: float | None = None

        if self._mid_price is not None and self._mid_price_ts is not None:
            depth_staleness_ms = abs(event_ts_ms - self._mid_price_ts)
            effective_mid = self._mid_price if depth_staleness_ms <= MAX_MID_STALE_MS else None
        elif self._mid_price is not None:
            effective_mid = self._mid_price

        # ── 指数价格新鲜度 ───────────────────────────────────────────────────
        index_staleness_ms: int | None = None
        effective_index: float | None = None

        if self._index_price is not None and self._index_price_ts is not None:
            index_staleness_ms = abs(event_ts_ms - self._index_price_ts)
            effective_index = self._index_price if index_staleness_ms <= MAX_INDEX_STALE_MS else None

        # ── 基差 (Basis) ─────────────────────────────────────────────────────
        basis_bps = self._basis_bps(effective_mid, effective_index)

        # ── 买盘深度 & 冲击比例 ──────────────────────────────────────────────
        # ── 买盘深度新鲜度（双重约束）────────────────────────────────────────────
        # 约束 1【因果方向】：bid_depth 必须来自强平「之前」（depth.event_ts <= liq.event_ts）
        #   原因：强平击穿买盘后，做市商数百毫秒内重新挂单，盘口迅速修复。
        #   若取强平后的深度，impact_ratio 分母偏大 → 冲击被系统性低估。
        #
        # 约束 2【时效性】：depth 距强平不超过 MAX_DEPTH_STALE_MS（当前 1500ms）
        #   原因：过旧的深度反映的是更早时刻的市场状态，与强平瞬间的流动性不符。
        #   长期目标：depth stream 升频至 ≤100ms 后，此值收紧至 100ms。
        #
        # 两个约束均须同时满足，缺一则 impact_ratio = None。
        effective_bid_depth: float | None = None
        if self._bid_depth_usdt is not None and self._bid_depth_usdt_ts is not None:
            # 约束 1：严格因果，只接受强平前（含同时刻）的深度快照
            if self._bid_depth_usdt_ts <= event_ts_ms:
                bid_staleness = event_ts_ms - self._bid_depth_usdt_ts
                # 约束 2：时效性，使用 MAX_DEPTH_STALE_MS（独立于 mid/index 阈值）
                if bid_staleness <= MAX_DEPTH_STALE_MS:
                    effective_bid_depth = self._bid_depth_usdt
            # 若 depth.event_ts > liq.event_ts：深度来自强平后，丢弃（因果违反）

        impact_ratio = self._impact_ratio(liq_current, effective_bid_depth)

        # ── Taker Buy 占比（事前窗口，liq_window_ms=5s）────────────────────────
        taker_buy_ratio = self._taker_buy_ratio(event_ts_ms)

        # ── Phase 5：信号时序特征 ─────────────────────────────────────────────
        # L1 前置预警：过去 180s 内 30s 子窗口的 taker_sell_ratio 峰值
        taker_sell_peak = self._taker_sell_peak_in_window(event_ts_ms)
        # L4 入场确认：买方力量是否正在回升（近 10s vs 前 10-30s）
        taker_buy_rising = self._taker_buy_ratio_rising(event_ts_ms)

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
            # Phase 5 信号时序
            taker_sell_peak_180s=taker_sell_peak,
            taker_buy_rising=taker_buy_rising,
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

    def _cleanup_trade_flow(self, now_ms: int) -> None:
        """
        清理 trade_flow 中超过最大观测窗口的历史数据。

        统一在 snapshot_at 顶部调用，而非分散在各计算方法内部。
        保留 _TAKER_FLOW_MAX_LOOKBACK_MS（180s）的数据，
        确保 taker_sell_peak_in_window 能访问到足够长的历史。
        """
        cutoff = now_ms - _TAKER_FLOW_MAX_LOOKBACK_MS
        while self._trade_flow and self._trade_flow[0][0] < cutoff:
            self._trade_flow.popleft()

    def _taker_buy_ratio(self, now_ms: int) -> float | None:
        """
        计算滑动窗口 [now_ms - liq_window_ms, now_ms] 内的 Taker Buy 成交额占比。

        语义说明（Observe Mode）：
          - 当 now_ms = liq.event_ts 时，此窗口反映强平「发生时」的买卖格局
            （即强平前 liq_window_sec 秒内主动买盘占比）。
          - 这是一个「预确认」指标：高 taker_buy_ratio 说明即便在强平时，
            市场仍有主动买盘抵抗（套利者已提前入场 or 抄底盘积极）。
          - 「事后验证」语义（强平发生后主动买盘开始入场）需使用
            post-event 窗口 [liq.event_ts, liq.event_ts + X]，
            该路径将在 Shadow Mode 入场时机确认时实现。

        注意：数据清理由 snapshot_at 顶部的 _cleanup_trade_flow 统一负责，
        此方法只做窗口内过滤，不再主动 popleft。
        """
        cutoff = now_ms - self._liq_window_ms

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

    def _taker_sell_peak_in_window(self, now_ms: int) -> float | None:
        """
        在过去 _TAKER_FLOW_MAX_LOOKBACK_MS（180s）内，以 _TAKER_SELL_SUBWIN_MS（30s）
        为子窗口，计算每个子窗口的 taker_sell_ratio，返回所有子窗口中的最大值（峰值）。

        物理语义（Phase 5 L1 前置预警）：
          - 被动市价卖单（强平产生）在强平事件「之前」就已经开始涌现（T-3min 内）
          - 捕捉"过去 3 分钟内曾经出现过大卖压"，而不要求卖压此刻仍在持续
          - 高峰值（> 0.65）说明近期有异常被动卖压，是强平级联的前兆

        返回：
          float  — 所有子窗口中 taker_sell_ratio 的最大值（0.0 到 1.0）
          None   — 整个 lookback 窗口内无任何成交数据（冷启动 or 数据断流）

        子窗口划分（固定步长，不滑动）：
          以 now_ms 为终点，向前按 30s 切分 6 个桶：
          [now-180s, now-150s], [now-150s, now-120s], ..., [now-30s, now]

        子窗口成交量不足时跳过（不影响其他窗口的峰值计算）。
        """
        lookback_start = now_ms - _TAKER_FLOW_MAX_LOOKBACK_MS
        n_subwindows   = _TAKER_FLOW_MAX_LOOKBACK_MS // _TAKER_SELL_SUBWIN_MS  # = 6

        peak: float | None = None
        for i in range(n_subwindows):
            win_start = lookback_start + i * _TAKER_SELL_SUBWIN_MS
            win_end   = win_start + _TAKER_SELL_SUBWIN_MS

            total_notional = 0.0
            sell_notional  = 0.0
            for ts, notional, is_buy in self._trade_flow:
                if win_start <= ts < win_end:
                    total_notional += notional
                    if not is_buy:  # is_buy=False → Taker Sell
                        sell_notional += notional

            if total_notional <= 0:
                continue  # 此子窗口无成交，跳过（不计入峰值）

            ratio = sell_notional / total_notional
            if peak is None or ratio > peak:
                peak = ratio

        return peak

    def _taker_buy_ratio_rising(self, now_ms: int) -> bool:
        """
        买方力量回升确认（Phase 5 L4 入场确认）。

        物理语义：
          - 强平冲击最激烈时，taker_sell 主导（卖压 >> 买压）
          - 强平消退、套利者/抄底者开始入场时，taker_buy 逐渐回升
          - 「买方比例正斜率」是套利修复已经启动的信号，而不仅仅是「有买单」

        算法：
          recent_avg  = [now-10s, now] 内的 taker_buy_ratio（近期买压）
          prior_avg   = [now-30s, now-10s] 内的 taker_buy_ratio（基准买压）
          返回：recent_avg > prior_avg

        返回 False 的情形：
          - 任一窗口内无成交数据（不够数据 → 保守处理，不确认入场）
          - recent_avg <= prior_avg（买压未回升或继续下降）
        """
        recent_cutoff = now_ms - _TAKER_BUY_RECENT_MS   # now - 10s
        prior_cutoff  = now_ms - _TAKER_BUY_PRIOR_MS    # now - 30s

        recent_total = recent_buy = 0.0
        prior_total  = prior_buy  = 0.0

        for ts, notional, is_buy in self._trade_flow:
            if ts >= recent_cutoff:          # [now-10s, now]
                recent_total += notional
                if is_buy:
                    recent_buy += notional
            elif ts >= prior_cutoff:         # [now-30s, now-10s]
                prior_total += notional
                if is_buy:
                    prior_buy += notional

        if recent_total <= 0 or prior_total <= 0:
            return False  # 数据不足 → 不确认入场

        recent_ratio = recent_buy / recent_total
        prior_ratio  = prior_buy  / prior_total
        return recent_ratio > prior_ratio

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

