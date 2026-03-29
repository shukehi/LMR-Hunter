"""
Episode outcome 计算引擎（纯计算，无 I/O）。

定义：
  - entry_price          = min_mid_price（episode 期间最低盘口中价，理论做多入场价）
  - price_at_episode_end = episode 结束后第一笔真实成交价（最早可下单时刻的市场价格）
  - MAE（最大不利偏移）   = 入场后 0-5 分钟最低价 vs entry（负值 = 亏损深度）
  - MFE（最大有利偏移）   = 入场后 0-5 分钟最高价 vs entry（正值 = 潜在盈利空间）
  - rebound_to_vwap_ms   = 价格首次回到 pre_event_vwap 的延迟（衡量修复速度）
  - rebound_depth_bps    = (pre_event_vwap - entry) / entry × 10000（反弹目标距离）

所有 bps = 10000 × ΔP / P_entry，正值 = 价格上涨（有利），负值 = 价格下跌（不利）。

主接口：compute_outcome(episode_id, end_event_ts, entry_price, pre_event_vwap, trades)
  - trades 为 [(trade_ts, price), ...] 按 trade_ts 升序排列
  - trades 应由调用方预先过滤为 end_event_ts 到 end_event_ts+15min 的成交
"""
from __future__ import annotations

from dataclasses import dataclass

_WINDOW_1M_MS  =      60_000   # 1 分钟观测窗口
_WINDOW_5M_MS  =  5 * 60_000   # 5 分钟（MAE/MFE 计算窗口）
_WINDOW_15M_MS = 15 * 60_000   # 15 分钟（全量观测窗口）


@dataclass(slots=True)
class EpisodeOutcome:
    """
    一次 episode 的完整 outcome 观测记录。

    使用说明：
      - 验证反弹是否发生：rebound_to_vwap_ms is not None
      - 验证反弹延迟：rebound_to_vwap_ms / 1000 秒
      - 验证 MAE（止损参考）：mae_bps（越负越差）
      - 验证 MFE（止盈参考）：mfe_bps（越正越好）
      - 验证反弹目标距离：rebound_depth_bps（VWAP 相对入场价的距离）
    """
    episode_id:          str
    entry_price:         float | None   # 理论入场价 = min_mid_price（episode 期间最低价）
    price_at_episode_end: float | None  # episode 结束后首笔成交价（可下单的最早真实价格）
    price_at_1m:         float | None   # episode 结束后 1 分钟的价格
    price_at_5m:         float | None   # episode 结束后 5 分钟的价格
    price_at_15m:        float | None   # episode 结束后 15 分钟的价格
    min_price_0_5m:      float | None   # 0-5 分钟最低价（MAE 来源）
    max_price_0_5m:      float | None   # 0-5 分钟最高价（MFE 来源）
    min_price_0_15m:     float | None   # 0-15 分钟最低价
    max_price_0_15m:     float | None   # 0-15 分钟最高价
    mae_bps:             float | None   # 最大不利偏移（0-5m，负值 = 入场后价格进一步下跌）
    mfe_bps:             float | None   # 最大有利偏移（0-5m，正值 = 入场后价格上涨空间）
    rebound_to_vwap_ms:  int   | None   # 价格首次 >= pre_event_vwap 的延迟；None = 15m 内未反弹
    rebound_depth_bps:   float | None   # 从 entry 到 pre_event_vwap 的距离（bps），即反弹目标
    trade_count_0_15m:   int            # 15 分钟内成交总笔数（数据密度参考）


def compute_outcome(
    episode_id:     str,
    end_event_ts:   int,
    entry_price:    float | None,
    pre_event_vwap: float | None,
    trades:         list[tuple[int, float]],  # [(trade_ts, price), ...] 升序
) -> EpisodeOutcome:
    """
    从 episode 结束后 15 分钟内的成交序列计算 outcome。

    参数：
        episode_id:     episode 唯一标识
        end_event_ts:   episode 末笔事件的交易所时间戳 (ms)
        entry_price:    理论入场价（episode 期间 min_mid_price）
        pre_event_vwap: 事前 VWAP（首笔强平时的 VWAP）
        trades:         [(trade_ts, price), ...] — end_event_ts 之后 15 分钟的成交，升序
    """
    n = len(trades)

    # episode 结束后首笔成交价（最早可下单时刻的市场价格）
    price_at_episode_end: float | None = trades[0][1] if trades else None

    # 按时间窗口边界分桶（trade_ts 相对于 end_event_ts 的偏移）
    t1_cutoff  = end_event_ts + _WINDOW_1M_MS
    t5_cutoff  = end_event_ts + _WINDOW_5M_MS

    trades_0_1m  = [(ts, p) for ts, p in trades if ts <= t1_cutoff]
    trades_0_5m  = [(ts, p) for ts, p in trades if ts <= t5_cutoff]
    trades_0_15m = trades   # 调用方已限制在 15m 内

    # 各时间点最后成交价
    price_at_1m  = trades_0_1m[-1][1]  if trades_0_1m  else None
    price_at_5m  = trades_0_5m[-1][1]  if trades_0_5m  else None
    price_at_15m = trades_0_15m[-1][1] if trades_0_15m else None

    # 各窗口极值
    prices_0_5m  = [p for _, p in trades_0_5m]
    prices_0_15m = [p for _, p in trades_0_15m]

    min_5m  = min(prices_0_5m)  if prices_0_5m  else None
    max_5m  = max(prices_0_5m)  if prices_0_5m  else None
    min_15m = min(prices_0_15m) if prices_0_15m else None
    max_15m = max(prices_0_15m) if prices_0_15m else None

    # MAE / MFE（从 entry_price 出发，基于 0-5 分钟窗口）
    mae: float | None = None
    mfe: float | None = None
    if entry_price is not None and entry_price > 0 and min_5m is not None:
        mae = round((min_5m - entry_price) / entry_price * 10_000, 1)
        mfe = round((max_5m - entry_price) / entry_price * 10_000, 1)  # type: ignore[arg-type]

    # 反弹到 VWAP 的延迟（15 分钟内首次价格 >= pre_event_vwap）
    rebound_ms: int | None = None
    if pre_event_vwap is not None:
        for ts, p in trades_0_15m:
            if p >= pre_event_vwap:
                rebound_ms = ts - end_event_ts
                break

    # 反弹幅度（entry → vwap 的距离，bps）
    rebound_depth: float | None = None
    if entry_price is not None and entry_price > 0 and pre_event_vwap is not None:
        rebound_depth = round((pre_event_vwap - entry_price) / entry_price * 10_000, 1)

    return EpisodeOutcome(
        episode_id           = episode_id,
        entry_price          = entry_price,
        price_at_episode_end = price_at_episode_end,
        price_at_1m          = price_at_1m,
        price_at_5m         = price_at_5m,
        price_at_15m        = price_at_15m,
        min_price_0_5m      = min_5m,
        max_price_0_5m      = max_5m,
        min_price_0_15m     = min_15m,
        max_price_0_15m     = max_15m,
        mae_bps             = mae,
        mfe_bps             = mfe,
        rebound_to_vwap_ms  = rebound_ms,
        rebound_depth_bps   = rebound_depth,
        trade_count_0_15m   = n,
    )
