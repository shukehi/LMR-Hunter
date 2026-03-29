"""
可交易口径研究报告 — 阶段 5 交付物。

用法：
    python -m src.analysis.research_report              # 分析全部数据
    python -m src.analysis.research_report --days 7     # 仅分析最近 7 天

核心问题（可交易口径）：
    1. fill_rate        — 在给定 entry_offset_bps 下，挂单能成交的比例
    2. net_win_rate     — 成交后扣费净盈利的比例
    3. expectancy       — 每笔成交的期望净收益（bps）
    4. MAE / MFE 分布   — 止损/止盈空间分析
    5. 参数敏感性        — entry_offset_bps × hard_stop_bps 的净期望热力图
    6. 按盘口分组统计    — 亚盘 / 欧盘 / 美盘 / 夜盘分层结论

设计约定：
    - entry_price = episode.min_mid_price（episode 期间盘口最低价，理论做多入场参考价）
    - 实际挂单价 fill_price = entry_price × (1 - entry_offset_bps / 10000)
    - 成交判定：mae_bps <= -entry_offset_bps（0-5min 内价格触及或低于挂单价）
    - 保守出场优先级：硬止损 > 止盈（回到 VWAP）> 时间止损（5min）
    - 硬止损触发判定：mae_bps <= -(entry_offset_bps + hard_stop_bps)（近似，bps 量级小）
    - 止盈触发判定：rebound_to_vwap_ms is not None and <= time_stop_sec * 1000
    - 时间止损：price_at_5m 作为平仓价（5min 强制出场）
"""
from __future__ import annotations

import argparse
import asyncio
import os
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite

# ── 导入 run validity（复用 daily_report.py 实现）────────────────────────────
from src.analysis.daily_report import build_validity_section, load_run_validity

DB_PATH     = os.getenv("DB_PATH", "/opt/lmr-hunter/data/lmr.db")
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/opt/lmr-hunter/data/reports"))

SESSION_LABELS = ["亚盘", "欧盘", "美盘", "夜盘"]

# ── 盘口分类 ─────────────────────────────────────────────────────────────────

def classify_session(ts_ms: int) -> str:
    """
    将 UTC 时间戳分类为交易时段。

    分段依据（UTC）：
        亚盘  00:00 – 07:00
        欧盘  07:00 – 13:30
        美盘  13:30 – 21:00
        夜盘  21:00 – 00:00
    """
    mins = (ts_ms // 60_000) % (24 * 60)   # 一天内的分钟数（UTC）
    if mins < 7 * 60:
        return "亚盘"
    elif mins < 13 * 60 + 30:
        return "欧盘"
    elif mins < 21 * 60:
        return "美盘"
    else:
        return "夜盘"


# ── 参数和结果数据类 ──────────────────────────────────────────────────────────

@dataclass
class SimParams:
    """单次模拟使用的参数组合。"""
    entry_offset_bps: float = 0.0    # 挂单价相对 episode 低点的偏移（向下，bps）
    hard_stop_bps:    float = 30.0   # 止损宽度（从成交价向下，bps）
    time_stop_sec:    int   = 300    # 时间止损（秒），默认 5 分钟
    maker_fee_bps:    float = 2.0    # Maker 手续费（bps），挂单入场 + 挂单止盈
    taker_fee_bps:    float = 4.0    # Taker 手续费（bps），触发止损时使用


@dataclass
class EpisodeRow:
    """从数据库联表读取的 episode + outcome 原始行。"""
    episode_id:          str
    start_event_ts:      int
    end_event_ts:        int
    liq_count:           int            # episode 内强平笔数
    liq_notional_total:  float
    min_mid_price:       Optional[float]
    pre_event_vwap:      Optional[float]
    max_deviation_bps:   Optional[float]
    mae_bps:             Optional[float]
    mfe_bps:             Optional[float]
    rebound_to_vwap_ms:  Optional[int]
    rebound_depth_bps:   Optional[float]
    price_at_5m:         Optional[float]
    entry_price:         Optional[float]   # = outcome.entry_price（= min_mid_price）
    trade_count_0_15m:   int = 0           # outcome 窗口 15 分钟内的成交笔数
    session:             str = field(default="")

    def __post_init__(self) -> None:
        self.session = classify_session(self.start_event_ts)


@dataclass
class TradeResult:
    """单笔模拟交易结果。"""
    episode_id:        str
    session:           str
    filled:            bool
    exit_reason:       Optional[str]    # "HARD_STOP" | "TAKE_PROFIT" | "TIME_STOP" | None
    pnl_bps:           Optional[float]  # 净 PnL（bps，相对成交价）；未成交为 None
    rebound_depth_bps: Optional[float]  # 反弹目标距离（bps）

    @property
    def is_win(self) -> bool:
        return self.pnl_bps is not None and self.pnl_bps > 0


# ── 合格性过滤 ────────────────────────────────────────────────────────────────

# 合格性阈值（与 docs/reviews/shadow-mode-admission.md 第 2 节保持一致）
QUAL_MIN_NOTIONAL_USD   = 100_000.0   # 最低强平名义价值
QUAL_MAX_DEVIATION_BPS  = -10.0       # 最深偏离（max_deviation_bps 为负值，需 <= -10）
QUAL_MIN_LIQ_COUNT      = 3           # 最少强平笔数
QUAL_MIN_TRADE_COUNT    = 30          # outcome 窗口最少成交笔数
QUAL_MIN_REBOUND_DEPTH  = 10.0        # 最小反弹目标距离（bps）
QUAL_MIN_GAP_MS         = 900_000     # 与上一条合格 episode 的最小间隔（15 分钟）


@dataclass
class QualReject:
    """被过滤掉的 episode 及拒绝原因。"""
    episode_id: str
    session:    str
    reasons:    list[str]


def qualify_episode(ep: EpisodeRow) -> list[str]:
    """
    检查单条 episode 的静态合格性（不含独立性检查，独立性需要序列上下文）。

    返回拒绝原因列表；空列表 = 静态条件全部通过。

    检查维度（对应 shadow-mode-admission.md 第 2.1-2.3 节）：
        2.1 数据完整性
        2.2 信号真实性
        2.3 因果结构有效性
    """
    reasons: list[str] = []

    # ── 2.1 数据完整性 ─────────────────────────────────────────────────────────
    if ep.mae_bps is None:
        reasons.append("mae_bps IS NULL")
    if ep.mfe_bps is None:
        reasons.append("mfe_bps IS NULL")
    if ep.price_at_5m is None:
        reasons.append("price_at_5m IS NULL")
    if ep.entry_price is None:
        reasons.append("entry_price IS NULL")
    if ep.pre_event_vwap is None:
        reasons.append("pre_event_vwap IS NULL")
    if ep.trade_count_0_15m < QUAL_MIN_TRADE_COUNT:
        reasons.append(
            f"trade_count_0_15m={ep.trade_count_0_15m} < {QUAL_MIN_TRADE_COUNT}"
        )

    # ── 2.2 信号真实性 ─────────────────────────────────────────────────────────
    if ep.liq_notional_total < QUAL_MIN_NOTIONAL_USD:
        reasons.append(
            f"liq_notional_total={ep.liq_notional_total:.0f} < {QUAL_MIN_NOTIONAL_USD:.0f}"
        )
    if ep.max_deviation_bps is None or ep.max_deviation_bps > QUAL_MAX_DEVIATION_BPS:
        reasons.append(
            f"max_deviation_bps={ep.max_deviation_bps} > {QUAL_MAX_DEVIATION_BPS}"
        )
    if ep.liq_count < QUAL_MIN_LIQ_COUNT:
        reasons.append(f"liq_count={ep.liq_count} < {QUAL_MIN_LIQ_COUNT}")

    # ── 2.3 因果结构有效性 ─────────────────────────────────────────────────────
    if ep.rebound_depth_bps is None or ep.rebound_depth_bps < QUAL_MIN_REBOUND_DEPTH:
        reasons.append(
            f"rebound_depth_bps={ep.rebound_depth_bps} < {QUAL_MIN_REBOUND_DEPTH}"
        )

    return reasons


def filter_qualified(
    episodes: list[EpisodeRow],
) -> tuple[list[EpisodeRow], list[QualReject]]:
    """
    对按 start_event_ts 升序排列的 episode 列表进行全量合格性过滤。

    依次执行：
      1. 静态合格性（qualify_episode）
      2. 统计独立性（与上一条合格 episode 的间隔 >= QUAL_MIN_GAP_MS）

    返回：
        qualified — 通过所有检查的 episode 列表（保持升序）
        rejected  — 被过滤掉的记录及拒绝原因列表
    """
    qualified: list[EpisodeRow]  = []
    rejected:  list[QualReject]  = []
    last_end_ts: int = 0   # 上一条合格 episode 的 end_event_ts

    for ep in episodes:
        reasons = qualify_episode(ep)

        # 2.4 统计独立性（仅在静态条件全部通过时检查，避免噪声 episode 占用间隔槽）
        if not reasons and last_end_ts > 0:
            gap_ms = ep.start_event_ts - last_end_ts
            if gap_ms < QUAL_MIN_GAP_MS:
                reasons.append(
                    f"independence: gap={gap_ms / 1000:.0f}s "
                    f"< {QUAL_MIN_GAP_MS // 1000}s from prev qualified"
                )

        if reasons:
            rejected.append(QualReject(
                episode_id = ep.episode_id,
                session    = ep.session,
                reasons    = reasons,
            ))
        else:
            qualified.append(ep)
            last_end_ts = ep.end_event_ts

    return qualified, rejected


# ── 数据加载 ─────────────────────────────────────────────────────────────────

async def load_episodes(
    db: aiosqlite.Connection,
    since_ts: int = 0,
) -> list[EpisodeRow]:
    """
    从数据库联表读取所有有效 episode（必须有对应 outcome 记录）。

    只返回 SELL 方向的 episode（策略逻辑：强平 SELL 导致价格下跌，做多反弹）。
    """
    rows = await (await db.execute(
        """
        SELECT
            e.episode_id,
            e.start_event_ts,
            e.end_event_ts,
            e.liq_count,
            e.liq_notional_total,
            e.min_mid_price,
            e.pre_event_vwap,
            e.max_deviation_bps,
            o.mae_bps,
            o.mfe_bps,
            o.rebound_to_vwap_ms,
            o.rebound_depth_bps,
            o.price_at_5m,
            o.entry_price,
            o.trade_count_0_15m
        FROM liquidation_episodes  e
        JOIN episode_outcomes       o ON o.episode_id = e.episode_id
        WHERE e.symbol = 'BTCUSDT'
          AND e.side   = 'SELL'
          AND e.start_event_ts >= ?
        ORDER BY e.start_event_ts ASC
        """,
        (since_ts,),
    )).fetchall()

    return [
        EpisodeRow(
            episode_id         = r[0],
            start_event_ts     = r[1],
            end_event_ts       = r[2],
            liq_count          = r[3],
            liq_notional_total = r[4],
            min_mid_price      = r[5],
            pre_event_vwap     = r[6],
            max_deviation_bps  = r[7],
            mae_bps            = r[8],
            mfe_bps            = r[9],
            rebound_to_vwap_ms = r[10],
            rebound_depth_bps  = r[11],
            price_at_5m        = r[12],
            entry_price        = r[13],
            trade_count_0_15m  = r[14],
        )
        for r in rows
    ]


# ── 单笔模拟 ─────────────────────────────────────────────────────────────────

def simulate_trade(ep: EpisodeRow, params: SimParams) -> TradeResult:
    """
    对单条 episode 模拟一笔可交易的限价单，返回 TradeResult。

    成交判定（近似）：
        fill_price = entry_price × (1 - entry_offset_bps / 10000)
        filled     = mae_bps <= -entry_offset_bps
        （0-5min 最低价触及或低于 fill_price）

    出场优先级（保守）：
        1. 硬止损：mae_bps <= -(entry_offset_bps + hard_stop_bps)  → HARD_STOP
        2. 止盈：rebound_to_vwap_ms is not None and <= time_stop_sec * 1000 → TAKE_PROFIT
        3. 时间止损：price_at_5m 作为出场价  → TIME_STOP

    净 PnL（bps，相对于 fill_price）：
        HARD_STOP   = -hard_stop_bps - maker_fee_bps - taker_fee_bps
        TAKE_PROFIT = rebound_depth_bps + entry_offset_bps - 2 × maker_fee_bps
        TIME_STOP   = (price_at_5m / fill_price - 1) × 10000 - maker_fee_bps - taker_fee_bps
    """
    # 无法模拟：缺少必要数据
    if ep.mae_bps is None or ep.entry_price is None:
        return TradeResult(
            episode_id        = ep.episode_id,
            session           = ep.session,
            filled            = False,
            exit_reason       = None,
            pnl_bps           = None,
            rebound_depth_bps = ep.rebound_depth_bps,
        )

    # ── 成交判定 ──────────────────────────────────────────────────────────────
    filled = ep.mae_bps <= -params.entry_offset_bps

    if not filled:
        return TradeResult(
            episode_id        = ep.episode_id,
            session           = ep.session,
            filled            = False,
            exit_reason       = None,
            pnl_bps           = None,
            rebound_depth_bps = ep.rebound_depth_bps,
        )

    fill_price = ep.entry_price * (1.0 - params.entry_offset_bps / 10_000)

    # ── 出场逻辑（保守：硬止损优先）────────────────────────────────────────────
    hs_threshold = -(params.entry_offset_bps + params.hard_stop_bps)
    hs_triggered = ep.mae_bps <= hs_threshold

    time_stop_ms = params.time_stop_sec * 1_000
    tp_triggered = (
        ep.rebound_to_vwap_ms is not None
        and ep.rebound_to_vwap_ms <= time_stop_ms
    )

    if hs_triggered:
        # 硬止损：Maker 入场 + Taker 出场
        pnl = (
            -params.hard_stop_bps
            - params.maker_fee_bps
            - params.taker_fee_bps
        )
        return TradeResult(
            episode_id        = ep.episode_id,
            session           = ep.session,
            filled            = True,
            exit_reason       = "HARD_STOP",
            pnl_bps           = round(pnl, 2),
            rebound_depth_bps = ep.rebound_depth_bps,
        )

    if tp_triggered:
        # 止盈：Maker 入场 + Maker 止盈
        if ep.rebound_depth_bps is not None:
            profit_from_fill = ep.rebound_depth_bps + params.entry_offset_bps
        else:
            # rebound_depth_bps 缺失时回退到 MFE
            profit_from_fill = (ep.mfe_bps or 0.0) - params.entry_offset_bps
        pnl = profit_from_fill - 2 * params.maker_fee_bps
        return TradeResult(
            episode_id        = ep.episode_id,
            session           = ep.session,
            filled            = True,
            exit_reason       = "TAKE_PROFIT",
            pnl_bps           = round(pnl, 2),
            rebound_depth_bps = ep.rebound_depth_bps,
        )

    # 时间止损：使用 price_at_5m 作为出场价，Maker 入场 + Taker 出场
    if ep.price_at_5m is not None and fill_price > 0:
        exit_return_bps = (ep.price_at_5m / fill_price - 1.0) * 10_000
        pnl = exit_return_bps - params.maker_fee_bps - params.taker_fee_bps
    else:
        # price_at_5m 缺失时保守估算：以 mae_bps 中间值作为出场参考
        # （取 entry_offset + 0 意味着以成交价出场，仅扣手续费）
        pnl = -(params.maker_fee_bps + params.taker_fee_bps)

    return TradeResult(
        episode_id        = ep.episode_id,
        session           = ep.session,
        filled            = True,
        exit_reason       = "TIME_STOP",
        pnl_bps           = round(pnl, 2),
        rebound_depth_bps = ep.rebound_depth_bps,
    )


# ── 统计聚合 ─────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    """一组 TradeResult 的汇总统计。"""
    n_total:      int = 0
    n_filled:     int = 0
    n_wins:       int = 0
    n_hard_stop:  int = 0
    n_take_profit:int = 0
    n_time_stop:  int = 0
    fill_rate:    float = 0.0
    win_rate:     float = 0.0       # 成交后胜率
    avg_win:      float = 0.0       # 平均盈利（bps）
    avg_loss:     float = 0.0       # 平均亏损（bps，正数表示亏损幅度）
    expectancy:   float = 0.0       # 期望净收益（bps / 成交笔）
    mae_p50:      Optional[float] = None
    mae_p90:      Optional[float] = None
    mfe_p50:      Optional[float] = None
    mfe_p90:      Optional[float] = None


def aggregate_stats(results: list[TradeResult], episodes: list[EpisodeRow]) -> Stats:
    """从模拟结果列表计算汇总统计指标。"""
    n_total  = len(results)
    filled   = [r for r in results if r.filled]
    wins     = [r for r in filled if r.is_win]
    losses   = [r for r in filled if not r.is_win]

    n_filled   = len(filled)
    fill_rate  = n_filled / n_total if n_total > 0 else 0.0

    n_wins  = len(wins)
    win_rate = n_wins / n_filled if n_filled > 0 else 0.0

    win_pnls  = [r.pnl_bps for r in wins  if r.pnl_bps is not None]
    loss_pnls = [r.pnl_bps for r in losses if r.pnl_bps is not None]

    avg_win  = statistics.mean(win_pnls)   if win_pnls  else 0.0
    avg_loss = statistics.mean(loss_pnls)  if loss_pnls else 0.0   # 负值
    all_pnls = [r.pnl_bps for r in filled if r.pnl_bps is not None]
    expectancy = statistics.mean(all_pnls) if all_pnls else 0.0

    # MAE / MFE 分布（所有有 outcome 数据的 episodes，不受 filled 限制）
    mae_vals = sorted([e.mae_bps for e in episodes if e.mae_bps is not None])
    mfe_vals = sorted([e.mfe_bps for e in episodes if e.mfe_bps is not None])

    def percentile(data: list[float], pct: float) -> Optional[float]:
        if not data:
            return None
        idx = int(len(data) * pct / 100)
        idx = min(idx, len(data) - 1)
        return data[idx]

    return Stats(
        n_total       = n_total,
        n_filled      = n_filled,
        n_wins        = n_wins,
        n_hard_stop   = sum(1 for r in filled if r.exit_reason == "HARD_STOP"),
        n_take_profit = sum(1 for r in filled if r.exit_reason == "TAKE_PROFIT"),
        n_time_stop   = sum(1 for r in filled if r.exit_reason == "TIME_STOP"),
        fill_rate     = fill_rate,
        win_rate      = win_rate,
        avg_win       = round(avg_win,  2),
        avg_loss      = round(avg_loss, 2),
        expectancy    = round(expectancy, 2),
        mae_p50       = percentile(mae_vals, 50),
        mae_p90       = percentile(mae_vals, 90),
        mfe_p50       = percentile(mfe_vals, 50),
        mfe_p90       = percentile(mfe_vals, 90),
    )


# ── 报告生成 ─────────────────────────────────────────────────────────────────

def _fmt_ts(ts_ms: Optional[int]) -> str:
    if ts_ms is None:
        return "—"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def _bps(v: Optional[float]) -> str:
    if v is None:
        return "  N/A"
    return f"{v:+.1f} bps"


def _bar(v: float, lo: float = -50, hi: float = 50, width: int = 20) -> str:
    """简易文本进度条，v 在 [lo, hi] 区间内映射到宽度 width。"""
    ratio = (v - lo) / (hi - lo) if hi > lo else 0.5
    ratio = max(0.0, min(1.0, ratio))
    filled = int(ratio * width)
    return "[" + "█" * filled + "·" * (width - filled) + "]"


def build_stats_block(stats: Stats, label: str = "全部") -> str:
    lines = [
        f"  样本数      : {stats.n_total} episodes",
        f"  成交率      : {_pct(stats.fill_rate)}  ({stats.n_filled}/{stats.n_total})",
    ]
    if stats.n_filled > 0:
        lines += [
            f"  净胜率      : {_pct(stats.win_rate)}  ({stats.n_wins}/{stats.n_filled})",
            f"  止盈成交    : {stats.n_take_profit}  |  硬止损 : {stats.n_hard_stop}"
            f"  |  时间止损 : {stats.n_time_stop}",
            f"  平均盈利    : {_bps(stats.avg_win)}",
            f"  平均亏损    : {_bps(stats.avg_loss)}",
            f"  期望净收益  : {_bps(stats.expectancy)}  {_bar(stats.expectancy)}",
        ]
        if stats.mae_p50 is not None:
            lines += [
                f"  MAE p50/p90 : {_bps(stats.mae_p50)} / {_bps(stats.mae_p90)}",
                f"  MFE p50/p90 : {_bps(stats.mfe_p50)} / {_bps(stats.mfe_p90)}",
            ]
    return "\n".join(lines)


def build_sensitivity_table(
    episodes: list[EpisodeRow],
    entry_offsets: list[float],
    hard_stops:    list[float],
    params_base:   SimParams,
) -> str:
    """
    生成 entry_offset_bps × hard_stop_bps 参数敏感性表格。
    行 = entry_offset，列 = hard_stop，格内 = 期望净收益（bps）。
    """
    lines = ["  参数敏感性热力图（期望净收益 bps | 成交率%）"]
    lines.append("")

    # 表头
    header = "  entry_offset↓ \\ hard_stop→ |"
    for hs in hard_stops:
        header += f" {hs:>5.0f}bps |"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))

    for eo in entry_offsets:
        row = f"  {eo:>6.1f} bps                |"
        for hs in hard_stops:
            p = SimParams(
                entry_offset_bps = eo,
                hard_stop_bps    = hs,
                time_stop_sec    = params_base.time_stop_sec,
                maker_fee_bps    = params_base.maker_fee_bps,
                taker_fee_bps    = params_base.taker_fee_bps,
            )
            results = [simulate_trade(ep, p) for ep in episodes]
            st = aggregate_stats(results, episodes)
            exp_str = f"{st.expectancy:+.1f}" if st.n_filled > 0 else "  N/A"
            fill_pct = f"{st.fill_rate * 100:.0f}%"
            row += f" {exp_str:>5}/{fill_pct:>4} |"
        lines.append(row)

    lines.append("")
    lines.append("  格式：期望净收益 bps / 成交率（保守模拟：硬止损优先于止盈）")
    return "\n".join(lines)


def build_qualification_section(
    all_episodes: list[EpisodeRow],
    qualified:    list[EpisodeRow],
    rejected:     list[QualReject],
) -> str:
    """
    生成样本合格性过滤摘要，显示：
    - 原始 / 合格 / 被过滤 的数量
    - 主要拒绝原因统计
    - 距离准入门槛的差距
    """
    n_all  = len(all_episodes)
    n_qual = len(qualified)
    n_rej  = len(rejected)

    # 统计拒绝原因频次（取每条记录的第一个原因作为主因）
    reason_counts: dict[str, int] = {}
    for r in rejected:
        # 归一化：截断具体数值，只保留原因类型
        raw = r.reasons[0] if r.reasons else "unknown"
        # 提取原因键（去掉 = 后面的具体数值）
        key = raw.split("=")[0].split(":")[0].strip()
        reason_counts[key] = reason_counts.get(key, 0) + 1

    # 合格样本的盘口分布
    qual_session: dict[str, int] = {}
    for ep in qualified:
        qual_session[ep.session] = qual_session.get(ep.session, 0) + 1

    # 准入门槛对比（shadow-mode-admission.md 第 3 节）
    THRESHOLDS = {"美盘": 25, "欧盘": 15, "亚盘": 10}
    gap_lines = []
    for session, need in THRESHOLDS.items():
        have = qual_session.get(session, 0)
        symbol = "✓" if have >= need else "✗"
        gap_lines.append(
            f"    {symbol} {session}: {have}/{need}"
            + (f"  还差 {need - have} 条" if have < need else "  已达标")
        )
    total_need = 60
    total_have = n_qual
    gap_lines.append(
        f"    {'✓' if total_have >= total_need else '✗'} 总计: "
        f"{total_have}/{total_need}"
        + (f"  还差 {total_need - total_have} 条" if total_have < total_need else "  已达标")
    )

    lines = [
        f"  原始 episode : {n_all}  合格 : {n_qual}  过滤掉 : {n_rej}",
        f"  合格样本盘口 : " + "  ".join(
            f"{s}:{qual_session.get(s, 0)}" for s in SESSION_LABELS
        ),
    ]
    if reason_counts:
        lines.append("  主要过滤原因：")
        for reason, cnt in sorted(reason_counts.items(), key=lambda x: -x[1]):
            lines.append(f"    {cnt:>3}条  {reason}")
    lines.append("  Shadow Mode 准入门槛进度（shadow-mode-admission.md 第 3 节）：")
    lines.extend(gap_lines)

    return "\n".join(lines)


async def build_report(db: aiosqlite.Connection, since_ts: int = 0) -> str:
    """生成完整可交易口径研究报告（基于合格样本）。"""
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── 数据可信性 ────────────────────────────────────────────────────────────
    validity = await load_run_validity(db, since_ts=since_ts)
    validity_section = build_validity_section(validity)

    # ── 加载并过滤 episode 数据 ───────────────────────────────────────────────
    all_episodes = await load_episodes(db, since_ts=since_ts)
    episodes, rejected = filter_qualified(all_episodes)

    if not all_episodes:
        return (
            f"LMR-Hunter 可交易口径研究报告 — {now_str}\n"
            + "=" * 60 + "\n"
            + validity_section + "\n\n"
            + "⚠️  暂无有效 episode 数据（需同时有 liquidation_episodes + episode_outcomes 记录）。\n"
        )

    # ── 数据概况（基于原始 + 合格口径） ──────────────────────────────────────
    ts_start = _fmt_ts(all_episodes[0].start_event_ts)
    ts_end   = _fmt_ts(all_episodes[-1].start_event_ts)
    session_counts: dict[str, int] = {}
    for ep in all_episodes:
        session_counts[ep.session] = session_counts.get(ep.session, 0) + 1

    notionals  = [ep.liq_notional_total for ep in all_episodes]
    deviations = [ep.max_deviation_bps  for ep in all_episodes if ep.max_deviation_bps is not None]

    summary_lines = [
        f"  样本时段 : {ts_start}  →  {ts_end}",
        f"  原始 episode 数 : {len(all_episodes)}  合格 : {len(episodes)}",
        f"  原始盘口分布 : " + "  ".join(
            f"{s}:{session_counts.get(s, 0)}" for s in SESSION_LABELS
        ),
        f"  强平名义价值 : "
        f"中位数 ${statistics.median(notionals):,.0f}  "
        f"最大 ${max(notionals):,.0f}",
    ]
    if deviations:
        summary_lines.append(
            f"  最大偏离 bps : "
            f"中位数 {statistics.median(deviations):.1f}  "
            f"最小（最深）{min(deviations):.1f}"
        )

    # 后续统计分析使用合格样本；如合格样本为空则跳过统计部分
    if not episodes:
        sep = "=" * 68
        return "\n".join([
            "LMR-Hunter 可交易口径研究报告",
            f"生成时间：{now_str}",
            sep,
            validity_section,
            sep,
            "【一】数据概况",
            "\n".join(summary_lines),
            sep,
            "【样本合格性】",
            build_qualification_section(all_episodes, episodes, rejected),
            sep,
            "⚠️  合格样本数为 0，跳过统计分析。请等待更多数据积累。",
            sep,
        ]) + "\n"

    # ── 默认参数全局模拟 ───────────────────────────────────────────────────────
    default_params = SimParams(
        entry_offset_bps = 0.0,
        hard_stop_bps    = 30.0,
        time_stop_sec    = 300,
        maker_fee_bps    = 2.0,
        taker_fee_bps    = 4.0,
    )
    all_results = [simulate_trade(ep, default_params) for ep in episodes]
    global_stats = aggregate_stats(all_results, episodes)

    # ── 按盘口分组 ─────────────────────────────────────────────────────────────
    session_blocks: list[str] = []
    for session in SESSION_LABELS:
        ep_sess = [ep for ep in episodes if ep.session == session]
        if not ep_sess:
            session_blocks.append(f"  {session} : 无样本")
            continue
        res_sess = [simulate_trade(ep, default_params) for ep in ep_sess]
        st_sess  = aggregate_stats(res_sess, ep_sess)
        block = f"  ── {session}（n={len(ep_sess)}）──\n"
        block += build_stats_block(st_sess, session)
        session_blocks.append(block)

    # ── 参数敏感性 ─────────────────────────────────────────────────────────────
    sensitivity = build_sensitivity_table(
        episodes,
        entry_offsets = [0.0, 5.0, 10.0, 20.0],
        hard_stops    = [20.0, 30.0, 50.0],
        params_base   = default_params,
    )

    # ── 回弹质量分布 ───────────────────────────────────────────────────────────
    rebounded = [ep for ep in episodes if ep.rebound_to_vwap_ms is not None]
    rebound_rate = len(rebounded) / len(episodes)
    rebound_latencies = sorted(
        ep.rebound_to_vwap_ms for ep in rebounded
        if ep.rebound_to_vwap_ms is not None
    )

    def _median_ms(ms_list: list[int]) -> str:
        if not ms_list:
            return "—"
        m = statistics.median(ms_list)
        return f"{m / 1000:.1f}s"

    rebound_lines = [
        f"  15min 内回到 VWAP 的比例  : {_pct(rebound_rate)}  ({len(rebounded)}/{len(episodes)})",
        f"  反弹延迟中位数            : {_median_ms(rebound_latencies)}",
    ]
    if rebound_latencies:
        within_1m = sum(1 for ms in rebound_latencies if ms <= 60_000)
        within_5m = sum(1 for ms in rebound_latencies if ms <= 300_000)
        rebound_lines += [
            f"  1min 内反弹              : {within_1m}/{len(rebounded)}",
            f"  5min 内反弹              : {within_5m}/{len(rebounded)}",
        ]

    # ── 辅助：按盘口的回弹质量 ────────────────────────────────────────────────
    rebound_by_session: list[str] = []
    for session in SESSION_LABELS:
        ep_sess = [ep for ep in episodes if ep.session == session]
        if not ep_sess:
            continue
        reb_sess = [ep for ep in ep_sess if ep.rebound_to_vwap_ms is not None]
        r_rate = len(reb_sess) / len(ep_sess)
        lats = sorted(ep.rebound_to_vwap_ms for ep in reb_sess
                      if ep.rebound_to_vwap_ms is not None)
        rebound_by_session.append(
            f"    {session}（n={len(ep_sess)}）: "
            f"回弹率 {_pct(r_rate)}  中位延迟 {_median_ms(lats)}"
        )

    # ── 组装报告 ───────────────────────────────────────────────────────────────
    sep = "=" * 68
    sub = "-" * 68

    sections = [
        f"LMR-Hunter 可交易口径研究报告",
        f"生成时间：{now_str}",
        sep,
        validity_section,
        sep,
        "【一】数据概况",
        sub,
        "\n".join(summary_lines),
        "",
        sep,
        "【二】样本合格性过滤",
        sub,
        build_qualification_section(all_episodes, episodes, rejected),
        "",
        sep,
        "【三】全局可交易模拟（仅合格样本 · 默认参数）",
        f"  entry_offset={default_params.entry_offset_bps:.0f}bps  "
        f"hard_stop={default_params.hard_stop_bps:.0f}bps  "
        f"time_stop={default_params.time_stop_sec}s  "
        f"maker_fee={default_params.maker_fee_bps:.1f}bps  "
        f"taker_fee={default_params.taker_fee_bps:.1f}bps",
        sub,
        build_stats_block(global_stats),
        "",
        sep,
        "【四】按盘口分组统计（仅合格样本 · 默认参数）",
        sub,
        "\n".join(session_blocks),
        "",
        sep,
        "【五】参数敏感性分析（仅合格样本）",
        sub,
        sensitivity,
        sep,
        "【六】回弹质量分布（辅助观测，不作为参数建议主依据）",
        sub,
        "\n".join(rebound_lines),
        "  按盘口：",
        "\n".join(rebound_by_session),
        "",
        sep,
        "【警告】",
        "  本报告基于 Observe Mode 真实观测数据，使用理想化的简化成交假设：",
        "  - 成交判定基于 mae_bps（0-5min 最低价），忽略挂单深度与实际流动性",
        "  - 时间止损使用 price_at_5m 快照，忽略滑点",
        "  - 硬止损触发仅为近似（bps 叠加而非精确复合）",
        "  - 样本量较小，结论仅供 Shadow Mode 参数探索参考",
        sep,
    ]

    return "\n".join(sections) + "\n"


# ── 主入口 ───────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="LMR-Hunter 可交易口径研究报告")
    parser.add_argument("--days", type=int, default=0,
                        help="仅分析最近 N 天数据（0 = 全部）")
    parser.add_argument("--out", type=str, default="",
                        help="输出文件路径（默认写入 REPORTS_DIR）")
    args = parser.parse_args()

    since_ts = 0
    if args.days > 0:
        import time
        since_ts = int((time.time() - args.days * 86400) * 1000)

    async with aiosqlite.connect(DB_PATH) as db:
        report = await build_report(db, since_ts=since_ts)

    print(report)

    out_path = args.out
    if not out_path:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        out_path = str(REPORTS_DIR / f"{date_str}-research.txt")

    Path(out_path).write_text(report, encoding="utf-8")
    print(f"报告已写入：{out_path}")


if __name__ == "__main__":
    asyncio.run(main())
