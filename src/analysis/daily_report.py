"""
每日观测报告 — Observe Mode 数据分析。

用法：
    python -m src.analysis.daily_report              # 分析全部数据
    python -m src.analysis.daily_report --days 3     # 仅分析最近 3 天

核心问题：
    1. 回弹发生率  — BTCUSDT SELL 强平后 N 分钟内价格回到锚点（Index Price / VWAP）上方的比例
    2. 规模门槛    — 多大的强平才有统计意义的回弹
    3. 基差门槛    — basis_bps 需要多深，回弹概率才足够高
    4. 冲击比例    — impact_ratio 高时回弹率是否不同
    5. 偏离门槛    — [DEPRECATED] deviation_bps 分桶（保留向后兼容）
    6. 加速率影响  — liq_accel_ratio 高时回弹率是否更低
    7. 最优参数建议 — 根据当前数据给出信号阈值的参考范围

输出：
    - 控制台文本报告
    - /opt/lmr-hunter/data/reports/YYYY-MM-DD.txt
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

DB_PATH      = os.getenv("DB_PATH", "/opt/lmr-hunter/data/lmr.db")
REPORTS_DIR  = Path(os.getenv("REPORTS_DIR", "/opt/lmr-hunter/data/reports"))
SYMBOL       = "BTCUSDT"

# 回弹判定：价格回到 VWAP 或 Index Price 上方视为回弹成功
RECOVERY_WINDOWS_MS = [60_000, 120_000, 300_000]   # 1min, 2min, 5min
RECOVERY_LABEL      = {60_000: "1min", 120_000: "2min", 300_000: "5min"}


# ── Run Validity（阶段 D）────────────────────────────────────────────────────────

async def load_run_validity(db: aiosqlite.Connection, since_ts: int) -> dict:
    """
    检查指定时间范围内的数据可信性。

    数据源：
    - service_heartbeats.status     — "DEGRADED" 表示该 run 已发生样本损坏
    - service_heartbeats.notes      — JSON 字段，含累计完整性计数器
      （overflow_dropped / isolated / lost；进程生命周期内单调递增）
    - risk_events                   — 严重度 CRITICAL / ERROR 的风控事件

    判定规则（与 runtime-optimization-plan.md 阶段 D 保持一致）：
      TRUSTED      — 无任何样本损坏，心跳状态全部 OK
      CONTAMINATED — 出现以下任一：
                       overflow_dropped > 0
                       isolated > 0
                       lost > 0
                       任何心跳 status = DEGRADED
      UNKNOWN      — 时间范围内无心跳记录，无法判断

    返回字段：
      status                  : "TRUSTED" | "CONTAMINATED" | "UNKNOWN"
      overflow_dropped        : 窗口内各心跳 notes 中 overflow_dropped 的最大值
      isolated                : 同上，isolated
      lost                    : 同上，lost
      degraded_heartbeat_count: status=DEGRADED 的心跳数量
      first_degraded_ts       : 最早一条 DEGRADED 心跳的时间戳（ms），None 表示无
      critical_risk_events    : CRITICAL/ERROR 风控事件列表
      heartbeat_count         : 窗口内心跳总数
    """
    # 1. 读取窗口内所有心跳
    rows = await (await db.execute(
        """
        SELECT ts, status, notes FROM service_heartbeats
        WHERE ts >= ?
        ORDER BY ts ASC
        """,
        (since_ts,),
    )).fetchall()

    heartbeat_count      = len(rows)
    max_overflow_dropped = 0
    max_isolated         = 0
    max_lost             = 0
    degraded_ts_list: list[int] = []

    for ts, status, notes_str in rows:
        if status == "DEGRADED":
            degraded_ts_list.append(ts)
        if notes_str:
            try:
                notes = json.loads(notes_str)
                max_overflow_dropped = max(max_overflow_dropped,
                                           notes.get("overflow_dropped", 0))
                max_isolated         = max(max_isolated,
                                           notes.get("isolated", 0))
                max_lost             = max(max_lost,
                                           notes.get("lost", 0))
            except (json.JSONDecodeError, AttributeError):
                pass

    # 2. 读取 CRITICAL / ERROR 风控事件
    risk_rows = await (await db.execute(
        """
        SELECT ts, component, event_type, severity, message
        FROM risk_events
        WHERE ts >= ? AND severity IN ('CRITICAL', 'ERROR')
        ORDER BY ts ASC
        """,
        (since_ts,),
    )).fetchall()

    critical_risk_events = [
        {
            "ts":         r[0],
            "component":  r[1],
            "event_type": r[2],
            "severity":   r[3],
            "message":    r[4],
        }
        for r in risk_rows
    ]

    # 3. 判定可信性
    if heartbeat_count == 0:
        validity_status = "UNKNOWN"
    elif (
        max_overflow_dropped > 0
        or max_isolated > 0
        or max_lost > 0
        or len(degraded_ts_list) > 0
    ):
        validity_status = "CONTAMINATED"
    else:
        validity_status = "TRUSTED"

    return {
        "status":                  validity_status,
        "overflow_dropped":        max_overflow_dropped,
        "isolated":                max_isolated,
        "lost":                    max_lost,
        "degraded_heartbeat_count": len(degraded_ts_list),
        "first_degraded_ts":       degraded_ts_list[0] if degraded_ts_list else None,
        "critical_risk_events":    critical_risk_events,
        "heartbeat_count":         heartbeat_count,
    }


# ── 数据读取 ───────────────────────────────────────────────────────────────────

async def load_signals(db: aiosqlite.Connection, since_ts: int) -> list[dict]:
    """加载信号记录（仅 BTCUSDT SELL 方向的有效记录）。"""
    rows = await (await db.execute(
        """
        SELECT ts, liq_notional_window, liq_accel_ratio,
               vwap_15m, mid_price, deviation_bps,
               index_price, basis_bps, impact_ratio
        FROM signals
        WHERE ts >= ?
          AND notes LIKE '%btc_sell%'
          AND mid_price IS NOT NULL
        ORDER BY ts ASC
        """,
        (since_ts,),
    )).fetchall()

    return [
        {
            "ts":           r[0],
            "liq_window":   r[1],
            "accel_ratio":  r[2],
            "vwap":         r[3],
            "mid_price":    r[4],
            "deviation":    r[5],
            "index_price":  r[6],
            "basis_bps":    r[7],
            "impact_ratio": r[8],
        }
        for r in rows
    ]


async def load_price_after(
    db: aiosqlite.Connection,
    signal_ts: int,
    window_ms: int,
) -> tuple[float | None, float | None]:
    """
    返回信号发生后 window_ms 毫秒内的 (最高价, 最低价)。
    用于判断价格是否在窗口内触达回弹目标。
    """
    end_ts = signal_ts + window_ms
    row = await (await db.execute(
        """
        SELECT MAX(price), MIN(price)
        FROM raw_trades
        WHERE symbol = ? AND trade_ts BETWEEN ? AND ?
        """,
        (SYMBOL, signal_ts, end_ts),
    )).fetchone()
    return (row[0], row[1]) if row and row[0] is not None else (None, None)


# ── 回弹分析 ──────────────────────────────────────────────────────────────────

async def analyze_recovery(
    db: aiosqlite.Connection,
    signals: list[dict],
) -> dict:
    """
    对每个信号计算各时间窗口内的回弹结果。

    回弹成功定义：窗口内最高价 >= 锚点（优先 Index Price，回退到 VWAP）
    """
    results = []

    for sig in signals:
        # 优先使用 Index Price 作为回弹目标锚点
        anchor = sig.get("index_price") or sig.get("vwap")
        sig_ts = sig["ts"]
        recovery = {}

        for win_ms in RECOVERY_WINDOWS_MS:
            hi, lo = await load_price_after(db, sig_ts, win_ms)
            if hi is None or anchor is None:
                recovery[win_ms] = None
            else:
                recovery[win_ms] = hi >= anchor

        results.append({**sig, "recovery": recovery})

    return results


# ── 统计计算 ──────────────────────────────────────────────────────────────────

def recovery_rate(results: list[dict], window_ms: int) -> tuple[float | None, int, int]:
    """返回 (回弹率, 有效样本数, 总样本数)。"""
    valid   = [r for r in results if r["recovery"].get(window_ms) is not None]
    success = [r for r in valid  if r["recovery"][window_ms] is True]
    if not valid:
        return None, 0, len(results)
    return len(success) / len(valid), len(valid), len(results)


def bucket_analysis(
    results: list[dict],
    key: str,
    buckets: list[tuple[float, float, str]],
    window_ms: int,
) -> list[dict]:
    """按指定字段分桶，计算各桶的回弹率。"""
    out = []
    for lo, hi, label in buckets:
        subset = [
            r for r in results
            if r[key] is not None and lo <= r[key] < hi
            and r["recovery"].get(window_ms) is not None
        ]
        if not subset:
            out.append({"label": label, "count": 0, "rate": None})
            continue
        success = sum(1 for r in subset if r["recovery"][window_ms])
        out.append({
            "label": label,
            "count": len(subset),
            "rate":  success / len(subset),
        })
    return out


# ── 报告生成 ──────────────────────────────────────────────────────────────────

def _fmt_ts(ts_ms: int | None) -> str:
    """将毫秒时间戳格式化为 UTC 字符串，None 时返回 'N/A'。"""
    if ts_ms is None:
        return "N/A"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def build_validity_section(validity: dict) -> str:
    """
    生成数据可信性摘要，置于报告最顶部。

    TRUSTED      — 绿灯，结论可用于研究参考
    CONTAMINATED — 红灯，结论不得用于参数决策
    UNKNOWN      — 黄灯，无心跳记录，无法判断
    """
    lines = []
    status = validity["status"]

    if status == "TRUSTED":
        badge = "✓ TRUSTED（可信）"
        guidance = "数据完整，当前报告结论可用于研究参考。"
    elif status == "CONTAMINATED":
        badge = "✗ CONTAMINATED（污染）"
        guidance = (
            "当前报告包含受污染数据，结论不得用于策略参数决策。\n"
            "  污染数据只能用于系统链路观察，不能作为净期望判断依据。"
        )
    else:
        badge = "? UNKNOWN（未知）"
        guidance = "时间范围内无心跳记录，无法判断数据可信性，请谨慎使用结论。"

    lines.append("=" * 60)
    lines.append(f"  数据可信性  {badge}")
    lines.append("=" * 60)
    lines.append(f"  {guidance}")

    # 完整性计数器明细
    lines.append("")
    lines.append(f"  心跳记录数         : {validity['heartbeat_count']}")
    lines.append(f"  overflow_dropped   : {validity['overflow_dropped']}"
                 + ("  ← 队列溢出，最旧样本已丢失" if validity["overflow_dropped"] > 0 else ""))
    lines.append(f"  isolated           : {validity['isolated']}"
                 + ("  ← DB 写入失败，已写隔离文件" if validity["isolated"] > 0 else ""))
    lines.append(f"  lost               : {validity['lost']}"
                 + ("  ← 永久丢失（隔离文件也失败）" if validity["lost"] > 0 else ""))
    lines.append(f"  DEGRADED 心跳数    : {validity['degraded_heartbeat_count']}")

    if validity["first_degraded_ts"] is not None:
        lines.append(f"  首次 DEGRADED 时间 : {_fmt_ts(validity['first_degraded_ts'])}")
        lines.append(f"  ⚠ 该时间点之前的数据可能仍可信，之后的数据应视为污染")

    # CRITICAL 风控事件摘要（最多展示前 5 条）
    risk_events = validity["critical_risk_events"]
    if risk_events:
        lines.append("")
        lines.append(f"  CRITICAL/ERROR 风控事件（共 {len(risk_events)} 条，展示最早 5 条）：")
        for ev in risk_events[:5]:
            lines.append(
                f"    [{_fmt_ts(ev['ts'])}] {ev['severity']} | "
                f"{ev['event_type']} | {(ev['message'] or '')[:60]}"
            )

    lines.append("")
    return "\n".join(lines)


def fmt_rate(rate: float | None) -> str:
    if rate is None:
        return "  N/A"
    return f"{rate * 100:5.1f}%"


def build_report(results: list[dict], days: int | None) -> str:
    lines = []
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines.append("=" * 60)
    lines.append(f"  LMR-Hunter 每日观测报告")
    lines.append(f"  生成时间: {now}")
    scope = f"最近 {days} 天" if days else "全部数据"
    lines.append(f"  数据范围: {scope}  样本: {len(results)} 个 BTC SELL 强平信号")
    lines.append("=" * 60)

    if not results:
        lines.append("\n  数据不足，暂无分析结果。")
        lines.append("  建议：等待系统运行至少 24 小时后再查看报告。")
        return "\n".join(lines)

    # ── 1. 整体回弹率 ─────────────────────────────────────────────────────────
    lines.append("\n【1】整体回弹率（价格回到锚点上方 — 优先 Index Price，回退 VWAP）")
    lines.append(f"  {'窗口':8s} {'回弹率':8s} {'有效样本':8s}")
    lines.append(f"  {'-'*30}")
    for win_ms in RECOVERY_WINDOWS_MS:
        rate, valid, total = recovery_rate(results, win_ms)
        label = RECOVERY_LABEL[win_ms]
        lines.append(f"  {label:8s} {fmt_rate(rate):8s} {valid}/{total}")

    # ── 2. 按强平规模分桶 ────────────────────────────────────────────────────
    lines.append("\n【2】按强平窗口规模 — 5min 回弹率")
    size_buckets = [
        (0,       10_000,  "< 10k"),
        (10_000,  50_000,  "10k-50k"),
        (50_000,  200_000, "50k-200k"),
        (200_000, 1e12,    "> 200k"),
    ]
    lines.append(f"  {'规模':12s} {'回弹率':8s} {'样本':6s}")
    lines.append(f"  {'-'*28}")
    for b in bucket_analysis(results, "liq_window", size_buckets, 300_000):
        lines.append(f"  {b['label']:12s} {fmt_rate(b['rate']):8s} {b['count']}")

    # ── 3. 按偏离深度分桶 (DEPRECATED — 使用 basis_bps 后此分析仅供参考)──────
    lines.append("\n【3】按偏离深度（deviation_bps，DEPRECATED）— 5min 回弹率")
    dev_results = [r for r in results if r.get("deviation") is not None]
    dev_buckets = [
        (-1000, -30, "< -30bps"),
        (-30,   -20, "-30~-20"),
        (-20,   -10, "-20~-10"),
        (-10,     0, "-10~0"),
        (0,    1000, "> 0"),
    ]
    lines.append(f"  {'偏离区间':12s} {'回弹率':8s} {'样本':6s}")
    lines.append(f"  {'-'*28}")
    for b in bucket_analysis(dev_results, "deviation", dev_buckets, 300_000):
        lines.append(f"  {b['label']:12s} {fmt_rate(b['rate']):8s} {b['count']}")

    # ── 3b. 按基差深度分桶（核心：对齐 strategy.md）──────────────────────────
    basis_results = [r for r in results if r.get("basis_bps") is not None]
    if basis_results:
        lines.append("\n【3b】按基差深度（basis_bps，核心指标）— 5min 回弹率")
        basis_buckets = [
            (-1000, -30, "< -30bps"),
            (-30,   -20, "-30~-20"),
            (-20,   -10, "-20~-10"),
            (-10,    -5, "-10~-5"),
            (-5,      0, "-5~0"),
            (0,    1000, "> 0"),
        ]
        lines.append(f"  {'基差区间':12s} {'回弹率':8s} {'样本':6s}")
        lines.append(f"  {'-'*28}")
        for b in bucket_analysis(basis_results, "basis_bps", basis_buckets, 300_000):
            lines.append(f"  {b['label']:12s} {fmt_rate(b['rate']):8s} {b['count']}")

    # ── 3c. 按冲击比例分桶（核心指标）────────────────────────────────────────
    impact_results = [r for r in results if r.get("impact_ratio") is not None]
    if impact_results:
        lines.append("\n【3c】按冲击比例（impact_ratio = 强平/买盘深度）— 5min 回弹率")
        impact_buckets = [
            (0,    0.5, "< 0.5"),
            (0.5,  1.0, "0.5-1.0"),
            (1.0,  2.0, "1.0-2.0"),
            (2.0,  1e6, "> 2.0（击穿）"),
        ]
        lines.append(f"  {'冲击比例':12s} {'回弹率':8s} {'样本':6s}")
        lines.append(f"  {'-'*28}")
        for b in bucket_analysis(impact_results, "impact_ratio", impact_buckets, 300_000):
            lines.append(f"  {b['label']:12s} {fmt_rate(b['rate']):8s} {b['count']}")

    # ── 4. 按加速率分桶 ──────────────────────────────────────────────────────
    lines.append("\n【4】按加速率（liq_accel_ratio）— 5min 回弹率")
    accel_buckets = [
        (0,   0.5,  "< 0.5（减速）"),
        (0.5, 1.0,  "0.5-1.0"),
        (1.0, 2.0,  "1.0-2.0（加速）"),
        (2.0, 1e6,  "> 2.0（急速）"),
    ]
    lines.append(f"  {'加速率':14s} {'回弹率':8s} {'样本':6s}")
    lines.append(f"  {'-'*30}")
    accel_results = [r for r in results if r["accel_ratio"] is not None]
    for b in bucket_analysis(accel_results, "accel_ratio", accel_buckets, 300_000):
        lines.append(f"  {b['label']:14s} {fmt_rate(b['rate']):8s} {b['count']}")

    # ── 5. 偏离和规模的基本统计 ───────────────────────────────────────────────
    lines.append("\n【5】信号特征分布")
    devs = [r["deviation"] for r in results if r.get("deviation") is not None]
    liqs = [r["liq_window"] for r in results]
    basis_vals = [r["basis_bps"] for r in results if r.get("basis_bps") is not None]

    if devs:
        lines.append(f"  偏离 bps — avg={statistics.mean(devs):+.1f}  "
                     f"median={statistics.median(devs):+.1f}  "
                     f"min={min(devs):.1f}  max={max(devs):.1f}  [DEPRECATED]")
    lines.append(f"  强平窗口 — avg={statistics.mean(liqs):,.0f}  "
                 f"median={statistics.median(liqs):,.0f}  "
                 f"max={max(liqs):,.0f} USDT")
    if basis_vals:
        lines.append(f"  基差 bps — avg={statistics.mean(basis_vals):+.1f}  "
                     f"median={statistics.median(basis_vals):+.1f}  "
                     f"min={min(basis_vals):.1f}  max={max(basis_vals):.1f}")

    # ── 6. 参数建议 ──────────────────────────────────────────────────────────
    lines.append("\n【6】参数校准建议")

    # 找到 5min 回弹率 > 55% 的基差桶
    if basis_results:
        lines.append("  a) 基差分析（核心）：")
        best_basis = []
        for b in bucket_analysis(basis_results, "basis_bps", basis_buckets, 300_000):
            if b["rate"] is not None and b["rate"] > 0.55 and b["count"] >= 5:
                best_basis.append(b)
        if best_basis:
            for b in best_basis:
                lines.append(f"    {b['label']:12s} → {fmt_rate(b['rate'])} ({b['count']} 样本)")
            lines.append(f"  建议将 basis_bps 阈值参考上述区间")
        else:
            lines.append("    暂无足够样本确认最优基差阈值（需各桶 ≥ 5 个样本）")

    # 回退到 deviation（兼容旧数据阶段）
    lines.append("  b) 偏离分析（DEPRECATED，兼容旧数据）：")
    best_dev_buckets = []
    for b in bucket_analysis(dev_results, "deviation", dev_buckets, 300_000):
        if b["rate"] is not None and b["rate"] > 0.55 and b["count"] >= 5:
            best_dev_buckets.append(b)

    if best_dev_buckets:
        for b in best_dev_buckets:
            lines.append(f"    {b['label']:12s} → {fmt_rate(b['rate'])} ({b['count']} 样本)")
    else:
        lines.append("    暂无足够样本（需各桶 ≥ 5 个样本）")
        if devs and min(devs) > -10:
            lines.append("    ⚠ 市场偏离幅度很小，阈值在当前行情下可能永远不触发")

    # 数据充足性评估
    lines.append(f"\n【7】数据充足性")
    if len(results) < 20:
        lines.append(f"  ⚠ 样本量不足（{len(results)} < 20），统计结论参考价值有限")
        lines.append(f"  预计还需运行 {max(1, (20 - len(results)) // 2)} 天")
    elif len(results) < 100:
        lines.append(f"  △ 样本量一般（{len(results)}），趋势可参考，阈值需谨慎使用")
    else:
        lines.append(f"  ✓ 样本量充足（{len(results)}），统计结论可信度较高")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


# ── 入口 ──────────────────────────────────────────────────────────────────────

async def main(days: int | None) -> None:
    from dotenv import load_dotenv
    load_dotenv("/opt/lmr-hunter/config/.env")

    db_path = os.getenv("DB_PATH", DB_PATH)
    since_ts = 0
    if days:
        since_ts = int(
            (datetime.now(timezone.utc).timestamp() - days * 86400) * 1000
        )

    db = await aiosqlite.connect(db_path)
    try:
        validity = await load_run_validity(db, since_ts)
        signals  = await load_signals(db, since_ts)
        results  = await analyze_recovery(db, signals)
        report   = build_validity_section(validity) + build_report(results, days)
    finally:
        await db.close()

    print(report)

    # 保存报告文件
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path  = REPORTS_DIR / f"{date_str}.txt"
    out_path.write_text(report, encoding="utf-8")
    print(f"\n报告已保存: {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LMR-Hunter 每日观测报告")
    parser.add_argument("--days", type=int, default=None,
                        help="分析最近 N 天的数据（默认全部）")
    args = parser.parse_args()
    asyncio.run(main(args.days))
