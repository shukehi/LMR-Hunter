"""
每日观测报告 — Observe Mode 数据分析。

用法：
    python -m src.analysis.daily_report              # 分析全部数据
    python -m src.analysis.daily_report --days 3     # 仅分析最近 3 天

核心问题：
    1. 回弹发生率  — BTCUSDT SELL 强平后 N 分钟内价格回到 VWAP 上方的比例
    2. 规模门槛    — 多大的强平才有统计意义的回弹
    3. 偏离门槛    — deviation_bps 需要多深，回弹概率才足够高
    4. 加速率影响  — liq_accel_ratio 高时回弹率是否更低
    5. 最优参数建议 — 根据当前数据给出信号阈值的参考范围

输出：
    - 控制台文本报告
    - /opt/lmr-hunter/data/reports/YYYY-MM-DD.txt
"""
from __future__ import annotations

import argparse
import asyncio
import os
import statistics
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

DB_PATH      = os.getenv("DB_PATH", "/opt/lmr-hunter/data/lmr.db")
REPORTS_DIR  = Path(os.getenv("REPORTS_DIR", "/opt/lmr-hunter/data/reports"))
SYMBOL       = "BTCUSDT"

# 回弹判定：价格回到 VWAP 上方视为回弹成功
RECOVERY_WINDOWS_MS = [60_000, 120_000, 300_000]   # 1min, 2min, 5min
RECOVERY_LABEL      = {60_000: "1min", 120_000: "2min", 300_000: "5min"}


# ── 数据读取 ───────────────────────────────────────────────────────────────────

async def load_signals(db: aiosqlite.Connection, since_ts: int) -> list[dict]:
    """加载信号记录（仅 BTCUSDT SELL 方向，有 VWAP 的有效记录）。"""
    rows = await (await db.execute(
        """
        SELECT ts, liq_notional_window, liq_accel_ratio,
               vwap_15m, mid_price, deviation_bps
        FROM signals
        WHERE ts >= ?
          AND notes LIKE '%btc_sell%'
          AND vwap_15m IS NOT NULL
          AND deviation_bps IS NOT NULL
        ORDER BY ts ASC
        """,
        (since_ts,),
    )).fetchall()

    return [
        {
            "ts":          r[0],
            "liq_window":  r[1],
            "accel_ratio": r[2],
            "vwap":        r[3],
            "mid_price":   r[4],
            "deviation":   r[5],
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

    回弹成功定义：窗口内最高价 >= VWAP（即价格至少回到 VWAP）
    """
    results = []

    for sig in signals:
        vwap    = sig["vwap"]
        sig_ts  = sig["ts"]
        recovery = {}

        for win_ms in RECOVERY_WINDOWS_MS:
            hi, lo = await load_price_after(db, sig_ts, win_ms)
            if hi is None:
                # 没有后续成交数据（信号太新或成交未落库）
                recovery[win_ms] = None
            else:
                recovery[win_ms] = hi >= vwap

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
    lines.append("\n【1】整体回弹率（价格回到 VWAP 上方）")
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

    # ── 3. 按偏离深度分桶 ────────────────────────────────────────────────────
    lines.append("\n【3】按偏离深度（deviation_bps）— 5min 回弹率")
    dev_buckets = [
        (-1000, -30, "< -30bps"),
        (-30,   -20, "-30~-20"),
        (-20,   -10, "-20~-10"),
        (-10,     0, "-10~0"),
        (0,    1000, "> 0"),
    ]
    lines.append(f"  {'偏离区间':12s} {'回弹率':8s} {'样本':6s}")
    lines.append(f"  {'-'*28}")
    for b in bucket_analysis(results, "deviation", dev_buckets, 300_000):
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
    devs = [r["deviation"] for r in results]
    liqs = [r["liq_window"] for r in results]

    lines.append(f"  偏离 bps — avg={statistics.mean(devs):+.1f}  "
                 f"median={statistics.median(devs):+.1f}  "
                 f"min={min(devs):.1f}  max={max(devs):.1f}")
    lines.append(f"  强平窗口 — avg={statistics.mean(liqs):,.0f}  "
                 f"median={statistics.median(liqs):,.0f}  "
                 f"max={max(liqs):,.0f} USDT")

    # ── 6. 参数建议 ──────────────────────────────────────────────────────────
    lines.append("\n【6】参数校准建议")

    # 找到 5min 回弹率 > 55% 的偏离桶（高于随机的最低门槛）
    best_dev_buckets = []
    for b in bucket_analysis(results, "deviation", dev_buckets, 300_000):
        if b["rate"] is not None and b["rate"] > 0.55 and b["count"] >= 5:
            best_dev_buckets.append(b)

    if best_dev_buckets:
        lines.append(f"  回弹率 > 55% 的偏离区间:")
        for b in best_dev_buckets:
            lines.append(f"    {b['label']:12s} → {fmt_rate(b['rate'])} ({b['count']} 样本)")
        lines.append(f"  建议将 deviation_bps 阈值设为上述区间的上界")
    else:
        lines.append("  暂无足够样本确认最优偏离阈值（需要各桶 ≥ 5 个样本）")
        lines.append(f"  当前数据中偏离极值: {min(devs):.1f} bps")
        if min(devs) > -10:
            lines.append("  ⚠ 市场偏离幅度很小，-30bps 阈值在当前行情下可能永远不触发")
            lines.append("    建议临时将阈值降至 -5 bps 以积累更多样本")

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
        signals = await load_signals(db, since_ts)
        results = await analyze_recovery(db, signals)
        report  = build_report(results, days)
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
