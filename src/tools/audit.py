"""
数据可靠性审计工具。

三个独立检查，回答"采集到的数据是否完整可信"：

  检查 1：aggTrade 序列号连续性
    原理：币安 aggTrade 的 "a" 字段严格递增。相邻两条 agg_id 不连续 → 中间有漏采。
    局限：历史行（agg_id=0）无法参与检查，只有迁移后的新数据有效。

  检查 2：REST vs WebSocket 强平数量交叉核对
    原理：用 REST /fapi/v1/forceOrders 独立拉取同一时间窗口的强平数据，
          与 raw_liquidations 对比数量和名义金额。
    阈值：数量偏差 > 5% 或名义金额偏差 > 5% → WARN；> 20% → FAIL。

  检查 3：min_mid_price 与 K 线价格区间一致性
    原理：episode.min_mid_price = depth20 盘口中间价最低值。
          中间价不应高于对应时间段的 K 线最高价（high），
          也不应低于 K 线最低价（low）的 99.5%（允许少量盘口/成交时序差异）。
    偏离 → 说明 depth 快照与成交数据之间存在数据问题。

用法：
  python3.12 -m src.tools.audit --db /opt/lmr-hunter/data/lmr.db
  python3.12 -m src.tools.audit --db /opt/lmr-hunter/data/lmr.db --days 7
  python3.12 -m src.tools.audit --db /opt/lmr-hunter/data/lmr.db --check gaps
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from dataclasses import dataclass, field

def _fmt_ts(ts_ms: int) -> str:
    """将毫秒时间戳格式化为 UTC 时间字符串。"""
    import datetime
    return datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")


# ── 判定阈值 ────────────────────────────────────────────────────────────────
_LIQ_WARN_PCT  = 5.0   # 强平数量/金额偏差 WARN 阈值（%）
_LIQ_FAIL_PCT  = 20.0  # 强平数量/金额偏差 FAIL 阈值（%）
_PRICE_LOW_TOL = 0.995 # min_mid_price 允许低于 kline.low 的最大比例（0.5% 容差）


# ── 结果容器 ─────────────────────────────────────────────────────────────────

@dataclass
class AuditResult:
    name: str
    status: str          # PASS / WARN / FAIL / SKIP
    summary: str
    details: list[str] = field(default_factory=list)

    def print(self) -> None:
        icon = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌", "SKIP": "⏭ "}.get(self.status, "?")
        print(f"\n{icon} [{self.status}] {self.name}")
        print(f"   {self.summary}")
        for d in self.details[:20]:
            print(f"   · {d}")
        if len(self.details) > 20:
            print(f"   · ... 还有 {len(self.details) - 20} 条（截断）")


# ── 检查 1：aggTrade 序列号连续性 ────────────────────────────────────────────

def check_agg_id_gaps(db_path: str, symbol: str) -> AuditResult:
    name = "aggTrade 序列号连续性"
    con = sqlite3.connect(db_path)
    try:
        # 只检查 agg_id > 0 的行（历史行为 DEFAULT 0，不参与检查）
        count_valid = con.execute(
            "SELECT COUNT(*) FROM raw_trades WHERE symbol=? AND agg_id > 0",
            (symbol,)
        ).fetchone()[0]

        if count_valid < 2:
            return AuditResult(
                name, "SKIP",
                f"有效 agg_id 行数={count_valid}，不足以检查（历史行 agg_id=0，迁移后新数据才有效）"
            )

        # 使用 LAG() 窗口函数按 symbol 分区，避免多 symbol 交错 id 导致误判。
        # id 为全局自增，BTC/ETH 行交错存储，JOIN b.id = a.id+1 无法正确配对同 symbol 行。
        rows = con.execute(
            """
            WITH ordered AS (
                SELECT
                    agg_id,
                    trade_ts,
                    LAG(agg_id) OVER (ORDER BY trade_ts, id) AS prev_agg_id
                FROM raw_trades
                WHERE symbol = ? AND agg_id > 0
            )
            SELECT
                prev_agg_id                              AS cur_id,
                agg_id                                   AS next_id,
                agg_id - prev_agg_id - 1                 AS gap,
                datetime(trade_ts / 1000, 'unixepoch')   AS ts
            FROM ordered
            WHERE prev_agg_id IS NOT NULL
              AND agg_id != prev_agg_id + 1
            ORDER BY trade_ts DESC
            LIMIT 50
            """,
            (symbol,)
        ).fetchall()

        total_gaps   = len(rows)
        total_missed = sum(r[2] for r in rows if r[2] > 0)

        if total_gaps == 0:
            return AuditResult(
                name, "PASS",
                f"检查 {count_valid:,} 条有效记录，未发现序列号跳跃",
            )

        status = "FAIL" if total_missed > 100 else "WARN"
        details = [
            f"时间={r[3]}  cur={r[0]}  next={r[1]}  漏={r[2]}"
            for r in rows
        ]
        return AuditResult(
            name, status,
            f"发现 {total_gaps} 处跳跃，累计漏采约 {total_missed} 条 aggTrade",
            details,
        )
    finally:
        con.close()


# ── 检查 2：REST vs WebSocket 强平交叉核对 ───────────────────────────────────

def check_liq_cross_validation(
    db_path: str, symbol: str, days: int
) -> AuditResult:
    """
    强平采集内部一致性检查。

    币安 /fapi/v1/allForceOrders 已停止维护，无法做外部 REST 交叉核对。
    改为内部一致性检查：寻找"K 线大跌但无强平记录"的时间窗口。

    逻辑：
    - 若某 5 分钟内最大跌幅 > DROP_BPS bps，而该时间窗口内 raw_liquidations 为空，
      则说明强平流可能在此窗口漏采（大跌通常伴随强平级联）。
    - 不是 100% 确定漏采（可能是纯抛售非强平），但值得告警。
    """
    name = "强平流内部一致性检查（大跌 vs 强平记录）"

    _DROP_BPS = 50   # 5 分钟跌幅阈值（bps），超过此阈值的窗口应有强平记录

    end_ts   = int(time.time() * 1000)
    start_ts = end_ts - days * 86_400_000

    con = sqlite3.connect(db_path)
    try:
        # 找出每 5 分钟窗口的最大跌幅（open - low）
        drop_windows = con.execute(
            """
            SELECT
                open_ts,
                close_ts,
                open,
                low,
                ROUND((open - low) / open * 10000, 1) AS drop_bps
            FROM raw_klines
            WHERE symbol = ?
              AND open_ts  >= ?
              AND is_closed = 1
              AND (open - low) / open * 10000 > ?
            ORDER BY drop_bps DESC
            """,
            (symbol, start_ts, _DROP_BPS)
        ).fetchall()

        suspicious: list[str] = []
        for open_ts, close_ts, price_open, price_low, drop_bps in drop_windows:
            liq_count = con.execute(
                """
                SELECT COUNT(*) FROM raw_liquidations
                WHERE symbol = ? AND event_ts BETWEEN ? AND ?
                """,
                (symbol, open_ts, close_ts)
            ).fetchone()[0]

            if liq_count == 0:
                ts_str = _fmt_ts(open_ts)
                suspicious.append(
                    f"{ts_str}  跌幅={drop_bps} bps  open={price_open:.1f}→low={price_low:.1f}  强平=0"
                )

        total_drops = len(drop_windows)
    finally:
        con.close()

    if total_drops == 0:
        return AuditResult(
            name, "PASS",
            f"过去 {days} 天无 K 线单根跌幅 > {_DROP_BPS} bps，无需检查"
        )

    ratio = len(suspicious) / total_drops
    details = [
        f"检查 {total_drops} 个跌幅 > {_DROP_BPS} bps 的 K 线窗口",
        f"其中 {len(suspicious)} 个窗口无任何强平记录（占比 {ratio*100:.0f}%）",
    ] + suspicious

    if ratio > 0.5:
        status = "FAIL"
        summary = f"{len(suspicious)}/{total_drops} 个大跌窗口无强平记录，强平流可能存在系统性漏采"
    elif ratio > 0.2:
        status = "WARN"
        summary = f"{len(suspicious)}/{total_drops} 个大跌窗口无强平记录，建议核查"
    else:
        status = "PASS"
        summary = f"大跌窗口中强平记录覆盖率正常（无记录窗口={len(suspicious)}/{total_drops}）"

    return AuditResult(name, status, summary, details)


# ── 检查 3：min_mid_price 与 K 线区间一致性 ──────────────────────────────────

def check_price_consistency(db_path: str) -> AuditResult:
    name = "min_mid_price 与 K 线价格区间一致性"
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            """
            SELECT
                e.episode_id,
                e.min_mid_price,
                MIN(k.low)  AS period_low,
                MAX(k.high) AS period_high,
                datetime(e.start_event_ts / 1000, 'unixepoch') AS start_time
            FROM liquidation_episodes e
            JOIN raw_klines k
              ON  k.symbol   = e.symbol
              AND k.open_ts  <= e.end_event_ts
              AND k.close_ts >= e.start_event_ts
            WHERE e.min_mid_price IS NOT NULL
            GROUP BY e.episode_id
            """,
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return AuditResult(name, "SKIP", "无 episode 数据可检查")

    above_high: list[str] = []   # mid_price 高于 K 线最高价 → 异常
    below_low:  list[str] = []   # mid_price 低于 K 线最低价 × 容差 → 异常

    for episode_id, mid, low, high, ts in rows:
        if mid is None or low is None or high is None:
            continue
        if mid > high * 1.001:
            above_high.append(
                f"{ts}  {episode_id[-12:]}  mid={mid:.2f} > high={high:.2f}"
            )
        elif mid < low * _PRICE_LOW_TOL:
            below_low.append(
                f"{ts}  {episode_id[-12:]}  mid={mid:.2f} < low={low:.2f} × {_PRICE_LOW_TOL}"
            )

    total_checked = len(rows)
    issues = above_high + below_low

    if not issues:
        return AuditResult(
            name, "PASS",
            f"检查 {total_checked} 个 episode，min_mid_price 均在 K 线价格区间内",
        )

    status = "FAIL" if len(issues) > total_checked * 0.1 else "WARN"
    details = (
        [f"[高于high] {d}" for d in above_high] +
        [f"[低于low]  {d}" for d in below_low]
    )
    return AuditResult(
        name, status,
        f"{len(issues)}/{total_checked} 个 episode 存在价格区间异常",
        details,
    )


# ── 主入口 ────────────────────────────────────────────────────────────────────

def run_audit(db_path: str, symbol: str, days: int, checks: set[str]) -> int:
    """执行审计，返回退出码（0=全部 PASS/WARN，1=有 FAIL）。"""
    print(f"\n{'═' * 60}")
    print(f"  LMR-Hunter 数据可靠性审计")
    print(f"  DB: {db_path}")
    print(f"  Symbol: {symbol}  |  回溯: {days} 天")
    print(f"{'═' * 60}")

    results: list[AuditResult] = []

    if "gaps" in checks or "all" in checks:
        results.append(check_agg_id_gaps(db_path, symbol))

    if "liq" in checks or "all" in checks:
        results.append(check_liq_cross_validation(db_path, symbol, days))

    if "price" in checks or "all" in checks:
        results.append(check_price_consistency(db_path))

    for r in results:
        r.print()

    print(f"\n{'─' * 60}")
    statuses = [r.status for r in results]
    if "FAIL" in statuses:
        print("  总结：FAIL — 至少一项检查未通过，建议立即排查")
        return 1
    if "WARN" in statuses:
        print("  总结：WARN — 存在需要关注的偏差，建议检查")
        return 0
    print("  总结：PASS — 全部检查通过")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="LMR-Hunter 数据可靠性审计")
    parser.add_argument("--db",     required=True,  help="SQLite 数据库路径")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days",   type=int, default=3, help="大跌内部一致性检查回溯天数")
    parser.add_argument(
        "--check",
        choices=["all", "gaps", "liq", "price"],
        default="all",
        help="运行指定检查（默认全部）",
    )
    args = parser.parse_args()
    checks = {args.check}
    sys.exit(run_audit(args.db, args.symbol, args.days, checks))


if __name__ == "__main__":
    main()
