"""
阶段 D 验收测试：受污染数据可见性（run validity）。

验证 load_run_validity() 的判定逻辑：
  1. 无心跳记录 → UNKNOWN
  2. 所有心跳 OK、无损坏计数 → TRUSTED
  3. 任何心跳 status=DEGRADED → CONTAMINATED
  4. notes 中 overflow_dropped > 0 → CONTAMINATED
  5. notes 中 isolated > 0 → CONTAMINATED
  6. notes 中 lost > 0 → CONTAMINATED
  7. CRITICAL 风控事件被正确收集
  8. since_ts 过滤有效（只看窗口内数据）
  9. notes JSON 解析失败时不崩溃（防御性处理）
 10. build_validity_section() 输出包含正确 badge 和 guidance
"""
from __future__ import annotations

import asyncio
import json
import time

import aiosqlite
import pytest
import pytest_asyncio

from src.analysis.daily_report import build_validity_section, load_run_validity

# ── 工具函数 ──────────────────────────────────────────────────────────────────

NOW_MS = 1_743_000_000_000   # 固定基准时间戳，避免依赖 time.time()


async def make_db() -> aiosqlite.Connection:
    """创建内存 SQLite，建立 validity 测试所需的两张表。"""
    db = await aiosqlite.connect(":memory:")
    await db.execute("""
        CREATE TABLE service_heartbeats (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        INTEGER NOT NULL,
            component TEXT,
            status    TEXT,
            msg_total INTEGER DEFAULT 0,
            liq_count INTEGER DEFAULT 0,
            trade_count INTEGER DEFAULT 0,
            latency_p50 REAL,
            latency_p95 REAL,
            latency_p99 REAL,
            reconnects INTEGER DEFAULT 0,
            notes     TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE risk_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ts         INTEGER NOT NULL,
            component  TEXT,
            event_type TEXT,
            severity   TEXT,
            message    TEXT,
            context    TEXT
        )
    """)
    await db.commit()
    return db


async def insert_heartbeat(
    db: aiosqlite.Connection,
    ts: int,
    status: str = "OK",
    overflow_dropped: int = 0,
    isolated: int = 0,
    lost: int = 0,
) -> None:
    notes = json.dumps({
        "trade_queue_size": 0,
        "overflow_dropped": overflow_dropped,
        "isolated":         isolated,
        "lost":             lost,
        "episode_stats":    {},
    })
    await db.execute(
        "INSERT INTO service_heartbeats (ts, component, status, notes) VALUES (?, ?, ?, ?)",
        (ts, "gateway", status, notes),
    )
    await db.commit()


async def insert_risk_event(
    db: aiosqlite.Connection,
    ts: int,
    severity: str = "CRITICAL",
    event_type: str = "WRITE_FAILURE_PERSISTENT",
    message: str = "test event",
) -> None:
    await db.execute(
        """INSERT INTO risk_events (ts, component, event_type, severity, message)
           VALUES (?, ?, ?, ?, ?)""",
        (ts, "storage.writer", event_type, severity, message),
    )
    await db.commit()


# ── 测试用例 ──────────────────────────────────────────────────────────────────

class TestLoadRunValidity:

    @pytest.mark.asyncio
    async def test_no_heartbeats_returns_unknown(self):
        """无心跳记录时返回 UNKNOWN。"""
        db = await make_db()
        try:
            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "UNKNOWN"
            assert result["heartbeat_count"] == 0
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_all_ok_heartbeats_returns_trusted(self):
        """所有心跳 OK 且无损坏计数 → TRUSTED。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS,       status="OK")
            await insert_heartbeat(db, NOW_MS + 60_000, status="OK")
            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "TRUSTED"
            assert result["overflow_dropped"] == 0
            assert result["isolated"] == 0
            assert result["lost"] == 0
            assert result["degraded_heartbeat_count"] == 0
            assert result["first_degraded_ts"] is None
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_degraded_status_returns_contaminated(self):
        """任何一条心跳 status=DEGRADED → CONTAMINATED。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS,            status="OK")
            await insert_heartbeat(db, NOW_MS + 60_000,   status="DEGRADED")
            await insert_heartbeat(db, NOW_MS + 120_000,  status="DEGRADED")
            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "CONTAMINATED"
            assert result["degraded_heartbeat_count"] == 2
            assert result["first_degraded_ts"] == NOW_MS + 60_000
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_overflow_dropped_in_notes_returns_contaminated(self):
        """notes 中 overflow_dropped > 0 → CONTAMINATED。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS, status="OK", overflow_dropped=103514)
            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "CONTAMINATED"
            assert result["overflow_dropped"] == 103514
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_isolated_in_notes_returns_contaminated(self):
        """notes 中 isolated > 0 → CONTAMINATED。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS, status="OK", isolated=5)
            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "CONTAMINATED"
            assert result["isolated"] == 5
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_lost_in_notes_returns_contaminated(self):
        """notes 中 lost > 0 → CONTAMINATED。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS, status="OK", lost=2)
            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "CONTAMINATED"
            assert result["lost"] == 2
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_max_of_cumulative_counters(self):
        """取各心跳 notes 中 overflow_dropped 的最大值，而非累加。"""
        db = await make_db()
        try:
            # 模拟进程运行中 overflow_dropped 累计增长
            await insert_heartbeat(db, NOW_MS,           overflow_dropped=1000)
            await insert_heartbeat(db, NOW_MS + 60_000,  overflow_dropped=5000)
            await insert_heartbeat(db, NOW_MS + 120_000, overflow_dropped=5000)
            result = await load_run_validity(db, since_ts=0)
            assert result["overflow_dropped"] == 5000   # 最大值，不是 1000+5000+5000
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_since_ts_filters_old_heartbeats(self):
        """since_ts 过滤：早于窗口的心跳不应影响结果。"""
        db = await make_db()
        try:
            # 在窗口之前发生了 overflow（旧污染 run）
            await insert_heartbeat(db, NOW_MS - 1000, status="DEGRADED",
                                   overflow_dropped=9999)
            # 窗口内全部干净
            await insert_heartbeat(db, NOW_MS,       status="OK")
            await insert_heartbeat(db, NOW_MS + 60_000, status="OK")

            result = await load_run_validity(db, since_ts=NOW_MS)
            assert result["status"] == "TRUSTED"
            assert result["heartbeat_count"] == 2
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_critical_risk_events_collected(self):
        """CRITICAL 风控事件被正确收集，INFO 事件被过滤。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS, status="OK")
            await insert_risk_event(db, NOW_MS + 1000, severity="CRITICAL",
                                    event_type="WRITE_FAILURE_PERSISTENT",
                                    message="DB 写入失败")
            await insert_risk_event(db, NOW_MS + 2000, severity="INFO",
                                    event_type="STARTUP",
                                    message="系统启动")   # 应被过滤

            result = await load_run_validity(db, since_ts=0)
            assert len(result["critical_risk_events"]) == 1
            assert result["critical_risk_events"][0]["event_type"] == "WRITE_FAILURE_PERSISTENT"
            assert result["critical_risk_events"][0]["severity"] == "CRITICAL"
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_malformed_notes_json_does_not_crash(self):
        """notes 字段格式损坏时不崩溃，视为无损坏计数（防御性处理）。"""
        db = await make_db()
        try:
            await db.execute(
                "INSERT INTO service_heartbeats (ts, component, status, notes) "
                "VALUES (?, ?, ?, ?)",
                (NOW_MS, "gateway", "OK", "{broken json"),
            )
            await db.execute(
                "INSERT INTO service_heartbeats (ts, component, status, notes) "
                "VALUES (?, ?, ?, ?)",
                (NOW_MS + 1000, "gateway", "OK", None),
            )
            await db.commit()

            result = await load_run_validity(db, since_ts=0)
            assert result["status"] == "TRUSTED"   # 没有因解析失败而误报 CONTAMINATED
            assert result["heartbeat_count"] == 2
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_since_ts_filters_risk_events(self):
        """since_ts 同样过滤 risk_events，窗口外的风控事件不影响判断。"""
        db = await make_db()
        try:
            await insert_heartbeat(db, NOW_MS, status="OK")
            # 在窗口之前的 CRITICAL 事件（属于旧 run）
            await insert_risk_event(db, NOW_MS - 5000, severity="CRITICAL")
            result = await load_run_validity(db, since_ts=NOW_MS)
            assert len(result["critical_risk_events"]) == 0
        finally:
            await db.close()


class TestBuildValiditySection:

    def _make_validity(self, status: str, **kwargs) -> dict:
        base = {
            "status":                  status,
            "overflow_dropped":        0,
            "isolated":                0,
            "lost":                    0,
            "degraded_heartbeat_count": 0,
            "first_degraded_ts":       None,
            "critical_risk_events":    [],
            "heartbeat_count":         10,
        }
        base.update(kwargs)
        return base

    def test_trusted_badge_in_output(self):
        v = self._make_validity("TRUSTED")
        out = build_validity_section(v)
        assert "TRUSTED" in out
        assert "✓" in out

    def test_contaminated_badge_and_warning_in_output(self):
        v = self._make_validity(
            "CONTAMINATED",
            overflow_dropped=103514,
            degraded_heartbeat_count=3,
            first_degraded_ts=NOW_MS,
        )
        out = build_validity_section(v)
        assert "CONTAMINATED" in out
        assert "✗" in out
        assert "不得用于策略参数决策" in out
        assert "103514" in out

    def test_unknown_badge_in_output(self):
        v = self._make_validity("UNKNOWN", heartbeat_count=0)
        out = build_validity_section(v)
        assert "UNKNOWN" in out
        assert "?" in out

    def test_risk_events_shown_in_contaminated_output(self):
        events = [
            {
                "ts": NOW_MS,
                "component": "storage.writer",
                "event_type": "WRITE_FAILURE_PERSISTENT",
                "severity": "CRITICAL",
                "message": "批量写入彻底失败",
            }
        ]
        v = self._make_validity("CONTAMINATED", critical_risk_events=events,
                                overflow_dropped=1)
        out = build_validity_section(v)
        assert "WRITE_FAILURE_PERSISTENT" in out

    def test_no_risk_events_section_when_empty(self):
        v = self._make_validity("TRUSTED")
        out = build_validity_section(v)
        assert "CRITICAL/ERROR 风控事件" not in out

    def test_first_degraded_ts_shown_when_present(self):
        v = self._make_validity(
            "CONTAMINATED",
            overflow_dropped=1,
            first_degraded_ts=NOW_MS,
        )
        out = build_validity_section(v)
        assert "首次 DEGRADED 时间" in out
        assert "2025" in out or "2026" in out   # 年份在格式化时间戳中
