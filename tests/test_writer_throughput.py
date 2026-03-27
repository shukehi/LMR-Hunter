"""
阶段 A 验收测试：trade 吞吐与完整性语义。

验收标准（来自 runtime-optimization-plan.md 阶段 A）：
  1. overflow_dropped 计数器正确追踪队列溢出
  2. 首次溢出触发 DEGRADED（经由下次 flush_trades 处理）
  3. DEGRADED 一旦触发，在进程生命周期内不自动恢复（即使写库恢复正常）
  4. isolated / lost 各自独立计数，不混用
  5. discarded 向后兼容 = overflow_dropped + isolated + lost
  6. integrity_ok() 正确反映三个子计数器
  7. max_batches 限制单轮排空次数，超出则仅发出 QUEUE_BACKLOG 警告，不触发 DEGRADED
  8. QUEUE_BACKLOG 不等于样本损坏：backlog 时 is_degraded 仍可为 False
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.gateway.models import Trade
from src.storage.writer import DatabaseWriter


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def make_trade(i: int = 0) -> Trade:
    return Trade(
        recv_ts=1_700_000_000_000 + i,
        trade_ts=1_700_000_000_000 + i,
        symbol="BTCUSDT",
        price=68000.0 + i,
        qty=0.001,
        is_buyer_maker=False,
    )


def make_writer(
    tmp_path: Path,
    batch_size: int = 5,
    maxlen: int = 10,
) -> tuple[DatabaseWriter, MagicMock]:
    """返回 (writer, mock_conn)。"""
    import aiosqlite
    conn = MagicMock(spec=aiosqlite.Connection)
    conn.executemany = AsyncMock(return_value=None)
    conn.commit      = AsyncMock(return_value=None)
    conn.execute     = AsyncMock(return_value=MagicMock(lastrowid=1))
    iso_dir = tmp_path / "isolation"
    writer  = DatabaseWriter(conn, trade_batch_size=batch_size,
                             trade_queue_maxlen=maxlen, isolation_dir=iso_dir)
    return writer, conn


# ── 1. overflow_dropped 计数 ──────────────────────────────────────────────────

class TestOverflowDropped:

    def test_no_overflow_within_maxlen(self, tmp_path):
        """队列未满时不产生 overflow_dropped。"""
        writer, _ = make_writer(tmp_path, maxlen=10)
        for i in range(10):
            writer.enqueue_trade(make_trade(i))
        assert writer.integrity["overflow_dropped"] == 0
        assert writer.integrity["enqueued"] == 10

    def test_overflow_when_full(self, tmp_path):
        """填满队列后再入队，每多一笔 overflow_dropped +1。"""
        writer, _ = make_writer(tmp_path, maxlen=5)
        # 填满
        for i in range(5):
            writer.enqueue_trade(make_trade(i))
        assert writer.integrity["overflow_dropped"] == 0
        # 再入队 3 笔 → 3 条溢出
        for i in range(5, 8):
            writer.enqueue_trade(make_trade(i))
        assert writer.integrity["overflow_dropped"] == 3
        assert writer.integrity["enqueued"] == 8
        # 队列长度仍是 maxlen
        assert writer.trade_queue_size == 5

    def test_overflow_needs_risk_event_flag(self, tmp_path):
        """首次溢出后 _needs_overflow_risk_event 应为 True。"""
        writer, _ = make_writer(tmp_path, maxlen=3)
        for i in range(3):
            writer.enqueue_trade(make_trade(i))
        assert not writer._needs_overflow_risk_event
        writer.enqueue_trade(make_trade(3))
        assert writer._needs_overflow_risk_event

    def test_overflow_flag_not_set_twice(self, tmp_path):
        """重复溢出不会重复设置标志（第二次溢出时已处于 degraded 或 pending 状态）。"""
        writer, _ = make_writer(tmp_path, maxlen=2)
        for i in range(2):
            writer.enqueue_trade(make_trade(i))
        writer.enqueue_trade(make_trade(2))   # 第一次溢出
        flag_after_first = writer._needs_overflow_risk_event
        writer.enqueue_trade(make_trade(3))   # 第二次溢出
        # 标志仍然是 True（未被清除），overflow_dropped=2
        assert writer._needs_overflow_risk_event == flag_after_first
        assert writer.integrity["overflow_dropped"] == 2


# ── 2. DEGRADED 触发（overflow 路径）──────────────────────────────────────────

class TestDegradedOnOverflow:

    @pytest.mark.asyncio
    async def test_overflow_triggers_degraded_on_flush(self, tmp_path):
        """
        溢出后 enqueue_trade 设置 _needs_overflow_risk_event；
        下一次 flush_trades 调用时触发 DEGRADED。
        """
        writer, _ = make_writer(tmp_path, batch_size=3, maxlen=3)
        # 填满队列
        for i in range(3):
            writer.enqueue_trade(make_trade(i))
        # 溢出 1 笔
        writer.enqueue_trade(make_trade(3))
        assert not writer.is_degraded    # flush 前还未 DEGRADED

        await writer.flush_trades()      # flush 时处理 risk_event

        assert writer.is_degraded
        assert writer.integrity["overflow_dropped"] == 1

    @pytest.mark.asyncio
    async def test_degraded_is_permanent_after_overflow(self, tmp_path):
        """
        DEGRADED 触发后，即使后续写库全部成功，is_degraded 也不恢复为 False。
        """
        writer, _ = make_writer(tmp_path, batch_size=3, maxlen=3)
        # 制造溢出
        for i in range(4):
            writer.enqueue_trade(make_trade(i))
        await writer.flush_trades()
        assert writer.is_degraded

        # 继续正常写入多批
        for i in range(10):
            writer.enqueue_trade(make_trade(100 + i))
        for _ in range(4):
            await writer.flush_trades()

        # 依然 DEGRADED
        assert writer.is_degraded
        assert writer.integrity["written"] > 0

    @pytest.mark.asyncio
    async def test_no_overflow_no_degraded(self, tmp_path):
        """没有溢出时，正常写入不会触发 DEGRADED。"""
        writer, _ = make_writer(tmp_path, batch_size=5, maxlen=20)
        for i in range(15):
            writer.enqueue_trade(make_trade(i))
        for _ in range(3):
            await writer.flush_trades()
        assert not writer.is_degraded
        assert writer.integrity["overflow_dropped"] == 0


# ── 3. isolated / lost 独立计数 ───────────────────────────────────────────────

class TestIsolatedAndLost:

    @pytest.mark.asyncio
    async def test_db_failure_increments_isolated(self, tmp_path):
        """DB 写入彻底失败，隔离文件成功 → isolated += batch_size。"""
        writer, conn = make_writer(tmp_path, batch_size=3, maxlen=20)
        conn.executemany = AsyncMock(side_effect=Exception("磁盘满"))
        conn.commit      = AsyncMock(side_effect=Exception("磁盘满"))

        for i in range(3):
            writer.enqueue_trade(make_trade(i))

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await writer.flush_trades()

        assert writer.integrity["isolated"] == 3
        assert writer.integrity["lost"]     == 0
        assert writer.integrity["overflow_dropped"] == 0

    @pytest.mark.asyncio
    async def test_db_and_isolation_failure_increments_lost(self, tmp_path):
        """DB + 隔离文件均失败 → lost += batch_size，isolated 不变。"""
        writer, conn = make_writer(tmp_path, batch_size=3, maxlen=20)
        conn.executemany = AsyncMock(side_effect=Exception("磁盘满"))
        conn.commit      = AsyncMock(side_effect=Exception("磁盘满"))

        for i in range(3):
            writer.enqueue_trade(make_trade(i))

        # 隔离文件也失败
        with patch("asyncio.sleep", new_callable=AsyncMock), \
             patch("asyncio.to_thread", side_effect=Exception("隔离目录不可写")):
            await writer.flush_trades()

        assert writer.integrity["lost"]     == 3
        assert writer.integrity["isolated"] == 0

    @pytest.mark.asyncio
    async def test_discarded_is_sum(self, tmp_path):
        """discarded 向后兼容 = overflow_dropped + isolated + lost。"""
        writer, conn = make_writer(tmp_path, batch_size=2, maxlen=4)

        # 制造 overflow_dropped=1
        for i in range(5):
            writer.enqueue_trade(make_trade(i))

        # 制造 isolated=2（DB 失败）
        conn.executemany = AsyncMock(side_effect=Exception("失败"))
        conn.commit      = AsyncMock(side_effect=Exception("失败"))
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await writer.flush_trades()

        intg = writer.integrity
        assert intg["discarded"] == (
            intg["overflow_dropped"] + intg["isolated"] + intg["lost"]
        )


# ── 4. integrity_ok() ─────────────────────────────────────────────────────────

class TestIntegrityOk:

    def test_ok_when_all_zero(self, tmp_path):
        writer, _ = make_writer(tmp_path)
        assert writer.integrity_ok() is True

    @pytest.mark.asyncio
    async def test_not_ok_when_overflow_dropped(self, tmp_path):
        writer, _ = make_writer(tmp_path, maxlen=2)
        for i in range(3):
            writer.enqueue_trade(make_trade(i))
        # overflow_dropped=1，即使没有 flush 过
        assert writer.integrity_ok() is False

    @pytest.mark.asyncio
    async def test_not_ok_when_isolated(self, tmp_path):
        writer, conn = make_writer(tmp_path, batch_size=2, maxlen=10)
        conn.executemany = AsyncMock(side_effect=Exception("失败"))
        conn.commit      = AsyncMock(side_effect=Exception("失败"))
        for i in range(2):
            writer.enqueue_trade(make_trade(i))
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await writer.flush_trades()
        assert writer.integrity_ok() is False


# ── 5. QUEUE_BACKLOG：有积压但无样本损坏，不触发 DEGRADED ──────────────────────

class TestQueueBacklog:

    @pytest.mark.asyncio
    async def test_backlog_does_not_degrade(self, tmp_path):
        """
        队列有积压但未溢出（没有 overflow_dropped），系统不应进入 DEGRADED。
        使用 maxlen=100，只 flush 部分数据模拟积压。
        """
        writer, _ = make_writer(tmp_path, batch_size=5, maxlen=100)
        # 入队 50 笔（未溢出）
        for i in range(50):
            writer.enqueue_trade(make_trade(i))
        # 只 flush 1 批（5 条），还剩 45 条在队列
        await writer.flush_trades()

        assert writer.trade_queue_size == 45
        assert not writer.is_degraded
        assert writer.integrity["overflow_dropped"] == 0
        assert writer.integrity_ok() is True
