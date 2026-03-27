"""
阶段 1 验收测试：数据完整性保证。

验证：
  1. 正常写入时样本不丢失
  2. 写入失败时 batch 不从队列移除（先复制再写）
  3. 重试成功后样本最终写入
  4. 全部重试失败后写入隔离文件，discarded 计数正确
  5. discarded = 0 时 integrity_ok() 返回 True
  6. 连续失败超阈值时系统标记 DEGRADED
"""
from __future__ import annotations

import asyncio
import json
import tempfile
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


async def make_writer(tmp_path: Path) -> tuple[DatabaseWriter, MagicMock]:
    """返回 (writer, mock_conn)。mock_conn 可控制 executemany 的行为。"""
    import aiosqlite
    conn = MagicMock(spec=aiosqlite.Connection)
    conn.executemany = AsyncMock(return_value=None)
    conn.commit      = AsyncMock(return_value=None)
    iso_dir = tmp_path / "isolation"
    writer  = DatabaseWriter(conn, trade_batch_size=5, isolation_dir=iso_dir)
    return writer, conn


# ── 测试用例 ──────────────────────────────────────────────────────────────────

class TestTradeIntegrity:

    @pytest.mark.asyncio
    async def test_normal_write_removes_from_queue(self, tmp_path):
        """正常写入后队列清空，完整性计数正确。"""
        writer, conn = await make_writer(tmp_path)

        for i in range(3):
            writer.enqueue_trade(make_trade(i))

        assert writer.trade_queue_size == 3
        assert writer.integrity["enqueued"] == 3

        written = await writer.flush_trades()

        assert written == 3
        assert writer.trade_queue_size == 0
        assert writer.integrity["written"] == 3
        assert writer.integrity["discarded"] == 0
        assert writer.integrity_ok() is True

    @pytest.mark.asyncio
    async def test_failure_does_not_lose_samples(self, tmp_path):
        """
        关键测试：写入失败时 batch 不从队列移除，样本不丢失。
        旧代码会在 popleft() 后才写库，失败则丢失。
        新代码先复制 batch，成功后才移除。
        """
        writer, conn = await make_writer(tmp_path)
        # 所有写入都失败
        conn.executemany = AsyncMock(side_effect=Exception("磁盘满"))
        conn.commit      = AsyncMock(side_effect=Exception("磁盘满"))

        for i in range(3):
            writer.enqueue_trade(make_trade(i))

        written = await writer.flush_trades()

        assert written == 0
        assert writer.trade_queue_size == 0          # 已写入隔离文件后移除
        assert writer.integrity["discarded"] == 3    # 记录为丢弃
        assert writer.integrity["written"]   == 0
        assert writer.integrity_ok() is False        # 有丢弃 → 不 OK

    @pytest.mark.asyncio
    async def test_retry_success_counts_correctly(self, tmp_path):
        """第 1 次失败、第 2 次成功 → 重试计数 = 1，written = batch_size。"""
        writer, conn = await make_writer(tmp_path)
        call_count = {"n": 0}

        async def flaky(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("暂时性错误")

        conn.executemany = AsyncMock(side_effect=flaky)
        conn.commit      = AsyncMock(return_value=None)

        for i in range(3):
            writer.enqueue_trade(make_trade(i))

        with patch("asyncio.sleep", new_callable=AsyncMock):
            written = await writer.flush_trades()

        assert written == 3
        assert writer.integrity["retried"]   == 1
        assert writer.integrity["written"]   == 3
        assert writer.integrity["discarded"] == 0
        assert writer.integrity_ok() is True

    @pytest.mark.asyncio
    async def test_isolation_file_written_on_total_failure(self, tmp_path):
        """全部重试失败后隔离文件必须存在且包含所有成交。"""
        writer, conn = await make_writer(tmp_path)
        conn.executemany = AsyncMock(side_effect=Exception("彻底失败"))
        conn.commit      = AsyncMock(side_effect=Exception("彻底失败"))

        for i in range(3):
            writer.enqueue_trade(make_trade(i))

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await writer.flush_trades()

        iso_files = list((tmp_path / "isolation").glob("trades_*.jsonl"))
        assert len(iso_files) == 1, "隔离文件应存在"

        lines = iso_files[0].read_text().strip().split("\n")
        assert len(lines) == 3, "隔离文件应包含 3 条成交"

        first = json.loads(lines[0])
        assert first["symbol"] == "BTCUSDT"
        assert "price" in first

    @pytest.mark.asyncio
    async def test_degraded_after_db_failure(self, tmp_path):
        """
        任何真实样本损坏（isolated > 0）都应立即标记 DEGRADED 并写 risk_events。

        新语义（阶段 A）：DEGRADED 在首次 isolated/lost/overflow_dropped > 0 时立即触发，
        不再等待连续失败累计到阈值。
        discarded（向后兼容）= overflow_dropped + isolated + lost。
        """
        import aiosqlite
        conn = MagicMock(spec=aiosqlite.Connection)
        # executemany 总是失败，但 execute（用于写 risk_events）成功
        conn.executemany = AsyncMock(side_effect=Exception("持续失败"))
        conn.commit      = AsyncMock(return_value=None)
        conn.execute     = AsyncMock(return_value=MagicMock(lastrowid=1))

        iso_dir = tmp_path / "isolation"
        writer  = DatabaseWriter(conn, trade_batch_size=1, isolation_dir=iso_dir)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            for _ in range(3):
                writer.enqueue_trade(make_trade())
                await writer.flush_trades()

        assert writer.is_degraded is True
        # discarded = isolated（DB 失败 → 已写隔离文件）= 3
        assert writer.integrity["discarded"] == 3
