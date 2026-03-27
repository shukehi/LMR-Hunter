"""
BUG-9 / BUG-19 / BUG-5 回归测试。

BUG-9  — FeatureCalculator._liquidation_windows 的驱逐逻辑
         使用高水位标记（_max_liq_ts），防止乱序事件中较小的 now_ms
         与已经驱逐操作不一致，保证驱逐阈值单调递增。

BUG-19 — models.py 中各 from_raw / from_rest 方法现在调用 _check_ts，
         对非正数、早于 2020 年、超出未来容忍上限的时间戳抛 ValueError。

BUG-5  — DatabaseWriter.flush_trades 的并发保护
         trade_flusher（每秒触发）与 _shutdown drain 循环可能并发执行
         flush_trades，导致 executemany 重复写入与计数膨胀。
         通过 _flushing 标志实现互斥，第二次调用立即返回 0。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.features.calculator import FeatureCalculator
from src.gateway.models import (
    _TS_FUTURE_TOLERANCE_MS,
    _TS_MIN_MS,
    Depth,
    Kline,
    Liquidation,
    Trade,
)

# ── 测试辅助 ───────────────────────────────────────────────────────────────────

# 合法的基准接收时间戳（2024 年，远离边界）
_RECV_TS = 1_700_000_000_000  # 2023-11-14 22:13:20 UTC (ms)


def _make_liq_raw(event_ts: int, recv_ts: int | None = None) -> tuple[dict, int]:
    """构造 Liquidation.from_raw 所需的 data dict + recv_ts。"""
    recv_ts = recv_ts if recv_ts is not None else _RECV_TS
    data = {
        "E": str(event_ts),
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "q": "0.01",
            "ap": "50000.0",
            "p": "50000.0",
            "X": "FILLED",
        },
    }
    return data, recv_ts


def _make_trade_raw(trade_ts: int, recv_ts: int | None = None) -> tuple[dict, int]:
    recv_ts = recv_ts if recv_ts is not None else _RECV_TS
    data = {
        "T": str(trade_ts),
        "s": "BTCUSDT",
        "p": "50000.0",
        "q": "0.001",
        "m": False,
    }
    return data, recv_ts


def _make_depth_raw(event_ts: int, recv_ts: int | None = None) -> tuple[dict, int]:
    recv_ts = recv_ts if recv_ts is not None else _RECV_TS
    data = {
        "E": str(event_ts),
        "s": "BTCUSDT",
        "b": [["50000.0", "1.0"]],
        "a": [["50001.0", "1.0"]],
    }
    return data, recv_ts


def _make_kline_raw(open_ts: int, recv_ts: int | None = None) -> tuple[dict, int]:
    recv_ts = recv_ts if recv_ts is not None else _RECV_TS
    close_ts = open_ts + 59_999
    data = {
        "k": {
            "t": str(open_ts),
            "T": str(close_ts),
            "s": "BTCUSDT",
            "o": "50000.0",
            "h": "50100.0",
            "l": "49900.0",
            "c": "50050.0",
            "v": "10.0",
            "x": True,
        }
    }
    return data, recv_ts


def _make_kline_rest_row(open_ts: int, recv_ts: int | None = None):
    """构造 Kline.from_rest 所需的 row list + recv_ts。"""
    recv_ts = recv_ts if recv_ts is not None else _RECV_TS
    close_ts = open_ts + 59_999
    # REST API 返回格式: [open_ts, open, high, low, close, vol, close_ts, ...]
    row = [
        str(open_ts),
        "50000.0",
        "50100.0",
        "49900.0",
        "50050.0",
        "10.0",
        str(close_ts),
    ]
    return row, "BTCUSDT", recv_ts


# ══════════════════════════════════════════════════════════════════════════════
# BUG-19：时间戳合理性校验
# ══════════════════════════════════════════════════════════════════════════════


class TestBug19TimestampValidation:
    """_check_ts 应在非法时间戳时抛 ValueError，合法时通过。"""

    # ── 非正数 ──────────────────────────────────────────────────────────────

    def test_liquidation_zero_event_ts_raises(self):
        data, recv = _make_liq_raw(0)
        with pytest.raises(ValueError, match="非正数"):
            Liquidation.from_raw(data, recv)

    def test_liquidation_negative_event_ts_raises(self):
        data, recv = _make_liq_raw(-1_000_000)
        with pytest.raises(ValueError, match="非正数"):
            Liquidation.from_raw(data, recv)

    def test_trade_zero_trade_ts_raises(self):
        data, recv = _make_trade_raw(0)
        with pytest.raises(ValueError, match="非正数"):
            Trade.from_raw(data, recv)

    def test_depth_zero_event_ts_raises(self):
        data, recv = _make_depth_raw(0)
        with pytest.raises(ValueError, match="非正数"):
            Depth.from_raw(data, recv)

    def test_kline_ws_zero_open_ts_raises(self):
        data, recv = _make_kline_raw(0)
        with pytest.raises(ValueError, match="非正数"):
            Kline.from_raw(data, recv)

    def test_kline_rest_zero_open_ts_raises(self):
        row, symbol, recv = _make_kline_rest_row(0)
        with pytest.raises(ValueError, match="非正数"):
            Kline.from_rest(row, symbol, recv)

    # ── 早于 2020-01-01 ──────────────────────────────────────────────────────

    def test_liquidation_pre2020_ts_raises(self):
        data, recv = _make_liq_raw(_TS_MIN_MS - 1)
        with pytest.raises(ValueError, match="早于 2020-01-01"):
            Liquidation.from_raw(data, recv)

    def test_trade_pre2020_ts_raises(self):
        data, recv = _make_trade_raw(_TS_MIN_MS - 1)
        with pytest.raises(ValueError, match="早于 2020-01-01"):
            Trade.from_raw(data, recv)

    def test_depth_pre2020_ts_raises(self):
        data, recv = _make_depth_raw(_TS_MIN_MS - 1)
        with pytest.raises(ValueError, match="早于 2020-01-01"):
            Depth.from_raw(data, recv)

    def test_kline_ws_pre2020_ts_raises(self):
        data, recv = _make_kline_raw(_TS_MIN_MS - 1)
        with pytest.raises(ValueError, match="早于 2020-01-01"):
            Kline.from_raw(data, recv)

    def test_kline_rest_pre2020_ts_raises(self):
        row, symbol, recv = _make_kline_rest_row(_TS_MIN_MS - 1)
        with pytest.raises(ValueError, match="早于 2020-01-01"):
            Kline.from_rest(row, symbol, recv)

    # ── 超出未来容忍上限 ─────────────────────────────────────────────────────

    def test_liquidation_far_future_ts_raises(self):
        """event_ts 比 recv_ts 快了 > 10s，超出时钟容忍。"""
        recv = _RECV_TS
        future_ts = recv + _TS_FUTURE_TOLERANCE_MS + 1  # 10001ms 之后
        data, _ = _make_liq_raw(future_ts, recv_ts=recv)
        with pytest.raises(ValueError, match="超出时钟容忍范围"):
            Liquidation.from_raw(data, recv)

    def test_trade_far_future_ts_raises(self):
        recv = _RECV_TS
        future_ts = recv + _TS_FUTURE_TOLERANCE_MS + 1
        data, _ = _make_trade_raw(future_ts, recv_ts=recv)
        with pytest.raises(ValueError, match="超出时钟容忍范围"):
            Trade.from_raw(data, recv)

    def test_depth_far_future_ts_raises(self):
        recv = _RECV_TS
        future_ts = recv + _TS_FUTURE_TOLERANCE_MS + 1
        data, _ = _make_depth_raw(future_ts, recv_ts=recv)
        with pytest.raises(ValueError, match="超出时钟容忍范围"):
            Depth.from_raw(data, recv)

    def test_kline_ws_far_future_ts_raises(self):
        recv = _RECV_TS
        future_ts = recv + _TS_FUTURE_TOLERANCE_MS + 1
        data, _ = _make_kline_raw(future_ts, recv_ts=recv)
        with pytest.raises(ValueError, match="超出时钟容忍范围"):
            Kline.from_raw(data, recv)

    def test_kline_rest_far_future_ts_raises(self):
        recv = _RECV_TS
        future_ts = recv + _TS_FUTURE_TOLERANCE_MS + 1
        row, symbol, _ = _make_kline_rest_row(future_ts, recv_ts=recv)
        with pytest.raises(ValueError, match="超出时钟容忍范围"):
            Kline.from_rest(row, symbol, recv)

    # ── 边界：容忍上限内的未来时间戳应通过 ──────────────────────────────────

    def test_liquidation_within_tolerance_passes(self):
        recv = _RECV_TS
        ts = recv + _TS_FUTURE_TOLERANCE_MS  # 恰好 = 容忍上限，应通过
        data, _ = _make_liq_raw(ts, recv_ts=recv)
        liq = Liquidation.from_raw(data, recv)
        assert liq.event_ts == ts

    # ── 边界：恰好等于 _TS_MIN_MS 应通过 ────────────────────────────────────

    def test_liquidation_at_min_ts_passes(self):
        data, recv = _make_liq_raw(_TS_MIN_MS)
        liq = Liquidation.from_raw(data, recv)
        assert liq.event_ts == _TS_MIN_MS

    # ── 合法时间戳完整路径 ───────────────────────────────────────────────────

    def test_all_models_with_valid_ts_pass(self):
        """合法 event_ts（当前时间附近）应能正常构造所有 4 种模型。"""
        valid_ts = _RECV_TS - 1_000  # 比接收时间早 1 秒

        liq_data, recv = _make_liq_raw(valid_ts)
        liq = Liquidation.from_raw(liq_data, recv)
        assert liq.event_ts == valid_ts

        trade_data, recv = _make_trade_raw(valid_ts)
        trade = Trade.from_raw(trade_data, recv)
        assert trade.trade_ts == valid_ts

        depth_data, recv = _make_depth_raw(valid_ts)
        depth = Depth.from_raw(depth_data, recv)
        assert depth.event_ts == valid_ts

        kline_data, recv = _make_kline_raw(valid_ts)
        kline = Kline.from_raw(kline_data, recv)
        assert kline.open_ts == valid_ts

        row, symbol, recv = _make_kline_rest_row(valid_ts)
        kline_rest = Kline.from_rest(row, symbol, recv)
        assert kline_rest.open_ts == valid_ts


# ══════════════════════════════════════════════════════════════════════════════
# BUG-9：FeatureCalculator 高水位驱逐标记
# ══════════════════════════════════════════════════════════════════════════════

BASE_TS = 1_700_000_000_000  # 固定基准，避免依赖系统时钟


def _liq(event_ts: int, notional: float = 100_000.0) -> "FakeLiq":
    """构造只含 event_ts 和 notional 的简单对象，供 on_liquidation 使用。"""

    @dataclass
    class FakeLiq:
        event_ts: int
        notional: float

    return FakeLiq(event_ts=event_ts, notional=notional)


class TestBug9HighWaterMark:
    """_max_liq_ts 应单调递增，驱逐阈值不随乱序小 now_ms 倒退。"""

    def test_max_liq_ts_increases_monotonically(self):
        """每次 snapshot_at 后，_max_liq_ts >= 上次的值。"""
        calc = FeatureCalculator(liq_window_sec=5)
        ts_seq = [BASE_TS + d for d in [10_000, 8_000, 15_000, 12_000, 20_000]]

        prev_max = 0
        for ts in ts_seq:
            calc.snapshot_at(ts)
            assert calc._max_liq_ts >= prev_max, (
                f"_max_liq_ts 不应随 now_ms={ts} 减小"
            )
            prev_max = calc._max_liq_ts

    def test_out_of_order_snapshot_does_not_reduce_max_ts(self):
        """大 now_ms 之后紧接乱序小 now_ms，_max_liq_ts 不变。"""
        calc = FeatureCalculator(liq_window_sec=5)
        calc.snapshot_at(BASE_TS + 20_000)
        assert calc._max_liq_ts == BASE_TS + 20_000

        calc.snapshot_at(BASE_TS + 8_000)   # 乱序、更小
        assert calc._max_liq_ts == BASE_TS + 20_000, (
            "乱序 snapshot 不应让高水位倒退"
        )

    def test_events_not_evicted_prematurely_by_out_of_order_call(self):
        """
        大 now_ms 驱逐过期事件后，乱序小 now_ms 不应把剩余事件额外驱逐。

        场景：
          t=15000 的事件在 2*window=10000ms 高水位内，不应被驱逐。
          snapshot_at(8000) 是乱序调用。若没有高水位保护，
          evict_before = 8000-10000 = -2000，实际上不会额外驱逐（前端 ts=15000 > -2000）。
          但有高水位保护后 evict_before = 20000-10000 = 10000，同样不驱逐 ts=15000。
          后续正常 snapshot_at(17000) 应仍能看到 ts=15000 的事件。
        """
        calc = FeatureCalculator(liq_window_sec=5)  # window=5000ms

        # 喂入 ts=5000 和 ts=15000 的强平
        calc.on_liquidation(_liq(BASE_TS + 5_000,  notional=100_000))
        calc.on_liquidation(_liq(BASE_TS + 15_000, notional=200_000))

        # 大 now_ms 驱逐 ts=5000（evict_before = 20000-10000 = 10000 > 5000）
        snap_large = calc.snapshot_at(BASE_TS + 20_000)
        # current=[15000,20000]: ts=15000 in range → 200k
        assert snap_large.liq_notional_window == pytest.approx(200_000.0)

        # 乱序 now_ms：不影响 ts=15000 仍在队列
        calc.snapshot_at(BASE_TS + 8_000)

        # 后续正常调用应仍能看到 ts=15000
        snap_after = calc.snapshot_at(BASE_TS + 17_000)
        # current=[12000,17000]: ts=15000 in range → 200k
        assert snap_after.liq_notional_window == pytest.approx(200_000.0)

    def test_window_calculation_correct_with_out_of_order_events(self):
        """乱序事件的窗口统计应正确包含其时间戳范围内的事件。"""
        calc = FeatureCalculator(liq_window_sec=5)  # window=5000ms

        calc.on_liquidation(_liq(BASE_TS + 3_000,  notional=50_000))
        calc.on_liquidation(_liq(BASE_TS + 7_000,  notional=80_000))
        calc.on_liquidation(_liq(BASE_TS + 12_000, notional=120_000))

        # 以 BASE_TS + 13000 为锚点
        snap = calc.snapshot_at(BASE_TS + 13_000)
        # current=[8000, 13000]: ts=12000 → 120k
        # prev=[3000, 8000]: ts=3000 and ts=7000 → 130k
        assert snap.liq_notional_window == pytest.approx(120_000.0)
        assert snap.liq_accel_ratio == pytest.approx(120_000.0 / 130_000.0, rel=1e-3)

    def test_eviction_respects_high_water_mark_boundary(self):
        """
        高水位下，2*window 之前的事件应被驱逐；在此之内的不应被驱逐。

        window=5000ms → 2W=10000ms
        高水位=20000ms → evict_before=10000ms
        ts=9999 应被驱逐（< 10000），ts=10000 应被保留（>= 10000）。
        """
        calc = FeatureCalculator(liq_window_sec=5)

        calc.on_liquidation(_liq(BASE_TS + 9_999,  notional=50_000))
        calc.on_liquidation(_liq(BASE_TS + 10_000, notional=100_000))
        calc.on_liquidation(_liq(BASE_TS + 15_000, notional=200_000))

        # 触发高水位 20000，驱逐 ts < 10000（即 ts=9999）
        calc.snapshot_at(BASE_TS + 20_000)

        # 乱序调用不应改变驱逐结果
        calc.snapshot_at(BASE_TS + 5_000)

        # 高水位仍为 20000，后续 snapshot 应能看到 ts=10000 和 ts=15000
        snap = calc.snapshot_at(BASE_TS + 20_000)
        # current=[15000, 20000]: ts=15000 → 200k
        assert snap.liq_notional_window == pytest.approx(200_000.0)

        snap2 = calc.snapshot_at(BASE_TS + 16_000)
        # current=[11000, 16000]: ts=15000 → 200k; ts=10000 outside
        assert snap2.liq_notional_window == pytest.approx(200_000.0)

        snap3 = calc.snapshot_at(BASE_TS + 21_000)
        # current=[16000, 21000]: nothing; prev=[11000, 16000]: ts=15000 → 200k
        assert snap3.liq_notional_window == pytest.approx(0.0)


# ══════════════════════════════════════════════════════════════════════════════
# BUG-5：flush_trades 并发保护
# ══════════════════════════════════════════════════════════════════════════════


class TestBug5FlushingGuard:
    """_flushing 标志应防止 flush_trades 并发执行。"""

    def _make_writer(self):
        """构造一个带模拟连接的 DatabaseWriter。"""
        from src.storage.writer import DatabaseWriter

        conn = MagicMock()
        # 模拟 executemany 和 commit 为异步操作
        conn.executemany = AsyncMock(return_value=None)
        conn.commit      = AsyncMock(return_value=None)
        conn.execute     = AsyncMock(return_value=MagicMock(lastrowid=1))

        writer = DatabaseWriter(conn, trade_batch_size=10, trade_queue_maxlen=1000)
        return writer, conn

    @pytest.mark.asyncio
    async def test_flushing_flag_initially_false(self):
        writer, _ = self._make_writer()
        assert writer._flushing is False

    @pytest.mark.asyncio
    async def test_flushing_flag_cleared_after_empty_flush(self):
        """空队列 flush 后，_flushing 应恢复 False。"""
        writer, _ = self._make_writer()
        result = await writer.flush_trades()
        assert result == 0
        assert writer._flushing is False

    @pytest.mark.asyncio
    async def test_flushing_flag_cleared_after_successful_flush(self):
        """成功写入后，_flushing 应恢复 False。"""
        from src.gateway.models import Trade

        writer, _ = self._make_writer()
        t = Trade(
            recv_ts=_RECV_TS, trade_ts=_RECV_TS - 1000,
            symbol="BTCUSDT", price=50000.0, qty=0.001, is_buyer_maker=False,
        )
        writer.enqueue_trade(t)
        await writer.flush_trades()
        assert writer._flushing is False

    @pytest.mark.asyncio
    async def test_concurrent_flush_second_call_returns_zero(self):
        """
        第一次 flush_trades 正在执行 executemany 期间，
        第二次调用应立即返回 0（不进行重复写入）。
        """
        from src.gateway.models import Trade

        writer, conn = self._make_writer()

        # 在 executemany 期间手动设置 _flushing=True，模拟第一次 flush 正在进行
        t = Trade(
            recv_ts=_RECV_TS, trade_ts=_RECV_TS - 1000,
            symbol="BTCUSDT", price=50000.0, qty=0.001, is_buyer_maker=False,
        )
        writer.enqueue_trade(t)

        # 手动锁定
        writer._flushing = True
        result = await writer.flush_trades()
        # 应立即返回 0，不调用 executemany
        assert result == 0
        conn.executemany.assert_not_called()

        # 解锁后正常调用应成功
        writer._flushing = False
        result2 = await writer.flush_trades()
        assert result2 == 1

    @pytest.mark.asyncio
    async def test_flushing_flag_cleared_on_db_exception(self):
        """即使 executemany 抛异常（走隔离路径），_flushing 最终也应恢复 False。"""
        from src.gateway.models import Trade

        writer, conn = self._make_writer()
        # 模拟 DB 写入失败（触发隔离路径）
        conn.executemany = AsyncMock(side_effect=Exception("db error"))

        # 同时让隔离文件也失败（避免真实 IO）
        with patch.object(writer, "_write_isolation", new=AsyncMock(return_value=False)):
            t = Trade(
                recv_ts=_RECV_TS, trade_ts=_RECV_TS - 1000,
                symbol="BTCUSDT", price=50000.0, qty=0.001, is_buyer_maker=False,
            )
            writer.enqueue_trade(t)
            await writer.flush_trades()

        # _flushing 必须被 finally 块释放
        assert writer._flushing is False

    @pytest.mark.asyncio
    async def test_no_double_write_when_both_callers_race(self):
        """
        模拟两个并发 flush_trades 调用：一个在 executemany 期间 await，
        另一个同时启动。只有一次 executemany 应该被执行。
        """
        from src.gateway.models import Trade

        writer, conn = self._make_writer()

        first_flush_started = asyncio.Event()
        first_flush_proceed = asyncio.Event()

        async def slow_executemany(*args, **kwargs):
            first_flush_started.set()
            await first_flush_proceed.wait()

        conn.executemany = slow_executemany

        t = Trade(
            recv_ts=_RECV_TS, trade_ts=_RECV_TS - 1000,
            symbol="BTCUSDT", price=50000.0, qty=0.001, is_buyer_maker=False,
        )
        writer.enqueue_trade(t)

        # 第一次 flush 开始（会 await 在 slow_executemany 内）
        task1 = asyncio.create_task(writer.flush_trades())
        await first_flush_started.wait()

        # 此时第一次 flush 持有锁，第二次应立即返回 0
        result2 = await writer.flush_trades()
        assert result2 == 0, "第二次并发调用应立即返回 0"

        # 释放第一次 flush
        first_flush_proceed.set()
        result1 = await task1
        assert result1 == 1, "第一次调用应成功写入 1 条"
