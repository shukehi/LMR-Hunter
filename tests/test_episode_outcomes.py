"""
阶段 4 验收测试：Episode Outcome 计算。

验收标准（来自 optimization-plan.md 阶段 4）：
  - 每个有效 episode 都能生成 outcome 记录
  - 能回答：反弹是否发生 / 延迟多久 / 幅度多大
  - MAE / MFE 方向正确，从理论入场价出发
  - None entry 或 None vwap 不应导致崩溃
"""
import pytest
from src.features.outcome import compute_outcome, EpisodeOutcome

BASE_TS = 1_700_000_000_000   # 固定基准时间戳 (ms)


def _trades(offsets_and_prices: list[tuple[int, float]]) -> list[tuple[int, float]]:
    """辅助：[(offset_ms, price), ...] → [(trade_ts, price), ...]"""
    return [(BASE_TS + off, price) for off, price in offsets_and_prices]


# ── 基础行为 ───────────────────────────────────────────────────────────────────

def test_empty_trades_all_none():
    """15 分钟内无成交 → 所有价格字段为 None，count=0，不崩溃。"""
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, [])
    assert oc.price_at_1m  is None
    assert oc.price_at_5m  is None
    assert oc.price_at_15m is None
    assert oc.mae_bps      is None
    assert oc.mfe_bps      is None
    assert oc.rebound_to_vwap_ms is None
    assert oc.trade_count_0_15m  == 0


def test_trade_count_matches_input():
    """trade_count_0_15m 等于输入成交数。"""
    trades = _trades([(i * 10_000, 85_000.0 + i) for i in range(7)])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, trades)
    assert oc.trade_count_0_15m == 7


# ── 价格快照 ───────────────────────────────────────────────────────────────────

def test_price_at_1m_is_last_trade_within_60s():
    """price_at_1m = 1 分钟（60s）以内最后一笔成交价。"""
    trades = _trades([
        (30_000, 85_100.0),   # +30s ✓
        (59_000, 85_200.0),   # +59s ✓ 最后一笔 ≤ 1m
        (65_000, 85_300.0),   # +65s > 1m，不计入 1m 快照
    ])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, trades)
    assert oc.price_at_1m == pytest.approx(85_200.0)
    assert oc.price_at_5m == pytest.approx(85_300.0)   # 65s < 5m，计入 5m 快照


def test_price_at_15m_is_last_trade_in_window():
    """price_at_15m = 15 分钟窗口内最后一笔成交价。"""
    trades = _trades([
        (60_000,  85_200.0),
        (600_000, 85_800.0),
        (899_000, 86_100.0),   # 最后一笔 < 15min (899s < 900s)
    ])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, trades)
    assert oc.price_at_15m == pytest.approx(86_100.0)


# ── MAE / MFE ─────────────────────────────────────────────────────────────────

def test_mae_mfe_from_entry_price():
    """
    MAE/MFE 从 entry_price 出发，基于 0-5 分钟极值计算。

    最低价 84_150 → MAE = (84150 - 85000) / 85000 × 10000 = -100.0 bps
    最高价 85_850 → MFE = (85850 - 85000) / 85000 × 10000 = +100.0 bps
    """
    entry = 85_000.0
    trades = _trades([
        (60_000,  84_150.0),   # 最低 → MAE
        (120_000, 85_850.0),   # 最高 → MFE
        (180_000, 85_500.0),
    ])
    oc = compute_outcome("EP_1", BASE_TS, entry, 86_000.0, trades)
    assert oc.mae_bps == pytest.approx(-100.0)
    assert oc.mfe_bps == pytest.approx(+100.0)


def test_mae_negative_mfe_positive():
    """MAE 始终为非正，MFE 始终为非负（相对于 entry_price）。"""
    entry = 85_000.0
    trades = _trades([(60_000, 84_000.0), (120_000, 86_000.0)])
    oc = compute_outcome("EP_1", BASE_TS, entry, 87_000.0, trades)
    assert oc.mae_bps is not None and oc.mae_bps <= 0
    assert oc.mfe_bps is not None and oc.mfe_bps >= 0


def test_none_entry_skips_bps():
    """entry_price = None 时，MAE/MFE/rebound_depth 均为 None，不崩溃。"""
    trades = _trades([(60_000, 85_500.0)])
    oc = compute_outcome("EP_1", BASE_TS, None, 86_000.0, trades)
    assert oc.mae_bps          is None
    assert oc.mfe_bps          is None
    assert oc.rebound_depth_bps is None


# ── 反弹检测 ───────────────────────────────────────────────────────────────────

def test_rebound_to_vwap_detected():
    """价格首次 >= pre_event_vwap 时记录延迟（以 end_event_ts 为起点）。"""
    vwap = 86_000.0
    trades = _trades([
        (60_000,  85_200.0),   # < vwap
        (120_000, 85_800.0),   # < vwap
        (180_000, 86_050.0),   # >= vwap → rebound！
        (240_000, 86_200.0),
    ])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, vwap, trades)
    assert oc.rebound_to_vwap_ms == 180_000


def test_rebound_exactly_at_vwap():
    """价格恰好等于 pre_event_vwap 也算反弹（>= 而非 >）。"""
    vwap = 86_000.0
    trades = _trades([(90_000, 86_000.0)])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, vwap, trades)
    assert oc.rebound_to_vwap_ms == 90_000


def test_no_rebound_returns_none():
    """价格在 15 分钟内始终未达到 pre_event_vwap → rebound_to_vwap_ms = None。"""
    trades = _trades([(60_000, 85_200.0), (300_000, 85_500.0)])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, trades)
    assert oc.rebound_to_vwap_ms is None


def test_none_vwap_skips_rebound():
    """pre_event_vwap = None 时，rebound 字段均为 None，不崩溃。"""
    trades = _trades([(60_000, 87_000.0)])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, None, trades)
    assert oc.rebound_to_vwap_ms  is None
    assert oc.rebound_depth_bps   is None


def test_rebound_depth_bps_calculation():
    """
    rebound_depth_bps = (pre_event_vwap - entry) / entry × 10000。

    例：entry=85000, vwap=86000 → (86000-85000)/85000 × 10000 ≈ 117.6 bps
    """
    entry = 85_000.0
    vwap  = 86_000.0
    trades = _trades([(60_000, 85_500.0)])
    oc = compute_outcome("EP_1", BASE_TS, entry, vwap, trades)
    expected = (vwap - entry) / entry * 10_000
    assert oc.rebound_depth_bps == pytest.approx(expected, abs=0.1)  # round(, 1)


# ── price_at_episode_end ───────────────────────────────────────────────────────

def test_price_at_episode_end_is_first_trade():
    """price_at_episode_end = 第一笔成交价（episode 结束后最早可下单的市场价格）。"""
    trades = _trades([
        (1_000,   84_500.0),   # 最早一笔 → price_at_episode_end
        (30_000,  84_800.0),
        (120_000, 85_200.0),
    ])
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, trades)
    assert oc.price_at_episode_end == pytest.approx(84_500.0)


def test_price_at_episode_end_none_when_no_trades():
    """15 分钟内无成交 → price_at_episode_end 为 None。"""
    oc = compute_outcome("EP_1", BASE_TS, 85_000.0, 86_000.0, [])
    assert oc.price_at_episode_end is None


def test_price_at_episode_end_independent_of_entry_price():
    """price_at_episode_end 与 entry_price 无关，entry=None 时仍可计算。"""
    trades = _trades([(500, 83_000.0)])
    oc = compute_outcome("EP_1", BASE_TS, None, 86_000.0, trades)
    assert oc.price_at_episode_end == pytest.approx(83_000.0)
    assert oc.mae_bps is None   # entry=None 时 MAE 不可计算


# ── get_trades_after_episode 边界测试 ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_trades_after_episode_excludes_episode_end_ts():
    """
    关键边界测试：trade_ts == end_event_ts 的成交不应被 get_trades_after_episode 返回。

    旧代码使用 BETWEEN（含起点），会把 episode 最后一笔成交误认为"episode 结束后首笔"，
    导致 price_at_episode_end 偏低（仍是下跌期的价格）。
    新代码使用 trade_ts > start_ts（严格排他），与 schema 回填逻辑一致。
    """
    import aiosqlite
    END_TS = BASE_TS + 5_000

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript("""
            CREATE TABLE raw_trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                recv_ts        INTEGER NOT NULL,
                trade_ts       INTEGER NOT NULL,
                symbol         TEXT    NOT NULL,
                price          REAL    NOT NULL,
                qty            REAL    NOT NULL,
                is_buyer_maker INTEGER NOT NULL,
                agg_id         INTEGER NOT NULL DEFAULT 0
            );
        """)

        # 在 episode 期间（含末尾时刻）的成交 — 不应被返回
        await conn.execute(
            "INSERT INTO raw_trades (recv_ts, trade_ts, symbol, price, qty, is_buyer_maker) "
            "VALUES (?, ?, 'BTCUSDT', 83000.0, 0.1, 0)",
            (END_TS, END_TS),
        )
        # episode 结束后的第一笔真实成交 — 应被返回且作为 price_at_episode_end
        await conn.execute(
            "INSERT INTO raw_trades (recv_ts, trade_ts, symbol, price, qty, is_buyer_maker) "
            "VALUES (?, ?, 'BTCUSDT', 84000.0, 0.1, 0)",
            (END_TS + 1, END_TS + 1),
        )
        await conn.commit()

        from src.storage.writer import DatabaseWriter
        writer = DatabaseWriter(conn)
        trades = await writer.get_trades_after_episode(
            symbol   = "BTCUSDT",
            start_ts = END_TS,
            end_ts   = END_TS + 900_000,
        )

    assert len(trades) == 1, "episode 末尾时刻的成交不应包含在内"
    assert trades[0][1] == pytest.approx(84_000.0), "第一笔应为 episode 结束后的成交"
