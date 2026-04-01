"""
SQLite 表结构定义与数据库初始化。

原则：
  - 所有时间戳为 Unix 毫秒整数
  - WAL 模式 + synchronous=NORMAL：在安全性和写入性能之间取得平衡
  - 高频数据（trades）仅在 trade_ts 上建索引
  - 低频重要数据（liquidations）在 event_ts 和 symbol 上建索引
"""
from __future__ import annotations

import aiosqlite

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-32000;

-- ── 原始行情数据 ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_liquidations (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts   INTEGER NOT NULL,   -- 本地收到时间戳 (ms)
    event_ts  INTEGER NOT NULL,   -- 交易所事件时间戳 (ms)
    symbol    TEXT    NOT NULL,
    side      TEXT    NOT NULL,   -- SELL=多头被强平, BUY=空头被强平
    qty       REAL    NOT NULL,
    price     REAL    NOT NULL,
    notional  REAL    NOT NULL,
    status    TEXT    NOT NULL    -- FILLED | PARTIALLY_FILLED
);
CREATE INDEX IF NOT EXISTS idx_liq_event_ts ON raw_liquidations(event_ts);
CREATE INDEX IF NOT EXISTS idx_liq_symbol   ON raw_liquidations(symbol);

CREATE TABLE IF NOT EXISTS raw_trades (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts        INTEGER NOT NULL,
    trade_ts       INTEGER NOT NULL,
    symbol         TEXT    NOT NULL,
    price          REAL    NOT NULL,
    qty            REAL    NOT NULL,
    is_buyer_maker INTEGER NOT NULL,  -- 1=主动卖, 0=主动买
    agg_id         INTEGER NOT NULL DEFAULT 0  -- aggTrade 序列号，严格递增，用于漏采检测
);
CREATE INDEX IF NOT EXISTS idx_trade_ts        ON raw_trades(trade_ts);
CREATE INDEX IF NOT EXISTS idx_trade_symbol_ts ON raw_trades(symbol, trade_ts);  -- episode outcome 查询复合索引

CREATE TABLE IF NOT EXISTS raw_klines (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    open_ts   INTEGER NOT NULL,   -- K线开盘时间 (ms)
    close_ts  INTEGER NOT NULL,   -- K线收盘时间 (ms)
    symbol    TEXT    NOT NULL,
    open      REAL    NOT NULL,
    high      REAL    NOT NULL,
    low       REAL    NOT NULL,
    close     REAL    NOT NULL,
    volume    REAL    NOT NULL,
    is_closed INTEGER NOT NULL,   -- 1=已收盘, 0=进行中
    recv_ts   INTEGER NOT NULL,
    UNIQUE(symbol, open_ts)       -- 同一K线更新时 UPSERT
);

-- ── 信号与订单（Observe/Shadow/Live 模式共用） ─────────────────────────────────

CREATE TABLE IF NOT EXISTS signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  INTEGER NOT NULL,
    symbol              TEXT    NOT NULL,
    liq_notional_window REAL,   -- 窗口内强平总名义价值 (USDT)
    liq_accel_ratio     REAL,   -- 加速率：当前窗口 / 前窗口
    vwap_15m            REAL,   -- 15分钟 VWAP
    mid_price           REAL,   -- 当前盘口中间价
    deviation_bps       REAL,   -- (mid_price - vwap) / vwap * 10000
    signal_fired        INTEGER DEFAULT 0,  -- 1=信号触发
    entry_price         REAL,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS shadow_orders (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         INTEGER NOT NULL,
    signal_id  INTEGER REFERENCES signals(id),
    symbol     TEXT    NOT NULL,
    side       TEXT    NOT NULL,
    qty        REAL    NOT NULL,
    order_type TEXT    NOT NULL,  -- LIMIT | MARKET
    price      REAL,
    status     TEXT    NOT NULL,  -- PENDING | FILLED | CANCELLED | EXPIRED
    fill_price REAL,
    fill_ts    INTEGER,
    pnl        REAL,
    notes      TEXT
);

CREATE TABLE IF NOT EXISTS live_orders (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               INTEGER NOT NULL,
    signal_id        INTEGER REFERENCES signals(id),
    exchange_id      TEXT,        -- 交易所订单 ID
    symbol           TEXT    NOT NULL,
    side             TEXT    NOT NULL,
    qty              REAL    NOT NULL,
    order_type       TEXT    NOT NULL,
    price            REAL,
    status           TEXT    NOT NULL,
    fill_price       REAL,
    fill_qty         REAL,
    fill_ts          INTEGER,
    commission       REAL,
    commission_asset TEXT,
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS fills (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            INTEGER NOT NULL,
    order_id      INTEGER,
    symbol        TEXT    NOT NULL,
    side          TEXT    NOT NULL,
    price         REAL    NOT NULL,
    qty           REAL    NOT NULL,
    commission    REAL,
    realized_pnl  REAL
);

CREATE TABLE IF NOT EXISTS positions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             INTEGER NOT NULL,
    symbol         TEXT    NOT NULL,
    side           TEXT    NOT NULL,
    qty            REAL    NOT NULL,
    entry_price    REAL    NOT NULL,
    unrealized_pnl REAL,
    status         TEXT    NOT NULL  -- OPEN | CLOSED
);

-- ── Episode 研究样本（阶段 3：一次连续冲击 = 一条研究样本）──────────────────────────

CREATE TABLE IF NOT EXISTS liquidation_episodes (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id           TEXT    NOT NULL UNIQUE,  -- "{symbol}_{start_event_ts}"
    symbol               TEXT    NOT NULL,
    side                 TEXT    NOT NULL,          -- SELL | BUY
    start_event_ts       INTEGER NOT NULL,          -- 首笔强平的交易所时间戳 (ms)
    end_event_ts         INTEGER NOT NULL,          -- 末笔强平的交易所时间戳 (ms)
    duration_ms          INTEGER NOT NULL,          -- end - start (ms)
    liq_count            INTEGER NOT NULL,          -- episode 内强平笔数
    liq_notional_total   REAL    NOT NULL,          -- 所有强平名义价值之和 (USDT)
    liq_peak_window      REAL    NOT NULL,          -- episode 期间最高滚动窗口值 (USDT)
    liq_accel_ratio_peak REAL,                      -- 最高加速率（NULL = 无数据）
    min_mid_price        REAL,                      -- 盘口中间价最低值（最深跌点）
    pre_event_vwap       REAL,                      -- 首笔强平时的 VWAP（事前基准）
    max_deviation_bps    REAL,                      -- 最大负偏离 bps（最小值，通常为负）
    created_at           INTEGER NOT NULL           -- episode 关闭时的本地时间戳 (ms)
);
CREATE INDEX IF NOT EXISTS idx_episodes_start  ON liquidation_episodes(start_event_ts);
CREATE INDEX IF NOT EXISTS idx_episodes_symbol ON liquidation_episodes(symbol);

-- ── Episode Outcome（阶段 4：episode 结束后 15 分钟的修复机制观测）──────────────────

CREATE TABLE IF NOT EXISTS episode_outcomes (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id           TEXT    NOT NULL UNIQUE REFERENCES liquidation_episodes(episode_id),
    entry_price          REAL,            -- 理论入场价 = episode.min_mid_price（episode 期间最低价）
    price_at_episode_end REAL,            -- episode 结束后首笔成交价（可下单的最早真实价格）
    price_at_1m          REAL,            -- episode 结束后 1 分钟的价格快照
    price_at_5m          REAL,            -- episode 结束后 5 分钟的价格快照
    price_at_15m         REAL,            -- episode 结束后 15 分钟的价格快照
    min_price_0_5m       REAL,            -- 0-5 分钟最低价（MAE 来源）
    max_price_0_5m       REAL,            -- 0-5 分钟最高价（MFE 来源）
    min_price_0_15m      REAL,            -- 0-15 分钟最低价
    max_price_0_15m      REAL,            -- 0-15 分钟最高价
    mae_bps              REAL,            -- 最大不利偏移 bps（负值 = 入场后价格进一步下跌）
    mfe_bps              REAL,            -- 最大有利偏移 bps（正值 = 入场后价格上涨空间）
    rebound_to_vwap_ms   INTEGER,         -- 价格首次 >= pre_event_vwap 的延迟 (ms)；NULL=未反弹
    rebound_depth_bps    REAL,            -- 从 entry 到 pre_event_vwap 的距离 (bps)
    trade_count_0_15m    INTEGER NOT NULL DEFAULT 0,  -- 15 分钟内成交笔数（数据密度参考）
    computed_at          INTEGER NOT NULL  -- outcome 计算时的本地时间戳 (ms)
);
CREATE INDEX IF NOT EXISTS idx_outcomes_episode ON episode_outcomes(episode_id);

-- ── 盘口深度快照（1 秒降采样，用于研究 episode 期间流动性真空深度与补单速度）──────────

CREATE TABLE IF NOT EXISTS raw_depth_snapshots (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts   INTEGER NOT NULL,
    event_ts  INTEGER NOT NULL,
    symbol    TEXT    NOT NULL,
    bids_json TEXT    NOT NULL,  -- JSON [[price, qty], ...] top 20 买档，高→低
    asks_json TEXT    NOT NULL   -- JSON [[price, qty], ...] top 20 卖档，低→高
);
CREATE INDEX IF NOT EXISTS idx_depth_symbol_ts ON raw_depth_snapshots(symbol, event_ts);

-- ── 风控与运维 ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS risk_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         INTEGER NOT NULL,
    component  TEXT    NOT NULL,
    event_type TEXT    NOT NULL,  -- HALT | COOLDOWN | LIMIT_BREACH | STALE_SIGNAL
    severity   TEXT    NOT NULL,  -- INFO | WARN | ERROR | CRITICAL
    message    TEXT,
    context    TEXT              -- JSON 扩展字段
);

CREATE TABLE IF NOT EXISTS service_heartbeats (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    component   TEXT    NOT NULL,
    status      TEXT    NOT NULL,  -- OK | DEGRADED | ERROR
    msg_total   INTEGER,
    liq_count   INTEGER,
    trade_count INTEGER,
    latency_p50 REAL,
    latency_p95 REAL,
    latency_p99 REAL,
    reconnects  INTEGER,
    notes       TEXT
);
"""


async def init_db(db_path: str) -> aiosqlite.Connection:
    """
    创建或打开 SQLite 数据库，执行所有 DDL，返回连接对象。

    在 DDL 之后自动执行增量迁移（幂等），无需手动操作即可升级已有数据库。
    调用方负责在程序退出时关闭连接。
    """
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.executescript(_DDL)
    await conn.commit()

    # ── 增量迁移（幂等，每次启动自动检查） ────────────────────────────────────
    # v3: 为 raw_trades 增加 agg_id 列（若已存在则跳过）。
    #     历史行保持 DEFAULT 0，不回填（历史 agg_id 已不可追溯）。
    cursor = await conn.execute("PRAGMA table_info(raw_trades)")
    trade_cols = {row[1] async for row in cursor}
    if "agg_id" not in trade_cols:
        await conn.execute(
            "ALTER TABLE raw_trades ADD COLUMN agg_id INTEGER NOT NULL DEFAULT 0"
        )
        await conn.commit()

    # v2: 为 episode_outcomes 增加 price_at_episode_end 列（若已存在则跳过），
    #     并回填历史 NULL 值（从 raw_trades 取 end_event_ts 之后首笔成交价）。
    cursor = await conn.execute("PRAGMA table_info(episode_outcomes)")
    existing_cols = {row[1] async for row in cursor}
    if "price_at_episode_end" not in existing_cols:
        await conn.execute(
            "ALTER TABLE episode_outcomes ADD COLUMN price_at_episode_end REAL"
        )
        await conn.commit()
        # 回填历史行（仅在首次迁移时运行，避免每次启动都扫全表）。
        # 上界限制为 end_event_ts + 15 分钟，与 get_trades_after_episode 的观测窗口一致，
        # 防止"窗口内无成交但窗口外数小时后有成交"时回填值与实时计算值（None）不一致。
        await conn.execute(
            """
            UPDATE episode_outcomes
            SET    price_at_episode_end = (
                       SELECT t.price
                       FROM   raw_trades t
                       JOIN   liquidation_episodes e
                              ON e.episode_id = episode_outcomes.episode_id
                       WHERE  t.symbol   = e.symbol
                         AND  t.trade_ts >  e.end_event_ts
                         AND  t.trade_ts <= e.end_event_ts + 900000
                       ORDER  BY t.trade_ts, t.rowid
                       LIMIT  1
                   )
            WHERE  price_at_episode_end IS NULL
            """
        )
        await conn.commit()

    # ── v4: 微观结构重构 — 新增列（幂等） ──────────────────────────────────────
    # liquidation_episodes: pre_event_index_price, max_basis_bps, max_impact_ratio
    cursor = await conn.execute("PRAGMA table_info(liquidation_episodes)")
    ep_cols = {row[1] async for row in cursor}

    for col, col_def in [
        ("pre_event_index_price", "REAL"),
        ("max_basis_bps", "REAL"),
        ("max_impact_ratio", "REAL"),
    ]:
        if col not in ep_cols:
            await conn.execute(
                f"ALTER TABLE liquidation_episodes ADD COLUMN {col} {col_def}"
            )
            await conn.commit()

    # episode_outcomes: rebound_to_basis_zero_ms, basis_rebound_depth
    cursor = await conn.execute("PRAGMA table_info(episode_outcomes)")
    oc_cols = {row[1] async for row in cursor}

    for col, col_def in [
        ("rebound_to_basis_zero_ms", "INTEGER"),
        ("basis_rebound_depth", "REAL"),
    ]:
        if col not in oc_cols:
            await conn.execute(
                f"ALTER TABLE episode_outcomes ADD COLUMN {col} {col_def}"
            )
            await conn.commit()

    # signals: index_price, basis_bps, bid_depth_usdt, impact_ratio
    cursor = await conn.execute("PRAGMA table_info(signals)")
    sig_cols = {row[1] async for row in cursor}

    for col, col_def in [
        ("index_price", "REAL"),
        ("basis_bps", "REAL"),
        ("bid_depth_usdt", "REAL"),
        ("impact_ratio", "REAL"),
    ]:
        if col not in sig_cols:
            await conn.execute(
                f"ALTER TABLE signals ADD COLUMN {col} {col_def}"
            )
            await conn.commit()

    return conn

