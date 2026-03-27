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
    is_buyer_maker INTEGER NOT NULL  -- 1=主动卖, 0=主动买
);
CREATE INDEX IF NOT EXISTS idx_trade_ts ON raw_trades(trade_ts);

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

    调用方负责在程序退出时关闭连接。
    """
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.executescript(_DDL)
    await conn.commit()
    return conn
