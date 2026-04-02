# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LMR-Hunter is an automated cryptocurrency trading system for BTC perpetual futures. It detects short-term liquidity vacuums caused by forced liquidations and participates in price recovery through passive limit orders.

**Current Stage**: Observe Mode (data collection and research, no live trading)

**Core Principle**: Don't predict trends, only respond to anomaly events (forced liquidations) with quantifiable edge.

## Development Commands

### Setup and Installation
```bash
# Install development dependencies (includes pytest and editable install)
python -m pip install -r requirements-dev.txt

# Production runtime dependencies only
python -m pip install -r requirements.txt
```

### Testing
```bash
# Run all tests (quick mode)
python -m pytest -q

# Run specific test file
python -m pytest tests/test_episode_model.py -v

# Run tests matching pattern
python -m pytest -k "test_episode" -v
```

### Running the System
```bash
# Main entry point (requires config/.env)
python -m src.main

# On production VPS (systemd managed)
sudo systemctl start lmr-hunter
sudo systemctl status lmr-hunter
sudo journalctl -u lmr-hunter -f
```

### Database Operations
```bash
# Database location configured in .env
# Default: /opt/lmr-hunter/data/lmr.db (production)
# or ./data/lmr.db (local development)

# Inspect database
sqlite3 data/lmr.db
```

## High-Level Architecture

### Data Flow (Current: Observe Mode)

```
Binance WebSocket → BinanceGateway
                      ├─ on_liquidation → FeatureCalculator → EpisodeBuilder → DatabaseWriter
                      ├─ on_trade       → FeatureCalculator (order flow) + DatabaseWriter (batched)
                      ├─ on_depth       → FeatureCalculator (mid_price + bid_depth)
                      ├─ on_kline       → FeatureCalculator + DatabaseWriter
                      └─ on_mark_price  → FeatureCalculator (index_price for Basis)

Background Tasks:
  - trade_flusher: Flushes trade queue every 1s
  - stats_reporter: Prints stats + writes heartbeat every 60s
  - outcome_processor: Backfills episode outcomes every 60s
```

### Module Responsibilities

**Gateway (`src/gateway/`)**: Establishes WebSocket connections, normalizes market data (liquidations, trades, depth, klines), broadcasts events to downstream modules.

**Feature Engine (`src/features/`)**:
- `calculator.py`: Computes basis_bps (perp vs index), impact_ratio (liq vs bid depth), taker_buy_ratio (order flow), sliding window liquidation aggregation. [DEPRECATED: VWAP_15M retained for backward compatibility only]
- `episode.py`: Aggregates continuous liquidations into research samples (episodes)
- `outcome.py`: Computes post-episode price recovery metrics (MAE/MFE/basis rebound)

**Storage (`src/storage/`)**:
- `writer.py`: Handles batched writes with retry logic and integrity monitoring (DEGRADED state detection)
- `schema.py`: SQLite schema with WAL mode for safe concurrent writes

**Monitor (`src/monitor/`)**: Health checks, Telegram alerts, latency monitoring

**Analysis (`src/analysis/`)**: Research reports and daily statistics (not part of runtime loop)

### Critical Design Patterns

**Time Semantics**: All features must align on `event_ts` (exchange time), not `recv_ts` (local receipt time). This prevents hindsight bias in research. Latency measurement uses `recv_ts - event_ts`.

**Episode Model**: Individual liquidation events are aggregated into "episodes" (continuous liquidation clusters). Research samples, outcome analysis, and tradability evaluation all operate at episode granularity, not individual liquidation events.

**Integrity State Machine**: The system tracks data integrity through states:
- `OK`: No sample corruption detected
- `DEGRADED`: Real sample damage occurred (overflow, isolation, permanent loss). This run cannot be used as trusted research baseline.

**Batched Trade Writes**: Trades arrive at ~100-1000/sec. They are queued in memory and flushed in batches (default: 2000 trades/batch, 1s interval) to avoid blocking the event loop.

**Graceful Shutdown**: On SIGINT/SIGTERM:
1. Stop gateway (no new data)
2. Cancel background tasks
3. Flush active episode
4. Drain trade queue
5. Close database connection

### Configuration Philosophy

All parameters are configurable via environment variables (see `config/.env.example`):
- `EPISODE_GAP_SEC=30`: Max gap between liquidations in same episode
- `EPISODE_NOISE_THRESHOLD_USDT=50000`: Min notional to count toward episode
- `LIQ_WINDOW_SEC=5`: Sliding window for liquidation aggregation
- `TRADE_BATCH_SIZE=2000`: Trades per batch write
- `OUTCOME_WINDOW_MS=900000`: Post-episode observation window (15 min)

**Parameter tuning is deferred to Shadow Mode**. Current values are conservative starting points to enable sample collection.

### Database Schema Philosophy

Two types of timestamps in every raw data table:
- `event_ts`: Exchange event time (primary for feature calculation)
- `recv_ts`: Local receipt time (for latency analysis)

**Never compute features using only `recv_ts`** - this causes time misalignment and invalidates research.

Key tables:
- `raw_liquidations`: Individual liquidation events
- `raw_trades`: Market trades (batched writes)
- `raw_klines`: 1-minute klines (retained for reference, no longer primary signal source)
- `raw_depth_snapshots`: Orderbook depth snapshots (1s downsampled)
- `liquidation_episodes`: Aggregated liquidation clusters (research samples)
- `episode_outcomes`: Post-episode price recovery metrics (backfilled, includes basis rebound)
- `signals`: Strategy signal triggers with basis_bps, impact_ratio, index_price
- `service_heartbeats`: System health monitoring

### Low-Latency Strategy Context

**Deployment**: Production runs on Tokyo VPS (AWS Lightsail) for ~2-3ms latency to Binance (vs ~300ms from local network). This 100x latency reduction enables:
- Reactive order placement (detect signal → place order in <50ms)
- Precise stop-loss execution (~0.05% slippage vs ~0.3%)
- Multi-level position building (3 orders in <50ms)
- Multi-symbol monitoring (20+ symbols vs 3-5)

**Latency Budget**:
- WS receive: ~2-3ms
- Feature calc + risk check: ~10-50ms
- Order placement: ~2-3ms
- **Total**: ~14-103ms (enables reactive strategies impossible at 300ms+)

### State Management Philosophy

**Run Quality**: Not all runs are trustworthy. A run is "trusted" only if:
- No silent sample loss occurred
- Episode definition remained stable
- Outcome computation completed for all eligible episodes

**DEGRADED State**: When `overflow_dropped`, `isolated`, or `lost` counters are non-zero, the system enters DEGRADED state. This run's data cannot be used for parameter calibration or strategy validation.

### Async/Concurrency Model

- Uses `uvloop` for performance (imported in `main.py`)
- All I/O is async (aiosqlite, aiohttp, websockets)
- Background tasks run concurrently with main event loop
- Trade queue is bounded (`TRADE_QUEUE_MAXLEN=50000`) to prevent memory overflow

## Project Stages (Sequential)

0. ✅ **Tokyo VPS deployment** - Validated 2.18ms Binance latency
1. ✅ **Observe Mode** - Currently running, collecting episode data
2. **Shadow Mode** - Virtual order simulation with latency penalty
3. **Testnet Mode** - Full execution chain on test network
4. **Live Mode** - Minimum position size on real capital

**DO NOT implement live trading logic until stages 1-3 are complete and validated.**

## Important Constraints

### MVP Scope
- Market: Binance Futures only
- Symbol: BTCUSDT perpetual only
- Direction: Long-only (buy dips after liquidation cascades)
- Entry: Passive limit orders (Post-Only)
- Stop-loss: Active market exit allowed (risk control priority)

### Frozen Decisions
- Fair value anchor: **Index Price** (Binance spot index via markPrice@1s stream)
- Edge metric: **Basis_Bps** = (perp_mid - index) / index × 10000
- [DEPRECATED] VWAP_15M retained in code for backward compat, not used for signals
- Single target take-profit (no partial exits in MVP)
- SQLite for persistence (no PostgreSQL until proven necessary)
- Python 3.12+ (not Rust/Go until latency becomes bottleneck)

### Signal Structure (4 Layers)
1. **Event Layer**: Abnormal liquidation detected? (liq_notional_window > threshold)
2. **Liquidity Layer**: Impact Ratio confirms real liquidity vacuum? (liq / bid_depth > threshold)
3. **Basis Layer**: Negative basis wide enough to cover friction? (basis_bps < -threshold)
4. **Micro-Decay Layer**: Taker Buy confirms repair capital entering? (taker_buy_ratio rising + accel_ratio < 1.0)

All 4 layers are hard gates. Risk checks (position limits, drawdown) are pre-trade constraints, not a separate signal layer.

### Data Integrity Principles
- Trades are batched (high volume) but liquidations/klines are written immediately (low volume, high priority)
- Retry logic with exponential backoff on write failures
- Isolated file fallback if database becomes unavailable
- All integrity violations increment counters tracked in heartbeat `notes` field
- If `overflow_dropped + isolated + lost > 0`, research validity is compromised

### Testing Philosophy
Tests validate:
- Time semantics correctness (`test_time_semantics.py`)
- Episode aggregation logic (`test_episode_model.py`)
- Outcome computation (`test_episode_outcomes.py`)
- Writer integrity under load (`test_writer_integrity.py`)
- Data loss detection (`test_bug_fixes.py`)

Tests use in-memory SQLite (`:memory:`) for speed.

## Key Documentation

**Read these docs before making architectural changes:**
- `docs/strategy.md` - Strategy logic, signal structure, parameter definitions
- `docs/architecture.md` - Module boundaries, data flow, state machines
- `docs/glossary.md` - Time semantics, run quality definitions
- `docs/risk-controls.md` - Risk gates and failure modes
- `docs/status.md` - Current blockers and next actions

## Common Pitfalls to Avoid

1. **Don't use `recv_ts` for feature calculation** - always use `event_ts` from exchange
2. **Don't write trades one-by-one** - they must be batched (100-1000/sec volume)
3. **Don't ignore DEGRADED state** - it means research data is corrupted
4. **Don't add trading logic in Observe Mode** - this stage is research-only
5. **Don't assume all episodes have outcomes** - outcomes are backfilled asynchronously
6. **Don't hard-code parameters** - everything must be configurable via environment variables
7. **Don't create empty commits** - check for actual changes before committing
8. **Don't optimize prematurely** - strategy validation comes before performance tuning

## Deployment Context

Production VPS setup:
- Location: Tokyo (AWS Lightsail)
- OS: Ubuntu 24.04 LTS
- Python: 3.12+
- Process manager: systemd
- Data directory: `/opt/lmr-hunter/data/`
- Config: `/opt/lmr-hunter/config/.env`

System has been running continuously since 2026-03-27, collecting real liquidation data.
