# lighter_MM_RUST

A high-performance **Rust** port of the Python `lighter_MM` market maker for the Lighter
(zkLighter) perpetuals exchange, with a clean **hot-path / cold-path** split and Rust-specific
optimizations (lock-free cache-aligned atomics, Welford ring buffers, single-writer hot path,
freshest-wins mailbox, FFI signing off the hot path).

Strategy: **volatility + order-book-imbalance (OBI) alpha**, with an external Binance OBI feed —
the same model as the live Python bot and the `standx` reference (and the hftbacktest
"Market Making with Alpha — OBI" tutorial). There is **no Cartea-Jaimungal** (that code is
deprecated/unused in the Python bot and is intentionally not ported).

See `PLAN.md` for the full design, the GPT-5.5 plan review, and module specs.

## Architecture

```
 Binance @depth@100ms ─► depth_client ─┐ atomic store
 Binance @bookTicker  ─► book_ticker  ─┤
                                       ▼
                           SharedAlpha / SharedBbo (cache-line aligned AtomicU64)   COLD (tokio)
                                       ▲ ~1ns lock-free read
 Lighter WS /stream ─► market-data task (HOT, single writer, synchronous) ──────────────┐
   order_book/{m}      • parse → LocalBook (sorted Vec, snapshot/delta, offset guard)    │
   ticker/{m}          • mid → VolObiCalculator (Welford vol + OBI z-score)              │
                       • read SharedAlpha/Position/Derived (atomics)                     │
                       • quote ladder → quality/inventory bias → collect ops             │
                       • watch::send(ops)  ───────────► freshest-wins mailbox            │
 paced_send ◄──────────────────────────────────────── watch::Receiver                   │
   • rate-limit gate (40/60s window + quota pacing + 429 backoff)                        │
   • sign batch via native signer FFI (spawn_blocking)                                   │
   • send TxWebSocket (REST free-slot fallback); Unknown ⇒ pause+refresh+reconcile       │
 account WS / reconcile / risk ──► OrderEvent (lossless) + reconcile snapshot ──────────►┘
```

The synchronous hot path (WS callback) owns `LocalBook` + `VolObiCalculator` + `OrderManager`
outright (no locks); cross-task signals are read through lock-free atomics; the only output is a
single-slot `watch` mailbox. All I/O (signing, sending, account, reconcile) is on the async cold path.

## Module map (`src/`)

| Module | Role | Status |
|---|---|---|
| `lighter/signer.rs` | FFI into the official native signer `.so` (`libloading`) | ✅ verified byte-parity to Python |
| `lighter/{rest,nonce,ws,tx_ws,auth,messages}.rs` | REST, optimistic nonce, WS subscribe, tx WebSocket, auth token, payloads | ✅ |
| `strategy/{rolling,vol_obi,quotes}.rs` | Welford rolling stats, vol/OBI engine, quote ladder | ✅ vol/OBI verified to 1e-17 vs Cython |
| `book/local_book.rs` | CBookSide-equivalent sorted book | ✅ |
| `binance/{obi,depth_client,book_ticker}.rs` | Binance OBI alpha + BBO feeds | ✅ |
| `shared.rs` | lock-free `SharedAlpha`/`SharedBbo`/`SharedPosition`/`Derived` | ✅ |
| `exec/{rate_limit,order_manager,collect,signing,paced_send}.rs` | rate limiter, order lifecycle, op collection, sign bridge, sender | ✅ |
| `account/{fill_accounting,persistence}.rs` | VWAP/PnL, live-state JSON | ✅ |
| `metrics/{trade_log,live_metrics}.rs` | buffered CSV, markout quality adjustment | ✅ |
| `risk.rs` | circuit breaker / pause | ✅ |
| `orchestrator.rs` | bootstrap + hot task + feeds; shadow mode + live wiring | ✅ shadow; ⏳ live account-WS wiring |
| `config.rs`, `types.rs`, `util.rs`, `logging.rs` | config, core types, numeric helpers (banker's rounding), logging | ✅ |

## Build & run

```bash
cargo build --release            # optimized (LTO, panic=abort)
cargo test                       # 89 unit tests incl. parity tests

# Verify the native signer FFI matches the Python SDK (offline, no orders):
cargo run --bin test_sign -- /home/ubuntu/lighter_MM/.env

# Parity of the vol/OBI engine vs the live Cython engine:
cargo run --bin test_obi_parity        # compare to: python3 /home/ubuntu/lighter_MM/_obi_parity_ref.py

# SHADOW mode — full hot path against LIVE market data, NO orders sent (safe):
RUST_LOG=info cargo run --release -- --symbol BTC --shadow

# LIVE mode (gated; sends real orders — requires .env credentials):
RUST_LOG=info cargo run --release -- --symbol BTC --live
```

`config.json` mirrors the Python bot's schema. Credentials come from `.env`
(`API_KEY_PRIVATE_KEY`, `API_KEY_INDEX`, `ACCOUNT_INDEX`, `WALLET_ADDRESS`, `MARKET_SYMBOL`).
The native signer binaries live in `signers/` (copied from the Python SDK).

## What's verified

- **Signer**: Rust FFI produces byte-identical `tx_info` to the Python SDK across all controllable
  fields (`tx_type=14`, valid `Sig`). No crypto reimplemented — same official `.so`.
- **Quote math**: volatility, OBI alpha, and bid/ask match the live Cython engine to 1e-17 on a
  deterministic scenario.
- **Hot path**: shadow mode runs WS → book → signal → quote ladder → order ops on live BTC data
  with the Binance alpha feed, no errors.

## Status / remaining

- LIVE account-channel WS streaming (account_orders / account_all / user_stats) + the reconcile
  stale-poller are wired as components (`paced_send`, `tx_ws`, `signing`, `order_manager` reconcile
  all exist and are unit-tested) but their integration into `orchestrator::run` for Live mode is in
  progress. Until complete, run **shadow** mode.
- A fill-simulating dry-run engine (the Python `dry_run.py`) is not ported (use the Python one for
  backtests; the Rust bot targets live/shadow).
- Docker packaging and a hot-path latency benchmark are pending.
```
