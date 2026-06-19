# lighter_MM_RUST

A high-performance **Rust** version of the Python
[`LIGHTER_Market_Making`](https://github.com/djienne/LIGHTER_Market_Making) bot for the Lighter
(zkLighter) perpetuals exchange. It keeps the same market-making model while moving the live hot
path to native Rust, so it uses substantially less CPU and RAM than the Python version. The port
uses a clean **hot-path / cold-path** split and Rust-specific optimizations (lock-free cache-aligned
atomics, Welford ring buffers, single-writer hot path, freshest-wins mailbox, FFI signing off the
hot path).

Observed resource use in live BTC runs on the current host: about **0.8% CPU** and **35 MB RSS**
while managing live orders. Treat this as an environment-specific runtime observation, not a fixed
guarantee.

Strategy: **volatility + order-book-imbalance (OBI) alpha**, with an external Binance OBI feed —
the same model as the live Python bot and the `standx` reference.

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
   • persistent TxWebSocket (recv loop + keepalive ping); Unknown ⇒ pause+refresh+reconcile │
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
| `account/{fill_accounting,persistence,pnl_actor}.rs` | VWAP/PnL, live-state JSON, live session PnL actor | ✅ |
| `metrics/{trade_log,live_metrics}.rs` | buffered CSV, markout quality adjustment | ✅ |
| `risk.rs` | circuit breaker / pause | ✅ |
| `orchestrator.rs` | bootstrap + hot task + feeds; shadow/live wiring, health logging | ✅ |
| `config.rs`, `types.rs`, `util.rs`, `logging.rs` | config, core types, numeric helpers (banker's rounding), logging | ✅ |

## Build & run

```bash
cargo build --release            # optimized (LTO, panic=unwind for clean cancel-all on task panic)
cargo test                       # 103 unit tests incl. parity and websocket/order-management tests

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

### Docker

The compose service defaults to a no-order warmup/validation run. It connects to live market data
and computes quotes, but it does not place orders. The binary flag for this mode is currently
`--shadow`, and logs show `mode=Shadow`.

```bash
mkdir -p logs
docker compose build
docker compose up
```

Live trading is explicit and uses real credentials. It starts live infrastructure immediately, runs
startup cancel-all/flat-book verification, then keeps normal order placement gated by
`trading.vol_obi.warmup_seconds` from `config.json` (`600` seconds in the checked-in config):

```bash
docker compose --env-file /path/to/.env run --name lighter-mm-live lighter-mm \
  --symbol BTC --config /app/config.json --live
```

Set `LIGHTER_UID` and `LIGHTER_GID` if the host `logs/` directory should be written by a user other
than UID/GID `1000:1000`.

See `docs/DOCKER.md` for the full Docker run-mode and shutdown notes.

For manual live runs, run `lighter-mm` as the foreground process or as the direct service process
and redirect logs to a file. Avoid wrapping the live process in `| tee` for production-style runs;
stop the actual `lighter-mm` PID with SIGINT/SIGTERM and confirm the log contains
`shutdown signal received`, `cancel-all OK`, `verified 0 active orders`, and `PNL_SUMMARY`.

```bash
mkdir -p logs
set -a
. /home/ubuntu/lighter_MM/.env
set +a
RUST_LOG=info ./target/release/lighter-mm --symbol BTC --live > "logs/live_$(date -u +%Y%m%d_%H%M%S).log" 2>&1
```

## Live PnL tracking

Live mode starts a cold-path PnL actor when `pnl.enabled=true` in `config.json`. It does not run in
shadow mode and does not add disk I/O, locks, or awaits to the quote loop. The actor consumes
accepted bot client IDs from the paced sender and deduped `account_all` fills from the private
websocket, then records only fills attributable to this bot process.

Artifacts are written under `pnl.persist_dir` (default `logs/`):

- `trades_{symbol}.csv`: one row per deduped strategy fill.
- `pnl_session_{symbol}.json`: latest live session summary.
- `pnl_snapshots_{symbol}.csv`: periodic session snapshots.

The headline field is `strategy_realized_pnl_usdc`. `strategy_mtm_pnl_usdc` adds estimated
unrealized PnL for any open inventory using the latest fresh Binance BBO mid. Account balance
changes are not labeled strategy PnL because they can include inherited inventory, transfers, or
manual account activity.

## What's verified

- **Signer**: Rust FFI produces byte-identical `tx_info` to the Python SDK across all controllable
  fields (`tx_type=14`, valid `Sig`). No crypto reimplemented — same official `.so`.
- **Quote math**: volatility, OBI alpha, and bid/ask match the live Cython engine to 1e-17 on a
  deterministic scenario.
- **Hot path**: shadow mode runs WS → book → signal → quote ladder → order ops on live BTC data
  with the Binance alpha feed, no errors.
- **WebSockets**: Lighter subscription sockets and the tx WebSocket send proactive keepalive frames
  under the documented 2-minute requirement, continuously drain reads, handle app-level pings with
  pongs, reconnect with stable-session backoff reset, and have local tests for tx response routing
  plus unknown-after-write handling.
- **Live safety**: startup/shutdown use cancel-all + REST verification; live mode has a per-account
  single-instance lock, a reconcile poller, orphan cancellation, max-live-order cap enforcement, and
  minute-level health logs for feed ages, position, capital, quota, and max-position state. Rejected
  order batches log the exchange code, reject class, and exact op summary for post-mortem review;
  business rejections force an immediate active-order reconcile before the next retry. Live PnL logs
  emit `PNL_FILL`, `PNL_HEALTH`, and `PNL_SUMMARY` lines for post-mortem review.
- **Docker live run**: Docker live mode has been validated on BTC with the 600-second warmup gate,
  live order placement, fills, PnL accounting, and clean SIGINT shutdown verification.

## Status / remaining

- LIVE mode is wired through `account_all`, `user_stats`, the REST stale-order reconcile poller, and
  the paced sender. The incremental `account_orders` stream is intentionally not used for full
  reconcile because it emits deltas rather than authoritative full active-order snapshots.
- A fill-simulating dry-run engine (the Python `dry_run.py`) is not ported (use the Python one for
  backtests; the Rust bot targets live/shadow).
- A hot-path latency benchmark is pending.
