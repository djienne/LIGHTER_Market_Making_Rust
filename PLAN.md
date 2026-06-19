# lighter_MM_RUST — Detailed Port Plan

A high-performance Rust port of the Python `lighter_MM` market maker, with a clean
hot-path / cold-path split and aggressive Rust-specific optimizations (lock-free
atomics, ring buffers, single-writer hot path, zero-alloc steady state).

Reference template: `standx/` (Rust MM for StandX — same OBI+vol strategy shape).
Source of truth for behavior: `lighter_MM/` (Python, currently live).

## Current status (2026-06-18)

This file started as the detailed implementation plan. Keep `README.md` as the operator-facing
runbook; keep `docs/CODEX_REVIEW_*.md` as historical review snapshots. Some older findings in those
review files have since been fixed and should not be read as the current state.

Current state:
- Live and shadow modes are wired through the hot market-data task, paced sender, `account_all`,
  `user_stats`, REST stale-order reconcile, risk pause/cancel-all handling, and live PnL tracking.
- The release profile intentionally uses `panic = "unwind"` so supervised task panics can still
  reach shutdown cancel-all.
- Live-only PnL is on the cold path via `account::pnl_actor`; it writes `trades_{symbol}.csv`,
  `pnl_session_{symbol}.json`, and `pnl_snapshots_{symbol}.csv`.
- Latest local validation: `cargo test` passes 103 tests and `cargo build --release` passes.
- Latest live BTC smoke: 20 minutes on 2026-06-18, max active orders observed 4, monitor violations
  0, fills 0, strategy PnL 0.0 USDC. A direct-process shutdown test verified `cancel-all OK`,
  `verified 0 active orders`, and `PNL_SUMMARY`.

---

## 0. Guiding principles

1. **Faithful behavior.** The Python bot is live with real money. The Rust port must
   reproduce its quoting math, order lifecycle, rate-limiting, and accounting
   *bit-for-bit where feasible*, validated against the Python outputs.
2. **Do not reinvent signing.** Lighter order signing is done by a native Go `cgo`
   shared library (`lighter-signer-linux-amd64.so`, secp256k1 ECDSA-recoverable +
   Poseidon-style L2 hashing). Reimplementing it in Rust is high-risk and unnecessary.
   **We FFI into the exact same `.so`** the Python SDK uses → identical signatures.
3. **Hot path is synchronous, lock-free, allocation-free** in steady state. Cold path
   is async (tokio), does all I/O (signing, network, disk, reconciliation).
4. **Phased delivery.** The live engine is `vol_obi`; ship that path end-to-end first.

---

## 1. Key findings that shape the design

### 1.1 Signer = FFI to the official `.so` (de-risked)
- Python loads `lighter/signers/lighter-signer-linux-amd64.so` via `ctypes` and calls
  exported C symbols: `CreateClient`, `SignCreateOrder`, `SignCreateGroupedOrders`,
  `SignCancelOrder`, `SignCancelAllOrders`, `SignModifyOrder`, `CreateAuthToken`,
  `SignUpdateLeverage`, `GenerateAPIKey`, `CheckClient`. (`nm -D` confirms; build is
  Go cgo with `secp256k1_ecdsa_sign_recoverable`.)
- **No official Rust SDK exists** (only proposal issue #91). Community Rust SDKs are
  unofficial/"use at your own risk" and disagree on the crypto scheme → unacceptable
  for live funds.
- **Decision:** Rust `signer` module loads the same `.so` via `libloading`, declares
  the same `#[repr(C)]` structs + `extern "C"` fns, wraps them in a safe API.
  - Structs to mirror: `CreateOrderTxReq`, `SignedTxResponse{txType,txInfo,txHash,messageToSign,err}`,
    `ApiKeyResponse`, `StrOrErr`. **Returned `char*` are malloc'd by the library and MUST be
    freed with libc `free` after copying** — Python `decode_and_free` (`signer_client.py:89`)
    always frees via `lighter.libc.free`. Our `take_cstring` does `CStr::from_ptr` → owned
    `String` → `libc::free`. (Verified ✓ — implemented and tested.)
  - **ABI guards:** `SignedTxResponse` = 40 bytes / align 8, `StrOrErr` = 16, `CreateOrderTxReq`
    = 48. Enforced with `const _: () = assert!(size_of::<…>()==…)` so a layout drift fails the build.

### 1.1a STATUS: signer FFI verified ✓ (P0 gate passed)
Rust `test_sign` loads `lighter-signer-linux-amd64.so`, `CreateClient` OK with live creds,
signs a fixed `CreateOrder` → `tx_type=14`, valid `Sig`. **All 12 controllable tx_info fields
are byte-identical to the Python SDK** for the same inputs (AccountIndex, ApiKeyIndex,
MarketIndex, ClientOrderIndex, BaseAmount, Price, IsAsk, Type, TimeInForce, ReduceOnly,
TriggerPrice, Nonce). Only `OrderExpiry`/`ExpiredAt`/`Sig` differ — they are *now-derived
inside the `.so`* (the two runs were ~99s apart), so exact tx_hash parity across runs is
impossible by construction and is NOT required (the exchange validates the Sig over each
tx's own fields; both use the identical library).

### 1.2 Nonce management (trivial to port)
- `OptimisticNonceManager`: init `nonce[k] = GET /api/v1/nextNonce - 1`;
  `next_nonce()` = `++nonce[k]`; `acknowledge_failure()` = `--nonce[k]`;
  `hard_refresh_nonce()` = re-fetch, set to `N-1`.
- Rust: `AtomicI64` per api-key-index. Single api key in practice.

### 1.3 Send paths
- **Fast:** persistent `TxWebSocket` to `wss://mainnet.zklighter.elliot.ai/stream`,
  message `{"type":"jsonapi/sendtxbatch","data":{"tx_types":<json-str>,"tx_infos":<json-str>}}`,
  excluded from the 200 msg/min client limit. Response `{code,message,volume_quota_remaining}`.
- **Free-slot fallback:** REST `POST /api/v1/sendTx` (single op) qualifies for a free
  15s slot when volume quota is exhausted. Also `POST /api/v1/sendTxBatch`.
- `tx_type` comes back from the signer (`SignedTxResponse.txType`) — we don't hardcode it.

### 1.4 Quote engine = `vol_obi` ONLY
- **`vol_obi` (the live engine):** Welford rolling vol + OBI z-score → fair price + skewed
  half-spread. Cython `_vol_obi_fast.pyx` is the hot impl. ✓ PORTED (parity-verified).

### 1.5 Hot vs cold (from Python + standx)
- **Hot (per orderbook tick, 10–1000 Hz):** parse delta → update local book → mid →
  `VolObiCalculator.on_book_update` (rolling stats) → `quote()` → `calculate_order_prices`
  → quality/inventory bias → `collect_order_operations` → publish ops to mailbox.
- **Cold (async):** sign+send (paced), account WS (orders/positions/fills), fill
  accounting + PnL, reconciliation, sanity checks, watchdog, telemetry, Binance alpha
  feeds, metrics/persistence.

---

## 2. Architecture

```
                         ┌────────────────────── COLD PATH (tokio async) ──────────────────────┐
 Binance @depth@100ms ─► binance::depth_task ─┐                                                  │
 Binance @bookTicker  ─► binance::bbo_task   ─┤ atomic store                                     │
                                              ▼                                                   │
                                       SharedAlpha / SharedBbo (cache-line aligned AtomicU64s)    │
                                              ▲ ~1ns lock-free read                               │
 Lighter WS /stream ──► md_task (HOT, single writer) ─────────────────────────────────────────┐ │
   order_book/{m}        • parse delta (serde/simd)                                            │ │
   ticker/{m}            • update LocalBook (sorted Vec + binary search, in-task, no lock)     │ │
                         • mid; VolObi rolling stats (Welford ring buffer)                     │ │
                         • read SharedAlpha/SharedPosition/derived params (atomic)             │ │
                         • calculate_order_prices → quality/inv bias → collect ops             │ │
                         • watch::Sender.send(ops)   ───────────► mailbox (always-freshest)    │ │
                                                                         │                      │ │
 paced_send_task ◄───────────────────────────────────────────────────── watch::Receiver       │ │
   • rate-limit gate (40 ops/60s window + quota pacing + 429 backoff)                          │ │
   • sign batch via signer FFI (spawn_blocking)                                                │ │
   • send TxWebSocket (fallback REST) ; update quota atomic ; enqueue BIND_LIVE                │ │
                                                                                               │ │
 account_all WS    ─► positions+trades → SharedPosition + live PnL actor                        │ │
 user_stats WS     ─► capital/portfolio → derived params recompute (atomics)                    │ │
 REST reconcile    ─► active-order snapshots → OrderManager + orphan/max-order safety           │ │
 sanity / watchdog / telemetry / balance / quota-recovery background loops ─────────────────────┘ │
                                                                                                  │
 RiskController (circuit breaker / pause)  •  persistence (live_state json, trade csv)            │
                         └────────────────────────────────────────────────────────────────────────┘
```

**Single-writer hot path:** the market-data task owns `LocalBook` and the VolObi
calculator outright (no sharing → no locks). Everything it needs from other tasks
(Binance alpha, position, capital-derived params, quota) it reads through lock-free
atomics. Its only output is `watch::send(Vec<BatchOp>)` — a single-slot "freshest wins"
mailbox (mirrors Python `_latest_ops`).

---

## 3. Rust-specific optimizations (explicit ask)

| Concern | Technique |
|---|---|
| Cross-task scalar shared state (alpha, vol, position, mid, capital, base_amt, max_pos_usd, quota, backoff_until) | `#[repr(align(64))] AtomicU64` holding `f64::to_bits` / i64; `Relaxed` reads, `Release` on the "commit" field. Cache-line aligned to avoid false sharing (copy `standx/binance/shared_alpha.rs`, `shared_bbo.rs`, `trading/position.rs`). |
| Rolling vol / OBI z-score | Fixed-capacity ring buffer `Box<[f64]>` + Welford online mean/M2, cache `mean`/`std` on push so `zscore()` is one division (copy `standx/strategy/rolling.rs`; match Python eviction-reverse-Welford + `M2<0` guard exactly). |
| Local order book | Two sorted `Vec<(f64 price, f64 size)>` (bids desc / asks asc) with `binary_search_by` insert/remove + `sum_sizes_from/to` range sums — mirrors Cython `CBookSide`. O(1) best bid/ask (ends of vec). Bulk snapshot = parse→sort→fill. No `BTreeMap` on hot path. |
| Order ops mailbox | `tokio::sync::watch` (single value, lossy/freshest) — never sends stale prices after a long pacing wait. |
| Order events hot queue | `crossbeam::ArrayQueue` (bounded SPSC/ MPSC ring) for BIND_LIVE/CLEAR_LIVE; reconcile snapshot = `arc_swap`/watch (last-writer-wins). |
| Signing | `spawn_blocking` (CPU-bound FFI) so it never stalls the runtime; batch multiple ops per signed tx. |
| Hot path allocation | Pre-size all `Vec`s (`with_capacity(2*levels)`); reuse scratch buffers; `Arc<str>` symbols; no per-tick `String`. |
| Numeric | `f64` everywhere on hot path (matches Python float). `rust_decimal` only if a cold-path accounting test demands it (default: f64 + explicit tick rounding `(_/tick).floor()*tick`). |
| Build | `lto=true, codegen-units=1, opt-level=3, panic="unwind"` so supervised task panics can still reach shutdown cancel-all. |

---

## 4. Crate layout

```
lighter_MM_RUST/
├── Cargo.toml
├── PLAN.md                      (this file)
├── README.md
├── config.json                 (ported from lighter_MM/config.json, same schema)
├── .env.example
├── signers/                    (copy of lighter-signer-*.so for all arches)
├── src/
│   ├── main.rs                 # CLI args (--symbol, --shadow/--live), signals, main()
│   ├── lib.rs
│   ├── config.rs               # serde structs for config.json + env overrides + validate
│   ├── types.rs                # Side, BatchOp, OrderAction, TxSendStatus/Result, ids
│   ├── logging.rs              # tracing-subscriber (non-blocking file+console)
│   │
│   ├── shared.rs               # lock-free SharedAlpha/Bbo/Position/Derived atomics
│   │
│   ├── book/
│   │   ├── mod.rs
│   │   ├── local_book.rs       # sorted-Vec CBookSide-equivalent (apply snapshot/delta)
│   │   └── sanity.rs           # WS-vs-REST top-of-book divergence check
│   │
│   ├── strategy/
│   │   ├── mod.rs
│   │   ├── rolling.rs          # RollingStats (Welford ring buffer) — matches pyx
│   │   ├── vol_obi.rs          # VolObiCalculator (on_book_update, quote)  [PHASE 1]
│   │   └── quotes.rs           # calculate_order_prices, spread factors, ladder,
│   │                           #   quality multiplier, inventory exit bias, fallback
│   │
│   ├── binance/
│   │   ├── mod.rs
│   │   ├── depth_client.rs     # @depth@100ms diff-depth sync + OBI alpha (from binance_obi.py)
│   │   └── book_ticker.rs      # @bookTicker → SharedBbo
│   │
│   ├── lighter/                # exchange integration (cold path I/O)
│   │   ├── mod.rs
│   │   ├── signer.rs           # FFI to lighter-signer .so (libloading) + safe wrapper
│   │   ├── nonce.rs            # OptimisticNonceManager (atomic)
│   │   ├── rest.rs             # reqwest: nextNonce, market details, accountActiveOrders,
│   │   │                       #   getMakerOnlyApiKeys, orderBookOrders, sendTx[Batch]
│   │   ├── tx_ws.rs            # TxWebSocket (jsonapi/sendtxbatch) + connect/recv/reconnect
│   │   ├── ws.rs               # market-data + account WS subscribe loops (fast & std)
│   │   ├── auth.rs             # WS auth token (CreateAuthToken) + 9-min refresh
│   │   └── messages.rs         # serde models for all WS/REST payloads
│   │
│   ├── exec/
│   │   ├── mod.rs
│   │   ├── order_manager.rs    # per-(side,level) lifecycle state machine + event drain
│   │   ├── rate_limit.rs       # 40/60s window, quota pacing, 429 backoff, write-slot gate
│   │   ├── collect.rs          # collect_order_operations (create/modify/cancel decisions)
│   │   └── paced_send.rs       # mailbox → gate → sign(FFI) → send(WS/REST)
│   │
│   ├── account/
│   │   ├── mod.rs
│   │   ├── fill_accounting.rs  # VWAP + realized PnL + fees (matches _apply_live_fill_accounting)
│   │   ├── persistence.rs      # live_state_{sym}.json atomic save/restore
│   │   └── pnl_actor.rs        # live-only cold-path strategy PnL tracking
│   │
│   ├── metrics/
│   │   ├── mod.rs
│   │   ├── live_metrics.rs     # markout settlement (5/30/60s), quality adjustment
│   │   └── trade_log.rs        # buffered CSV
│   │
│   ├── risk.rs                 # RiskController (circuit breaker, pause/recover)
│   ├── orchestrator.rs         # startup sequence, task supervision, warmup, shutdown
└── tests/                      # parity tests vs Python golden vectors
```

`Cargo.toml` deps: `tokio`(full,parking_lot), `tokio-tungstenite`(native-tls),
`futures-util`, `serde`/`serde_json`, `reqwest`(json,rustls), `libloading`,
`tracing`/`tracing-subscriber`, `anyhow`/`thiserror`, `crossbeam`/`crossbeam-utils`,
`parking_lot`, `arc-swap`, `csv`, `dotenvy`, `fast-float`, `hex`, `chrono`.

---

## 5. Module specs (behavioral contracts to reproduce)

### 5.1 `strategy::rolling` (hot)
Welford ring buffer of capacity `window_steps` (6000). `push`: if full, reverse-Welford
evict oldest then forward-Welford add; cache `mean`,`std`; guard `M2<0→0`. `std = sqrt(M2/n)`;
`zscore(x) = (x-mean)/std`, 0 if `std<1e-10`.

### 5.2 `strategy::vol_obi` (hot) — PRIMARY
- `on_book_update(mid, &bids, &asks)`: push `mid-prev_mid` to `mid_stats`; OBI =
  `sum_bid_sizes(p≥mid*(1-depth)) − sum_ask_sizes(p≤mid*(1+depth))` push to `imb_stats`;
  once `samples≥min_warmup`: `volatility=mid_stats.std*vol_scale` (`vol_scale=sqrt(1e9/step_ns)≈3.162`),
  `local_alpha=imb_stats.zscore(obi)`; if alpha override fresh+warm use it.
- `quote(mid,pos) → (bid,ask)`: `half_spread=volatility*vol_to_half_spread`;
  `fair=mid + c1*alpha` (`c1=c1_ticks*tick`); `norm_pos=clamp(pos*mid/max_pos_usd,-1,1)`;
  `bid_depth_tick=half_spread/tick*(1+skew*norm_pos)`, `ask_depth_tick=…*(1-skew*norm_pos)`;
  raw bid/ask = fair∓depth*tick; min-spread floor vs mid; snap floor/ceil; `None` if bid≥ask.

### 5.3 `strategy::quotes` (hot)
- `calculate_order_prices`: L0 quote → position-limit suppression
  (`|pos*mid|≥max_pos_usd` ⇒ drop add side; both ⇒ reduce-only fallback) →
  ladder levels `1..N` via precomputed `SPREAD_FACTORS[l]=spread_factor_level1^l` →
  tick rounding.
- `apply_quality_spread_multiplier` (1.0–1.5) and `apply_inventory_exit_bias`
  (tighten exit, widen add by inventory ratio, with adverse-markout boost) — exact
  formulas in `lighter_estimators`/`live_metrics` readers.

### 5.4 `binance::depth_client` (cold)
Official diff-depth sync: buffer events → REST `/fapi/v1/depth?limit=1000` snapshot →
align on `lastUpdateId` → drain (`U≤lastId+1≤u`, then `pu==prev_u`) → live apply →
`_update_alpha` (OBI z-score on Binance book) → `SharedAlpha.update`. `book_ticker` →
`SharedBbo`. Reconnect with backoff. URLs `wss://fstream.binance.com/ws/{sym}@...`.

### 5.5 `lighter::signer` (cold, CPU) — CRITICAL
`libloading::Library::new("signers/lighter-signer-linux-amd64.so")`; bind symbols with
exact ctypes signatures (see §1.1). Safe wrapper:
- `create_client(url, api_key_priv_hex, chain_id, api_key_index, account_index)` once.
  *(verify chain_id source — REST/info or constant; CreateClient takes c_int chain_id.)*
- `sign_create_order(market_index, client_order_index, base_amount, price, is_ask,
   order_type=LIMIT(0), tif=POST_ONLY(2), reduce_only, trigger=0, expiry=-1, nonce, api_key_index, account_index) → (tx_type, tx_info_json, tx_hash)`.
- `sign_modify_order`, `sign_cancel_order`, `sign_cancel_all_orders(tif, ts_ms,…)`,
  `create_auth_token(deadline_ts, api_key_index, account_index)`, `sign_update_leverage`.
- Decode `*const c_char` → owned `String`; map `err` ptr → `Result`.
Call from `spawn_blocking`; serialize sign+send under one async `Mutex` (nonce safety).

### 5.6 `exec::rate_limit` (hot-ish)
`write_slot(op_count, cancel_only) -> bool` 4 phases: (1) global 429 backoff
(`sleep` if ≤2s else skip); (2) 40-ops/60s sliding `VecDeque<Instant>` (wait ≤30s else
skip); (3) min interval 0.1s (0.5s cancel-only); (4) quota pacing mult
(≥500→1.0, 50–499→1.5, 10–49→3.0, <10→∞ free-slot 15–16s). Backoff: `min(15*2^(lvl-1),120)`.

### 5.7 `exec::order_manager` (hot+cold boundary)
Per `(side, level)` lifecycle `IDLE→PLACING→LIVE→{MODIFYING→LIVE | CANCELING→IDLE}`.
`drain_hot_events` (BIND_LIVE/CLEAR_LIVE/CLEAR_ALL from crossbeam queue),
`drain_reconcile_events` (latest snapshot). `client_order_index → order_index` map
(bounded ~200, keep live+recent). Watchdog clears PLACING>30s.

### 5.8 `account::fill_accounting` (cold)
`apply(side,price,size) → (pos_after, realized_delta, realized_cum, vwap_after, fee)`.
Fee `=|price*size*maker_fee_rate|`. Flat→open; same-sign→VWAP add; opposite→realize
`(exit-entry)*close_size` (signed by side), flip/flatten on cross. Persist after each.

### 5.9 `lighter::ws` + `auth` + reconcile (cold)
Fast loop (`tokio::select!` recv/timeout/reconnect) for `order_book/ticker`; long-timeout
private loops for `account_all` and `user_stats`. Private channels use auth tokens generated by
the native signer and refresh through reconnect/subscription handling. Orderbook snapshot vs delta
is handled by `type` (`subscribed/...`=snap, `update/...`=delta) plus nonce/offset stale guards.
The incremental `account_orders` stream is intentionally not used as an authoritative reconcile
source because later messages are deltas, not full snapshots. Full active-order reconciliation uses
REST `accountActiveOrders`, detects/cancels orphans, enforces `max_live_orders_per_market`, and arms
risk pause/cancel-all after debounced mismatches.

### 5.10 `orchestrator` (cold)
Startup: validate config → REST market details (ticks, mins) → spawn Binance OBI/BBO feeds →
build shared atomics and hot task → in live mode acquire the single-instance lock, initialize nonce,
connect TxWebSocket, run verified startup cancel-all, start paced sender, private account streams,
REST stale-order reconcile, and optional live PnL actor → run the hot md loop. Shutdown sets the
halt gate, waits for in-flight send serialization via `sdk_lock`, aborts the sender, runs verified
cancel-all, and asks the PnL actor to flush `PNL_SUMMARY`.

---

## 6. Config — Rust live-engine subset of `lighter_MM/config.json`
Same active keys/sections (`trading.{leverage,levels_per_side,base_amount,capital_usage_percent,
default_quote_update_threshold_bps,spread_factor_level1,…,quote_engine,vol_obi{…},
alpha{…},live_quality{…},inventory_exit_bias{…}},
performance{…}, websocket{…}, safety{…}, pnl{…}`). serde structs with `#[serde(default)]`;
env overrides (`MARKET_SYMBOL`, `API_KEY_*`, `ACCOUNT_INDEX`, `WALLET_ADDRESS`) via dotenvy.

---

## 7. Milestone status

- **P0 — Scaffold/signer:** done; Rust FFI signs with the official `.so` and matches Python
  controllable `tx_info` fields.
- **P1 — Market data + signal:** done; local book, rolling stats, Vol/OBI, Binance OBI/BBO feeds,
  and shared atomics are implemented with parity tests.
- **P2 — Execution:** done for live/shadow; nonce, REST, TxWebSocket, rate limiter, order manager,
  signing bridge, and paced sender are implemented.
- **P3 — Account + safety:** done for live operation; `account_all`, `user_stats`, REST reconcile,
  risk pause/cancel-all, persistence, metrics, and live PnL are wired.
- **P5 — Hardening:** partially done; README/runbook, Docker packaging, smoke/live checks, and
  review fixes are in place. A dedicated hot-path latency benchmark remains.

Routine gates: `cargo test`, `cargo build --release`, targeted smoke/live checks, and review of any
changes touching live order management.

---

## 8. Testing & validation
- **Signer parity:** implemented via `test_sign`; both Rust and Python call the same `.so`.
- **Vol/OBI parity:** implemented via `test_obi_parity`; deterministic parity is within floating
  tolerance.
- **Unit tests:** latest local suite passes 103 tests, including websocket response routing,
  unknown-after-write handling, rate limiting, risk, order lifecycle, and live PnL attribution.
- **Live smoke:** run only when explicitly intended, with a single instance per account/api key.
  Prefer direct-process log redirection over `| tee` so SIGINT/SIGTERM hits the bot process and the
  shutdown log contains verified cancel-all and `PNL_SUMMARY`.

## 9. Remaining risks / open items
- A dedicated hot-path latency benchmark is still pending.
- Docker live validation should continue periodically after material execution changes; the
  2026-06-19 Docker BTC run covered the 600-second warmup gate, live order placement, fills, PnL
  accounting, and clean shutdown verification.
- Long unattended live soaks should continue to monitor quota, active-order cap, reconcile
  mismatches, private websocket freshness, and PnL attribution (`PNL_SKIP`/`PNL_LOCAL_MISMATCH`).
- If quota consumption accelerates materially, increase the quote-replacement bps threshold or tune
  the adaptive quota bands before continuing long live runs.

---

## 10. GPT-5.5 (codex) plan-review — incorporated corrections

Codex verdict: "FFI into the existing signer `.so` is the right call." The following
binding corrections are now part of the design (the most live-breaking first):

1. **C-string ownership (DONE):** copy then `libc::free` every returned `char*`
   (`signer.rs::take_cstring`). Already implemented & tested.
2. **ABI asserts (DONE):** `const` size/align asserts on the FFI structs.
3. **WS unknown-outcome is sacred:** if a `sendtxbatch` WS frame *may* have been written
   but the response was lost (`TxSendStatus::Unknown`), **never REST-retry** — instead
   pause trading, `hard_refresh_nonce`, and force reconciliation (Python
   `market_maker_v2.py:3910,4000`). REST fallback is only valid when *no* WS frame was sent.
4. **Order-state events must be LOSSLESS:** BIND_LIVE / CLEAR_LIVE / CLEAR_ALL mutate
   canonical order state and use an **unbounded** queue (`crossbeam::SegQueue` or tokio
   unbounded mpsc), drained every hot tick (Python unbounded deque, `:814,:1175`). Only the
   *quote-ops mailbox* is lossy (`watch`, freshest-wins). If a bounded queue is ever used,
   overflow ⇒ pause + reconcile (not silent drop).
5. **Book storage matches CBookSide exactly:** store BOTH sides **ascending** by price;
   best_bid = last element, best_ask = first; `OBI = sum_sizes_from(lower) − sum_sizes_to(upper)`
   (`_vol_obi_fast.pyx:25,647`). (Storing bids-desc is allowed only if parity tests pass; we
   default to ascending to remove the risk.)
6. **Half-even rounding:** raw price/amount = `round(value/tick)` where Python `round()` is
   **banker's rounding (half-to-even)** (`market_maker_v2.py:380`). Rust `f64::round` is
   half-away-from-zero ⇒ implement `py_round_half_even()` and use it everywhere raw ints are
   produced; cover with parity tests.
7. **Hot path wakes on Binance alpha too**, not only Lighter book ticks — bump the quote
   signal on external-alpha update (`market_maker_v2.py:672,4802`).
8. **Full hot-read set:** mid/book (owned), + lock-free reads of: risk pause, order events,
   position, capital-derived `base_amount`/`max_pos_usd`, quota & adaptive threshold, quality
   adjustment, inventory-exit bias (`:4812,4841,4866`).
9. **Warmup is time-based** (`warmup_seconds`) with a bypass to **reduce-only** quoting when
   startup inventory exists; VolObi also has an internal readiness gate (`:4731`).
10. **Exact sizing math:** port `calculate_dynamic_base_amount` (capital·usage·leverage →
    size, nearest amount tick, min base/quote/order-value, `:3203`) and
    `_dynamic_max_position_dollar` (subtract both-side resting reserve, ×0.9, `:1413`) verbatim.
11. **Live-send gating:** order sending stays DISABLED until account WS, reconciliation,
    maker-only-key detection, emergency close, leverage setup, unknown-outcome handling, and
    fill accounting are all implemented (Python live startup/shutdown `:5343,5543,5578,5707`).

Current note: the blocking items from this review were addressed during later implementation and
live-safety passes. The historical review text above is kept because it explains design decisions,
but use the status sections in this file and `README.md` for the current runbook.

---

## 11. Build status & review history

**Built + verified:** signer FFI, Vol/OBI parity, Binance alpha/BBO feeds, shared lock-free atomics,
order manager + collect, rate limiter, signing bridge, TxWebSocket, paced sender, fill accounting,
risk, persistence, metrics, REST/nonce/ws/auth, live/shadow orchestrator wiring, REST reconcile,
and live PnL tracking. Latest local status: 103 unit tests pass and release build passes.

The review files in `docs/` are retained as historical audit records. They are useful for root-cause
context, but some findings have since been fixed; prefer this file plus `README.md` for current
status.
