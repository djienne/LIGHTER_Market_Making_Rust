# lighter_MM_RUST — Detailed Port Plan

A high-performance Rust port of the Python `lighter_MM` market maker, with a clean
hot-path / cold-path split and aggressive Rust-specific optimizations (lock-free
atomics, ring buffers, single-writer hot path, zero-alloc steady state).

Reference template: `standx/` (Rust MM for StandX — same OBI+vol strategy shape).
Source of truth for behavior: `lighter_MM/` (Python, currently live).

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
4. **Phased delivery.** The live engine is `vol_obi`. Ship that path end-to-end first;
   Cartea-Jaimungal (CJ) HJB engine is phase 2.

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
- **Cartea-Jaimungal is DROPPED.** It is deprecated/unused leftover in the Python bot
  (unrelated to the OBI strategy). NOT ported. `subscribe_to_public_trades` (which only fed
  the CJ estimator) and all `_cj_*` machinery are skipped. `nalgebra` dep can be removed.

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
   trade/{m}             • mid; VolObi rolling stats (Welford ring buffer)                     │ │
                         • read SharedAlpha/SharedPosition/derived params (atomic)             │ │
                         • calculate_order_prices → quality/inv bias → collect ops             │ │
                         • watch::Sender.send(ops)   ───────────► mailbox (always-freshest)    │ │
                                                                         │                      │ │
 paced_send_task ◄───────────────────────────────────────────────────── watch::Receiver       │ │
   • rate-limit gate (40 ops/60s window + quota pacing + 429 backoff)                          │ │
   • sign batch via signer FFI (spawn_blocking)                                                │ │
   • send TxWebSocket (fallback REST) ; update quota atomic ; enqueue BIND_LIVE                │ │
                                                                                               │ │
 account_orders WS ─► order events → OrderManager (fills, cancels, reconcile snapshots)        │ │
 account_all WS    ─► positions+trades → SharedPosition (atomic) + fill accounting (PnL)        │ │
 user_stats WS     ─► capital/portfolio → derived params recompute (atomics)                    │ │
 reconciler / sanity / watchdog / telemetry / balance / quota-recovery background loops ────────┘ │
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
| Build | `lto=true, codegen-units=1, opt-level=3, panic="abort"` (copy standx profile). |

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
│   ├── main.rs                 # CLI args (--symbol, --live/--dry-run), signals, main()
│   ├── lib.rs
│   ├── config.rs               # serde structs for config.json + env overrides + validate
│   ├── types.rs                # Side, BatchOp, OrderAction, TxSendStatus/Result, ids
│   ├── logging.rs              # tracing-subscriber (non-blocking file+console)
│   │
│   ├── shared/                 # lock-free cross-task primitives
│   │   ├── mod.rs
│   │   ├── atomic_f64.rs       # AlignedAtomicF64 helpers
│   │   ├── shared_alpha.rs     # Binance OBI alpha+vol (from standx)
│   │   ├── shared_bbo.rs       # Binance best bid/ask (from standx)
│   │   ├── shared_position.rs  # position (atomic f64) (from standx)
│   │   └── derived.rs          # base_amount, max_pos_usd, capital, mid, quota (atomics)
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
│   │   # NOTE: cartea_jaimungal / cj_estimator are NOT ported (deprecated, unused).
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
│   │   ├── reconcile.rs        # local↔remote order diff, orphan cancel, stale poller
│   │   └── persistence.rs      # live_state_{sym}.json atomic save/restore
│   │
│   ├── metrics/
│   │   ├── mod.rs
│   │   ├── live_metrics.rs     # markout settlement (5/30/60s), quality adjustment
│   │   └── trade_log.rs        # buffered CSV
│   │
│   ├── risk.rs                 # RiskController (circuit breaker, pause/recover)
│   ├── orchestrator.rs         # startup sequence, task supervision, warmup, shutdown
│   └── dry_run.rs              # paper-trading fill simulator                [PHASE 1.5]
└── tests/                      # parity tests vs Python golden vectors
```

`Cargo.toml` deps: `tokio`(full,parking_lot), `tokio-tungstenite`(native-tls),
`futures-util`, `serde`/`serde_json`, `reqwest`(json,rustls), `libloading`,
`tracing`/`tracing-subscriber`, `anyhow`/`thiserror`, `crossbeam`/`crossbeam-utils`,
`parking_lot`, `arc-swap`, `csv`, `dotenvy`, `fast-float`, `hex`, `chrono`,
and (phase 2) `nalgebra` for the HJB matrix exponential.

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
- `calculate_order_prices`: gate (CJ readiness if CJ engine) → L0 quote → position-limit
  suppression (`|pos*mid|≥max_pos_usd` ⇒ drop add side; both ⇒ reduce-only fallback) →
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
Fast loop (`tokio::select!` recv/timeout/reconnect) for `order_book/ticker/trade`;
std loop (long 1800s timeout) for `user_stats/account_all/account_orders`. Private
channels need auth token (`CreateAuthToken`, refresh @9min). Orderbook snapshot vs delta
by `type` (`subscribed/...`=snap, `update/...`=delta) + offset stale-guard. Reconcile:
account_orders snapshot/delta drives BIND_LIVE/CLEAR_LIVE + orphan detection; REST stale
poller every 3s (WS down) / 60s (WS up); orphan cancel via signed batch; pause after
`debounce=2` mismatches.

### 5.10 `orchestrator` (cold)
Startup: validate config → REST market details (ticks, mins) → build calculators →
spawn Binance feeds (vol_obi engine) → signer create_client → cancel_all at startup →
TxWebSocket connect → subscribe md/ticker/(trades for CJ) → (live) subscribe account
channels + init fill accounting from account snapshot → wait for first book/account →
optional panic-close → spawn background tasks (balance, sanity, watchdog, telemetry,
reconciler, order_state_reconcile, paced_send) → run hot md loop → graceful shutdown
(bounded 15s task cancel, flush logs, cancel_all, optional panic-close, persist state).

---

## 6. Config — reuse `lighter_MM/config.json` verbatim
Same keys/sections (`trading.{leverage,levels_per_side,base_amount,capital_usage_percent,
default_quote_update_threshold_bps,spread_factor_level1,…,quote_engine,vol_obi{…},
cartea_jaimungal{…},cj_estimator{…},alpha{…},live_quality{…},inventory_exit_bias{…}},
performance{…}, websocket{…}, safety{…}`). serde structs with `#[serde(default)]`;
env overrides (`MARKET_SYMBOL`, `API_KEY_*`, `ACCOUNT_INDEX`, `WALLET_ADDRESS`) via dotenvy.

---

## 7. Phasing / milestones

- **P0 — Scaffold:** Cargo workspace, config, types, logging, signer FFI + a standalone
  `bin/test_sign.rs` that loads the `.so`, creates client, signs a dummy order, prints
  tx_info. **Gate: signing works & matches Python output for identical inputs.**
- **P1 — Market data + signal:** local_book, rolling, vol_obi, binance feeds, shared
  atomics. `bin/test_obi.rs` parity vs Python on a captured WS tape. **Gate: vol/alpha
  match Python within 1e-9 on the same input stream.**
- **P2 — Execution:** nonce, rest, tx_ws, rate_limit, order_manager, collect, paced_send.
  Dry-run engine. **Gate: end-to-end dry-run quotes for 10 min, no panics, sane orders.**
- **P3 — Account + safety:** account WS, fill_accounting, reconcile, risk, persistence,
  metrics, orchestrator. **Gate: live-shadow (order.enabled=false) parity vs Python.**
- **P5 — Hardening:** Docker, README, soak test, latency benchmark (p50/p99 hot path),
  + final codex reviews (overall correctness + hot/cold/latency/lock-free). (CJ engine
  dropped — not in scope.)

Each gate: build (`cargo build --release`), `cargo clippy`, unit/parity tests, and a
**GPT-5.5 (codex) review** of the diff before moving on.

---

## 8. Testing & validation
- **Golden vectors:** dump Python intermediate values (vol, alpha, quote prices, VWAP,
  PnL, rate-limit decisions) to JSON; assert Rust matches within tolerance.
- **WS tape replay:** capture a live Lighter+Binance WS session; replay into both Python
  and Rust; diff orderbook state, mids, signals, and emitted ops.
- **Signer parity:** same (market,price,amount,nonce,…) → identical tx_info/tx_hash from
  Python `ctypes` path and Rust `libloading` path (both call the same `.so`).
- **Dry-run soak:** 30+ min, assert no drift/deadlock; latency histogram of hot path.
- **NEVER place live orders during dev.** Use dry-run / `order.enabled=false`. The VPS
  already runs live bots — keep this crate fully isolated in `lighter_MM_RUST/`.

## 9. Risks & open items (verify during P0/P1)
- **chain_id** value for `CreateClient` (likely from REST `/info` or a mainnet constant).
- Exact **`.so` C-string ownership** (Python doesn't free → confirm no leak/UAF; copy then drop).
- **TxWebSocket** exact init/handshake message & response schema (`code` 200→0 normalize).
- **account_all / account_orders** field names (`sign`, `is_ask`, `status`, `client_order_index`,
  `order_index`, fill `size/price/timestamp`) — lock down from live capture.
- **order_expiry / GTT vs POST_ONLY** semantics (live uses POST_ONLY tif=2, expiry=-1=28d).
- Multi-threaded vs current-thread tokio for the hot md task (pin md task; use a
  dedicated runtime/thread if jitter observed).

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

Blocking open items (codex): C-string free ✓, ABI asserts ✓, TxWebSocket unknown-outcome,
account-stream field capture (live), raw-rounding parity, nonce-failure matrix, bounded-event
overflow policy, maker-only handling. Non-blocking: tokio current- vs multi-thread (latency tuning).

---

## 11. Build status & second-round codex review (full reviews in `docs/`)

**Built + verified:** signer FFI (byte-parity to Python), vol/OBI engine (1e-17 parity to the
Cython engine), Binance alpha feeds, shared lock-free atomics, order manager + collect, rate
limiter, signing bridge, tx WebSocket, paced sender, fill accounting, risk, persistence, metrics,
REST/nonce/ws/auth, orchestrator with **shadow mode (verified live)** and **live mode fully wired**
(paced_send + account_orders/account_all/user_stats WS with 9-min token refresh + REST reconcile
poller). 89 unit tests pass.

**Two GPT-5.5 reviews run** (`docs/CODEX_REVIEW_correctness.md`, `docs/CODEX_REVIEW_hotpath_latency.md`).
Fixes APPLIED from them:
- Live wiring completed (was the #1 finding); `set_max_position_dollar` before `quote`.
- `accountActiveOrders` rows with no `status` treated LIVE (prevents reconcile mass-clear → dup orders).
- `ClearLive` only clears when the slot id matches (no clobbering a freshly-placed order).
- Sign-batch abort rolls back ALL reserved nonces (no nonce gap); create price guarded to u32 range.
- Order events + reconcile drained every tick before the quote throttle.
- Rate limiter OWNED by paced_send (no mutex held across `write_slot` awaits).

**Tracked follow-ups (not yet applied; see review docs):**
- Sender: re-borrow freshest ops AFTER the rate wait; `NotSent` → REST `sendTxBatch` fallback;
  free-slot path truncate multi-op → 1 reducing op via REST `sendTx`; count window ops on any
  attempted frame (not only `Ok`); nonce hard-refresh on quota/reject classes.
- Reconcile: detect + cancel orphan exchange orders and enforce `max_live_orders_per_market`.
- Sizing: drive `base_amount` from capital+min-quote (currently fixed from config; max-pos IS dynamic).
- Hot-path latency micro-opts: avoid `serde_json::Value` clone (borrow/simd-json), reuse level/ladder
  scratch buffers (`SmallVec`/fixed arrays), publish adaptive threshold to an atomic the hot path reads.
- Lock-free nuance: load the commit timestamp `Acquire` in `SharedAlpha`/`SharedBbo` (seqlock for `Derived`).
- Pin the market-data/hot task to a dedicated current-thread runtime for p99 (after the above).
- Docker image + hot-path latency benchmark.
```
