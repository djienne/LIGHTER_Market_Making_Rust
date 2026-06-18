# Parity fix plan — decision/hot path vs Python

Inputs: codex (gpt-5.5) review of spread+cancel/replace, and a 7-area parity-audit workflow
(12 confirmed deviations, 11 to-fix). **Both confirm the core FORMULAS match** (level-0 spread,
ladder spacing, adjuster math, pure collector rules, vol/OBI to 1e-17). Every gap below is *glue/
wiring*, not math. "Rust may be better, not worse" — we only close gaps where Rust is wrong/weaker.

Config values (config.json): capital_usage_percent=0.15, leverage=2, min_order_value_usd=14.5,
vol_obi.warmup_seconds=600, vol_obi.min_half_spread_bps=4.0, safety.stale_order_debounce_count=2,
rate_limit_send_interval=0.15. All already parsed in config.rs.

## Current status (2026-06-18)

This parity plan has mostly been executed:
- Group A remains intentionally skipped because the adverse-selection quality loop is dead in the
  Python production path.
- Groups B, C, D, E, and F are implemented in the Rust live path.
- Group G remains an intentional non-port.
- Group H is closed by keeping Rust's explicit 0.15s configured/default send interval consistent
  with `config.json`; it was not changed to 0.1.

---

## Group A — Live adverse-selection quality loop — **SKIPPED**
User: "ignore A — it is not used in the python code." The live-metrics quality adjustment is NOT
active in the Python production path, so this is NOT a real deviation (the audit/codex flagged dead
Python code). We keep quality multiplier = 1.0, adverse_bps = 0.0, no quality size_multiplier
(audit #2/#4/#5 are non-issues). Group B keeps ONLY the capital-derived base size.

## Group A (original, not implemented) — Live adverse-selection quality loop [audit #2/#4/#5, codex P0/P1]
**Gap:** `maybe_quote` calls `apply_quality_spread_multiplier(.,1.0,.)` and
`apply_inventory_exit_bias(.,adverse_bps=0.0,.)` and never applies `size_multiplier`. The ported
`LiveMetricsTracker` (metrics/live_metrics.rs) is never instantiated. So under adverse markouts the
Rust does NOT widen spreads, shrink size, or boost inventory-exit (Python does all three).
**Fix:**
1. Fill feed: `account_all` handler parses `msg.trades[market]`, dedups by `trade_id`, derives side
   (`bid_account_id==acct`→Buy else Sell), parses price/size, sends `FillEvent{side,price,size}` over
   a new mpsc to the hot task.
2. `HotTask` owns `live_metrics: Option<LiveMetricsTracker>` (Some in Live) + `fill_rx`. Each
   `maybe_quote`: drain fills→`record_fill(side,price,size,Some(mid))`; `update(now_ms,mid)`;
   `adj=current_adjustment()`.
3. Apply `adj.spread_multiplier`→quality mult, `adj.adverse_bps`→inventory bias; `size_multiplier`→Group B.
Shadow keeps neutral (no fill feed).

## Group B — Capital-derived dynamic base_amount — **DONE** [audit #3]
**Gap:** order size is the static config `base_amount` (0.0002 BTC); Python sizes each order as
`capital*capital_usage_percent*leverage/mid` (normalized), recomputed live. `capital_usage_percent`
is dead config in Rust.
**Fix:** in `maybe_quote`, when capital>0 compute `raw=capital*cap_use*leverage/mid`; apply Group A
`size_multiplier`; `order_size=normalize_live_order_size(raw*size_mult, mid, amount_tick, min_base,
max(min_quote,min_order_value_usd))`; pass to `collect_order_operations` (and keep
`dynamic_max_position` coherent). Fall back to seed base when capital unknown (shadow).

## Group C — Wall-clock warmup_seconds=600 gate — **DONE** [audit #6]
**Gap:** Rust quotes as soon as count warmup (min_warmup_samples=100) hits — seconds in, on a noisy
vol estimate. Python suppresses ALL quoting for `warmup_seconds` (600s), bypassed only to quote a
reduce-only exit if already holding inventory.
**Fix:** record `loop_start: Instant` (first book tick); gate quoting on `elapsed>=warmup_seconds`
**AND** `calc.warmed_up()`. Bypass: if `elapsed<warmup` but `|position|>=EPS`, allow fallback reduce-only.

## Group D — calc.reset() only on first init, not every snapshot — **DONE** [audit #7]
**Gap:** `on_orderbook` resets the vol/OBI calc on EVERY in-connection snapshot frame (wiping warmup);
Python resets only on disconnect.
**Fix:** `calc.reset()` only when `!book.initialized` (first snapshot); apply later in-connection
snapshots to the book WITHOUT resetting calc. Reset calc in the WS `on_disconnect` callback instead.

## Group E — Binance SharedAlpha/SharedBbo reset on reconnect — **DONE** [audit #8]
**Gap:** on Binance depth reconnect/re-snapshot the shared alpha is not reset → stale alpha leaks and
post-resync warmup is bypassed. Python calls `SharedAlpha.reset()`.
**Fix:** add `reset()` to SharedAlpha/SharedBbo (sample_count=0, last_update_ms=0, value=0); call from
`BinanceObi::reset()` and the depth/book_ticker reconnect paths.

## Group F — Reconcile circuit-breaker pauses — **DONE** [audit #9, #10]
**Gap:** Rust only `mark_reconcile(false)` on mismatch/over-cap; Python ALSO arms a cooldown pause.
**Fix:** in the poller — after a failed/mismatch poll, if `mismatch_streak>=max(1,stale_order_debounce_count)`
→ `trigger_pause`; in the over_cap branch, add `trigger_pause` alongside the cancel-all.

## Group G — Freshest-reborrow under pacing — **INTENTIONAL NON-PORT (different, not worse)**
**Gap (codex P2):** sender sends the batch it gated on, not the freshest published during the gate
sleep — so a quote can be ≤0.15s stale.
**Decision:** NOT ported. The Rust marks each emitted batch's slots pending PRE-send (the ≤4
duplicate-create keystone), so mark↔send must stay coherent. The two ways to adopt Python's
peek-then-pull both regress: (a) re-borrow-freshest leaves the gated batch's unique slots
marked-but-unsent (under-quote until 5s reconcile grace); (b) skip-and-defer STARVES under fast
republish (Python never starves because it always sends the freshest). The staleness cost is ≤ one
send-interval on a continuously-requoting maker — negligible — vs. losing the dup guarantee.
Documented in paced_send.rs::send_once.

## Group H — rate_limit_send_interval default/doc decision — **CLOSED** [audit #11]
**Decision:** keep Rust's default aligned with the live `config.json` value of 0.15s. No live impact
from the earlier audit note because `config.json` explicitly sets 0.15s and the current default
matches it.

---

## Execution status

Implemented/closed items were validated with the normal local checks and live smoke runs. Remaining
follow-up outside this parity plan: longer unattended live soak plus Docker/latency benchmark work
tracked in `PLAN.md`.
