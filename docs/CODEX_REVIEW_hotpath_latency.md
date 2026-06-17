**Verdict**
The intended split is good, but the current implementation is not a zero-alloc hot path and live hot/cold wiring is incomplete. Biggest issues are stale `watch` semantics, JSON `Value` cloning, per-cycle quote Vec churn, and derived/rate state not actually read by the hot path.

**Top Fixes**
1. **Fix stale freshest-wins sends. Correctness.**
   [exec/paced_send.rs:45](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:45) clones ops once, then [send_once](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:62) can await rate sleeps/sign/send. That can send old prices after a newer quote exists. Also [orchestrator.rs:341](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:341) only sends nonempty ops, so an empty current decision does not clear a stale pending batch.
   Concrete change: publish every cycle including empty/tombstone, attach a sequence, and re-borrow `watch` after every await before signing. Better: watch desired target state, not differential ops.

2. **Remove `serde_json::Value` from the hot path. High latency.**
   [lighter/ws.rs:122](/home/ubuntu/lighter_MM_RUST/src/lighter/ws.rs:122) parses full `Value`; [orchestrator.rs:241](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:241) clones it into `OrderBookMsg`; [orchestrator.rs:245](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:245) does the same for ignored ticker.
   Concrete change: parse `Message::Text` directly into a borrowed hot struct with `&str` fields, or use `simd-json` on mutable bytes. Route ticker out of hot path or unsubscribe if unused.

3. **Stop allocating parsed level Vecs per orderbook tick. High latency.**
   [orchestrator.rs:261](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:261) and [orchestrator.rs:262](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:262) allocate bid/ask `Vec<(f64,f64)>` every message.
   Concrete change: add reusable `bid_scratch`/`ask_scratch` to `HotTask`, or add `LocalBook::apply_delta_levels(&[PriceLevel])` that parses and upserts directly.

4. **Eliminate quote ladder Vec churn. High latency, easy win.**
   [quotes.rs:50](/home/ubuntu/lighter_MM_RUST/src/strategy/quotes.rs:50) allocates `none_levels` unconditionally. [quotes.rs:86](/home/ubuntu/lighter_MM_RUST/src/strategy/quotes.rs:86) allocates levels. [quotes.rs:100](/home/ubuntu/lighter_MM_RUST/src/strategy/quotes.rs:100), [quotes.rs:131](/home/ubuntu/lighter_MM_RUST/src/strategy/quotes.rs:131), [quotes.rs:179](/home/ubuntu/lighter_MM_RUST/src/strategy/quotes.rs:179) clone/collect more Vecs. [order_manager.rs:114](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:114) allocates ops.
   Concrete change: use fixed `[Level; MAX_LEVELS]` or `SmallVec`, apply quality/inventory bias in place, and `collect_order_operations_into(&mut ops_scratch)`.

5. **Do not hold/smuggle a mutex through async rate sleeps. Correctness/cold-path contention.**
   [paced_send.rs:66](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:66)-[71](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:71) calls async `write_slot`; that function sleeps at [rate_limit.rs:351](/home/ubuntu/lighter_MM_RUST/src/exec/rate_limit.rs:351), [366](/home/ubuntu/lighter_MM_RUST/src/exec/rate_limit.rs:366), [378](/home/ubuntu/lighter_MM_RUST/src/exec/rate_limit.rs:378), [388](/home/ubuntu/lighter_MM_RUST/src/exec/rate_limit.rs:388).
   Concrete change: compute delay under lock, drop lock, sleep, re-check. Never hold `parking_lot::MutexGuard` across `.await`.

6. **Drain order events before quote throttling. Correctness/latency.**
   [orchestrator.rs:282](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:282)-[289](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:289) returns before draining events. Events are only drained at [orchestrator.rs:291](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:291)-[294](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:294), so order state can lag by `min_loop_interval`.
   Concrete change: drain `evt_rx` at the start of every book callback, before throttle. Add a max-drain budget plus pause/reconcile if exceeded.

7. **Actually read derived hot parameters. Correctness.**
   `Derived::base_amount()` exists at [shared.rs:212](/home/ubuntu/lighter_MM_RUST/src/shared.rs:212), but hot uses fixed `self.base_amount` at [orchestrator.rs:320](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:320). `VolObiCalculator` stores max position at [vol_obi.rs:66](/home/ubuntu/lighter_MM_RUST/src/strategy/vol_obi.rs:66), but `set_max_position_dollar` at [vol_obi.rs:204](/home/ubuntu/lighter_MM_RUST/src/strategy/vol_obi.rs:204) is never called.
   Concrete change: each quote cycle read derived base/max, update calc max before `quote`, and stop using fixed constructor values for live sizing/skew.

8. **Do not hardcode quote update threshold. Correctness/architecture.**
   [orchestrator.rs:320](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:320) passes `8.0`. Rate limiter has adaptive threshold at [rate_limit.rs:266](/home/ubuntu/lighter_MM_RUST/src/exec/rate_limit.rs:266), but hot never sees it.
   Concrete change: cold rate limiter publishes adaptive threshold to an atomic; hot reads it.

9. **LocalBook sorted Vec is cache-friendly but not free. Medium/high depending depth.**
   [local_book.rs:56](/home/ubuntu/lighter_MM_RUST/src/book/local_book.rs:56)-[63](/home/ubuntu/lighter_MM_RUST/src/book/local_book.rs:63) does `Vec::remove/insert` memmove. OBI then scans ranges at [local_book.rs:89](/home/ubuntu/lighter_MM_RUST/src/book/local_book.rs:89)-[105](/home/ubuntu/lighter_MM_RUST/src/book/local_book.rs:105).
   Concrete change: if book depth stays small, pre-reserve snapshot depth and keep it. If depth is 1k+, store integer tick keys and either cap to OBI-relevant band/top N or move to a tick-indexed/Fenwick range-sum structure. Also avoid `f64 == price` at [local_book.rs:53](/home/ubuntu/lighter_MM_RUST/src/book/local_book.rs:53); use raw tick `i64`.

10. **Lock-free atomics: mostly safe scalars, but commit ordering is inconsistent. Correctness nuance.**
   `SharedAlpha::update` commits via `sample_count` Release at [shared.rs:51](/home/ubuntu/lighter_MM_RUST/src/shared.rs:51), read Acquire at [shared.rs:61](/home/ubuntu/lighter_MM_RUST/src/shared.rs:61), but timestamp/alpha loads are Relaxed at [shared.rs:56](/home/ubuntu/lighter_MM_RUST/src/shared.rs:56), [66](/home/ubuntu/lighter_MM_RUST/src/shared.rs:66). Standx uses Acquire on staleness fields. Same issue in `SharedBbo` [shared.rs:111](/home/ubuntu/lighter_MM_RUST/src/shared.rs:111)-[133](/home/ubuntu/lighter_MM_RUST/src/shared.rs:133).
   Concrete change: use a single Release commit field per update, usually `last_update_ms`, and load it Acquire before payload. Keep `Derived` Relaxed only if mixed-epoch base/max/capital is acceptable; otherwise add a version seqlock.

11. **RollingStats is fine.**
   [rolling.rs:24](/home/ubuntu/lighter_MM_RUST/src/strategy/rolling.rs:24) allocates once; [rolling.rs:37](/home/ubuntu/lighter_MM_RUST/src/strategy/rolling.rs:37)-[75](/home/ubuntu/lighter_MM_RUST/src/strategy/rolling.rs:75) is steady-state allocation-free. Reverse Welford eviction looks correct.

12. **Live hot/cold boundary is not wired. Correctness.**
   [orchestrator.rs:106](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:106)-[108](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:108) creates mailbox/event channels, but no paced sender/account tasks are started. [orchestrator.rs:111](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:111)-[115](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:115) references `run_live`, but no such function exists.
   Concrete change: either wire live fully or make `Mode::Live` refuse to start. Silent no-send live mode is dangerous.

13. **Pin the hot task after the above fixes. Jitter reduction.**
   [main.rs:15](/home/ubuntu/lighter_MM_RUST/src/main.rs:15) uses default multithread Tokio; [orchestrator.rs:130](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:130) spawns hot like any other task.
   Concrete change: run market-data WS + `HotTask` on a dedicated OS thread/current-thread runtime, ideally core-pinned. Keep cold I/O/signing on the multithread runtime. Worth it for p99 after parse/alloc fixes, not before.