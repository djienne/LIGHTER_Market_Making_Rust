Not safe to run live yet.

**Must Fix**
1. [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:240) feeds every `account_orders` WS message into full reconcile, but Python says only the first message is a snapshot and later messages are incremental. [order_manager.rs](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:285) then clears tracked slots absent from that incremental message, making real resting orders locally `Idle` and eligible for duplicate creates.
Fix: distinguish first snapshot vs incremental; only full snapshots/REST go through full reconcile, incremental WS must emit targeted `BindLive`/`ClearLive`.

2. [order_manager.rs](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:280) `process_reconcile` only updates mappings, clears missing tracked ids, and refreshes already-tracked slots; it never binds idle slots and never cancels orphan exchange orders. Startup cancel-all + poller is not enough over 1 hour.
Fix: detect `remote_live_client_ids - tracked_client_ids`, pause, cancel those exchange `order_index` values, and enforce `max_live_orders_per_market`.

3. [paced_send.rs](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:149) `Unknown` pauses and notifies reconcile, but because reconcile cannot bind/cancel unknown live orders, an accepted create can remain orphaned until pause expires, then be duplicated.
Fix: on `Unknown`, mark reconcile failed and do not resume until REST/account snapshot proves zero or orphan cancels succeed.

4. [paced_send.rs](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:73) signs/reserves nonces before transport; [paced_send.rs](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:156) `NotSent` only logs. If WS connect fails before write, nonce is skipped; if REST `send_tx` errors after reaching server, the order can be orphaned.
Fix: `NotSent` must either REST-fallback the same signed tx/batch or rollback/hard-refresh all reserved nonces and suppress duplicate creates until reconcile.

5. [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:331) shutdown `cancel_all_orders` is not safe: it is outside the sender `sdk_lock`, detached `paced_send` can still finish a create after cancel-all, and [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:346) treats any REST response code as success.
Fix: stop/await sender or share the same write lock, require `code == 0 || code == 200`, retry with nonce refresh/backoff, then REST-verify no active orders before exit.

6. [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:150) only waits for `ctrl_c`; SIGTERM/service stop can bypass shutdown cancel-all.
Fix: handle SIGINT and SIGTERM and run the same bounded cancel-and-verify path.

7. [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:101) seeds live `max_pos_usd` to `1e12`; [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:535) uses that if capital has not arrived. Live can quote before `user_stats/account_all` readiness with position assumed zero.
Fix: in Live seed max position to `0`, wait for fresh capital and position snapshots, and suppress quoting while either feed is stale.

8. [orchestrator.rs](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:558) still uses fixed `self.base_amount`; Python recomputes/min-notional-normalizes size. With the stated ~$13 order size vs [config min_order_value_usd=14.5](/home/ubuntu/lighter_MM_RUST/config.json:12), this can cause avoidable rejects and nonce/rate-limit churn.
Fix: port/use dynamic base sizing and `normalize_live_order_size`, then collect with `derived.base_amount()`.

**Answered**
A: Yes, an accepted `Unknown`/ambiguous `NotSent` can become orphaned and later duplicated. `process_reconcile` only refreshes tracked slots; it does not bind idle slots or cancel unknown live orders.

B: No, shutdown cancel-all is not guaranteed and can leave resting orders.

C: Yes, nonce can desync: `NotSent` gaps nonces, generic/quota rejects do not rollback/hard-refresh, and shutdown cancel-all races the sender.

D: Reduce-only side/signing looks correct in isolation: [order_manager.rs](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:101), [signing.rs](/home/ubuntu/lighter_MM_RUST/src/exec/signing.rs:56), [signer.rs](/home/ubuntu/lighter_MM_RUST/src/lighter/signer.rs:232). The bound is still unsafe until feed readiness, orphan handling, and dynamic sizing are fixed.