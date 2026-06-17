**Findings**

1. [orchestrator.rs:121](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:121), [orchestrator.rs:356](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:356): `Mode::Live` drops `_ops_rx` and never spawns `paced_send`, account WS, or reconcile; live ops go nowhere. Fix: keep the receiver and wire sender/account_orders/account_all/user_stats/reconcile before enabling Live.

2. [paced_send.rs:44](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:44), [paced_send.rs:66](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:66): sender clones ops before the pacing wait, unlike Python’s post-wait freshest pull; stale prices/stale creates can be sent. Fix: re-borrow mailbox after `write_slot`, then drop stale creates against current slot state before signing.

3. [paced_send.rs:90](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:90): quota=0 free-slot path only uses REST if the batch already has exactly one tx; multi-op batches still go WS after the 15s wait. Fix: sort cancel/reducing first, truncate to one op, send via REST `sendTx`.

4. [paced_send.rs:156](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:156): `NotSent` after WS connect failure is not REST-fallbacked and the consumed watch value may never retry. Fix: on `NotSent`, send the same signed batch via REST `sendTxBatch`; reserve `Unknown` for no-retry pause/reconcile.

5. [signing.rs:32](/home/ubuntu/lighter_MM_RUST/src/exec/signing.rs:32), [signing.rs:83](/home/ubuntu/lighter_MM_RUST/src/exec/signing.rs:83): sign failure aborts the whole batch after earlier nonces may have been reserved but not sent. Fix: match Python: rollback only the failed op and continue, or rollback/hard-refresh all reserved nonces on abort.

6. [paced_send.rs:138](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:138): rejected/quota/nonce responses do not rollback/hard-refresh nonces like Python. Fix: implement Python’s per-class handling: quota/generic rollback signed nonces + hard refresh, nonce hard refresh, 429 rollback/backoff.

7. [paced_send.rs:107](/home/ubuntu/lighter_MM_RUST/src/exec/paced_send.rs:107): rate window records ops only on `Ok`; definite `Rejected` and possible-written `Unknown` sends are invisible to local 40/60 pacing. Fix: count signed ops when a REST/WS frame is actually attempted; do not count only true `NotSent`.

8. [order_manager.rs:273](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:273): reconcile only clears/refreshes tracked slots; it never detects/cancels orphan exchange orders or enforces max live order count. Fix: return unknown exchange ids and enqueue cancel-only ops as Python does.

9. [messages.rs:82](/home/ubuntu/lighter_MM_RUST/src/lighter/messages.rs:82), [order_manager.rs:275](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:275): `status=None` is treated non-live; REST `accountActiveOrders` rows can be active without a status, so reconcile can clear all slots and duplicate orders. Fix: for active-order snapshots, default missing status to live or prefilter only WS snapshots.

10. [order_manager.rs:226](/home/ubuntu/lighter_MM_RUST/src/exec/order_manager.rs:226): `ClearLive` ignores `client_order_id`; a late clear for old order A can clear new order B in the same slot. Fix: clear only when the slot’s current id matches the event id.

11. [orchestrator.rs:243](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:243): market-data disconnect callback says reset but does nothing; reconnect deltas can apply to a stale book/vol window. Fix: reset `LocalBook`, `VolObiCalculator`, `mid`, and offset on disconnect.

12. [orchestrator.rs:320](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:320), [orchestrator.rs:323](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:323): `max_pos_usd` is read but not pushed into `VolObiCalculator` before `quote()`, so L0 inventory skew can use stale/unlimited risk. Fix: call `set_max_position_dollar(max_pos_usd)` before `quote`.

13. [orchestrator.rs:94](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:94), [orchestrator.rs:336](/home/ubuntu/lighter_MM_RUST/src/orchestrator.rs:336): live base size/risk uses fixed startup `base_amount` and seeded `max_pos_usd=1e12`, not Python’s capital/min-quote recompute. Fix: drive `derived.base_amount/max_pos_usd` from user_stats+mid and use `derived.base_amount()` in collect.

14. [binance/obi.rs:31](/home/ubuntu/lighter_MM_RUST/src/binance/obi.rs:31): Binance reconnect reset clears local OBI stats but not `SharedAlpha`, so old alpha remains usable until stale timeout. Fix: add `SharedAlpha::reset()` and call it on depth reset.

15. [signing.rs:36](/home/ubuntu/lighter_MM_RUST/src/exec/signing.rs:36), [signing.rs:42](/home/ubuntu/lighter_MM_RUST/src/exec/signing.rs:42): create price raw is blindly cast to `i32`; SDK create struct uses `uint32`, so high raw prices can wrap negative. Fix: checked `u32` range and pass an ABI-compatible value, failing closed on overflow.