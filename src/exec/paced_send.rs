//! Paced order sender — port of `_paced_send` + `sign_and_send_batch`.
//!
//! Drains the freshest-wins quote-ops mailbox (`watch`), applies the rate-limit gate, signs
//! the batch (FFI in `spawn_blocking`), and sends via the tx WebSocket (REST free-slot
//! fallback when quota is exhausted). Results flow back to the decision task as LOSSLESS
//! `OrderEvent`s. The `Unknown` outcome NEVER triggers a same-batch retry — it pauses
//! trading, hard-refreshes the nonce, and requests reconciliation (codex invariant).

use crate::exec::rate_limit::{classify_reject, RateLimiter, RejectKind};
use crate::exec::signing::sign_batch;
use crate::lighter::nonce::NonceManager;
use crate::lighter::rest::RestClient;
use crate::lighter::signer::Signer;
use crate::lighter::tx_ws::TxWebSocket;
use crate::risk::RiskController;
use crate::shared::Derived;
use crate::types::{BatchOp, MarketConfig, OrderAction, OrderEvent, Side, TxSendStatus};
use parking_lot::Mutex as PMutex;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Notify};

pub struct SenderCtx {
    pub signer: Arc<Signer>,
    pub nonce: Arc<NonceManager>,
    pub rest: Arc<RestClient>,
    pub tx_ws: Arc<TxWebSocket>,
    pub market: MarketConfig,
    pub account_index: i64,
    pub derived: Arc<Derived>,
    pub risk: Arc<PMutex<RiskController>>,
    /// Set by shutdown to stop placing NEW orders before cancel-all runs.
    pub halt: Arc<std::sync::atomic::AtomicBool>,
    /// True iff the optimistic nonce is believed in-sync with the exchange. Set false when a
    /// MANDATORY hard_refresh exhausts its retries (REST down) — the sender then refuses to place
    /// new orders even if the pause cooldown elapses, and the reconcile poller re-syncs the nonce
    /// + clears this once REST is reachable (codex: never resume on a known-bad nonce).
    pub nonce_ok: Arc<std::sync::atomic::AtomicBool>,
    pub reconcile: Arc<Notify>,
    /// Serializes sign+send so the nonce sequence is atomic per batch (Python _sdk_write_lock).
    pub sdk_lock: Arc<tokio::sync::Mutex<()>>,
    /// Report order-state mutations back to the decision task (lossless).
    pub events: mpsc::UnboundedSender<OrderEvent>,
}

/// Run the sender loop (OWNS the rate limiter — it is the only task that touches it, so no
/// lock is needed and `write_slot` can await freely). Exits when the mailbox sender is dropped.
pub async fn run(
    ctx: SenderCtx,
    mut rate: RateLimiter,
    mut mailbox: watch::Receiver<Vec<BatchOp>>,
) {
    use std::sync::atomic::Ordering::SeqCst;
    // Periodic wake so pause-recovery (maybe_recover) runs even when NO new ops arrive — Python
    // attempts recovery every loop iteration. Without this, a quiet mailbox (no quote changes)
    // could leave us paused past the cooldown indefinitely (codex).
    let mut recover_tick = tokio::time::interval(std::time::Duration::from_millis(500));
    recover_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let got_ops = tokio::select! {
            r = mailbox.changed() => match r { Ok(()) => true, Err(_) => break },
            _ = recover_tick.tick() => false,
        };

        // Attempt pause-recovery on EVERY wake (ops OR tick), independent of order flow. Resumption
        // is gated on a successful post-pause reconcile (last_reconcile_ok) AND a fresh market-data
        // feed (ws_healthy) — NOT merely the cooldown deadline — AND a trusted nonce.
        let ws_healthy = ctx.derived.md_age_ms() < MD_HEALTH_MS;
        let nonce_ok = ctx.nonce_ok.load(SeqCst);
        let can_trade = ctx.risk.lock().maybe_recover(ws_healthy) && nonce_ok;

        if !got_ops {
            continue; // tick-only wake: recovery attempted, nothing new to send
        }
        let ops = mailbox.borrow_and_update().clone();
        if ops.is_empty() {
            continue;
        }
        if ctx.halt.load(SeqCst) {
            continue; // shutting down — do not place new orders
        }
        // While not allowed to trade, place NO new orders but still let cancel-only batches
        // through; reset the dropped creates so they are not stuck pending.
        if !can_trade && !cancel_only(&ops) {
            clear_failed_creates(&ctx, &ops);
            continue;
        }
        if let Err(e) = send_once(&ctx, &mut rate, ops).await {
            tracing::warn!("paced_send: {e}");
        }
    }
}

/// Market-data is "healthy" for pause-recovery if a book update arrived within this window. BTC
/// order-book updates many times/sec, so a healthy feed is always well under this; a larger gap
/// means the market-data WS is down and we must not resume trading (Python check_websocket_health).
const MD_HEALTH_MS: u64 = 10_000;

/// Attempts to re-sync the nonce from the API on a path where a correct nonce is MANDATORY.
const NONCE_RESYNC_ATTEMPTS: usize = 3;

fn cancel_only(ops: &[BatchOp]) -> bool {
    ops.iter().all(|o| o.action == OrderAction::Cancel)
}

/// Re-sync the optimistic nonce from the API, retrying briefly. If it cannot be re-synced (REST
/// down), PAUSE trading and mark the reconcile failed so the sender stops firing batches with a
/// known-bad counter — which would restart the invalid-nonce cascade — until a later successful
/// reconcile + the cooldown clear the pause. Returns true iff the nonce was re-synced.
/// (codex: a `let _ = hard_refresh(...)` that silently ignores failure is unsafe on these paths.)
async fn resync_nonce_or_pause(ctx: &SenderCtx, reason: &str) -> bool {
    use std::sync::atomic::Ordering::SeqCst;
    for attempt in 1..=NONCE_RESYNC_ATTEMPTS {
        match ctx.nonce.hard_refresh(&ctx.rest).await {
            Ok(()) => {
                ctx.nonce_ok.store(true, SeqCst);
                return true;
            }
            Err(e) => {
                tracing::warn!("nonce hard_refresh attempt {attempt}/{NONCE_RESYNC_ATTEMPTS} failed ({reason}): {e}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    tracing::error!(
        "nonce re-sync FAILED ({reason}); pausing trading + blocking resume until re-synced"
    );
    // Mark the nonce untrustworthy so the sender refuses NEW orders until the poller re-syncs it,
    // and pause so we stop firing batches with a known-bad counter (which restarts the cascade).
    ctx.nonce_ok.store(false, SeqCst);
    {
        let mut r = ctx.risk.lock();
        r.mark_reconcile(false, "nonce_resync_failed");
        r.trigger_pause("nonce_resync_failed");
    }
    false
}

/// Reset slots for CREATE ops that failed to send (they were marked pending pre-send by the
/// hot task). The hot task will then retry them. Modifies/cancels are left for reconcile.
fn clear_failed_creates(ctx: &SenderCtx, ops: &[BatchOp]) {
    for op in ops {
        if op.action == OrderAction::Create {
            let _ = ctx.events.send(OrderEvent::ClearLive {
                side: op.side,
                level: op.level,
                client_order_id: op.client_order_id,
            });
        }
    }
}

async fn send_once(
    ctx: &SenderCtx,
    rate: &mut RateLimiter,
    ops: Vec<BatchOp>,
) -> anyhow::Result<()> {
    let cancel = cancel_only(&ops);
    let op_count = ops.len();

    // Rate-limit gate (owned limiter; safe to await). If we skip, reset the marked-pending
    // creates so the hot task retries them rather than leaving stuck slots.
    //
    // NOTE (parity, deliberate): Python "peeks for pacing then pulls the freshest batch after the
    // wait". We intentionally send the batch we gated on instead. The hot task marks each emitted
    // batch's slots pending PRE-send (the ≤4 dup-prevention keystone), so mark↔send must stay
    // coherent: re-borrowing the freshest would leave the gated batch's unique slots marked-but-
    // unsent, and a skip-and-defer would starve under fast republish. The cost is at most ~one
    // send-interval (≤0.15s) of staleness on a continuously-requoting maker — negligible — in
    // exchange for keeping the duplicate-create guarantee. (codex P2: accepted as not-worse.)
    if !rate.write_slot(op_count, cancel).await {
        clear_failed_creates(ctx, &ops);
        return Ok(());
    }

    // Serialize sign+send (atomic nonce window).
    let _guard = ctx.sdk_lock.lock().await;
    // Re-check halt AFTER acquiring the lock: if shutdown set halt while we slept in the rate
    // gate, abort now so we can't send after shutdown's cancel-all verified a flat book.
    if ctx.halt.load(std::sync::atomic::Ordering::SeqCst) {
        clear_failed_creates(ctx, &ops);
        return Ok(());
    }

    // Sign on a blocking thread (FFI is CPU-bound). On sign failure, reset marked creates.
    let signer = ctx.signer.clone();
    let nonce = ctx.nonce.clone();
    let market = ctx.market.clone();
    let ops_for_sign = ops.clone();
    let signed = match tokio::task::spawn_blocking(move || {
        sign_batch(&signer, &nonce, &market, &ops_for_sign)
    })
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            clear_failed_creates(ctx, &ops);
            return Err(e);
        }
        Err(e) => {
            clear_failed_creates(ctx, &ops);
            return Err(e.into());
        }
    };

    // Send. Free-slot single-op REST path when quota is exhausted; else tx WebSocket batch.
    let quota = ctx.derived.quota();
    let free_slot = matches!(quota, Some(q) if q <= 0);
    let result = if free_slot && signed.tx_types.len() == 1 {
        match ctx
            .rest
            .send_tx(signed.tx_types[0], &signed.tx_infos[0])
            .await
        {
            Ok(tx) => crate::types::TxSendResult {
                status: if tx.code == 0 || tx.code == 200 {
                    TxSendStatus::Ok
                } else {
                    TxSendStatus::Rejected
                },
                code: tx.code,
                message: tx.message,
                quota_remaining: tx.volume_quota_remaining,
            },
            // A REST transport error AFTER the request was written is ambiguous — the tx may
            // have reached the exchange. Treat as Unknown (pause+cancel-all+reconcile), not NotSent.
            Err(e) => crate::types::TxSendResult::unknown(format!("rest_err:{e}")),
        }
    } else {
        ctx.tx_ws
            .send_batch(&signed.tx_types, &signed.tx_infos)
            .await
    };

    // Nonces advanced at sign time, one per signed op. On any non-success outcome we must
    // correct the optimistic local nonce or the NEXT batch is "ahead" and the exchange rejects
    // it ("invalid nonce") — a self-sustaining cascade. This is the single-instance keystone fix.
    let reserved = signed.ops.len();
    match result.status {
        TxSendStatus::Ok => {
            rate.record_ops_sent(op_count);
            if let Some(q) = result.quota_remaining {
                ctx.derived.set_quota(Some(q));
                rate.set_quota(Some(q));
                if q <= 20 {
                    tracing::warn!("low volume quota remaining after send: quota={q}");
                }
            }
            ctx.risk.lock().record_success();
            rate.reset_global_backoff();
            tracing::info!(
                "SENT {} ops (quota={:?}): {}",
                op_count,
                result.quota_remaining,
                signed
                    .ops
                    .iter()
                    .map(|o| format!("{:?}/{}@{:.1}", o.action, o.side, o.price))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            // Optimistic state updates: bind creates/modifies live, clear cancels.
            for op in &signed.ops {
                match op.action {
                    OrderAction::Create | OrderAction::Modify => {
                        let _ = ctx.events.send(OrderEvent::BindLive {
                            side: op.side,
                            level: op.level,
                            client_order_id: op.client_order_id,
                            exchange_id: op.exchange_id,
                            price: op.price,
                            size: op.size,
                        });
                    }
                    OrderAction::Cancel => {
                        let _ = ctx.events.send(OrderEvent::ClearLive {
                            side: op.side,
                            level: op.level,
                            client_order_id: op.client_order_id,
                        });
                    }
                }
            }
        }
        TxSendStatus::Rejected => {
            // Python classifies on `message or f"code={code}"` — an empty message with a
            // meaningful code (e.g. 429) must still be classified by the code (codex).
            let err_msg = if result.message.trim().is_empty() {
                format!("code={}", result.code)
            } else {
                result.message.clone()
            };
            ctx.risk.lock().record_rejection(&err_msg);
            tracing::warn!("order rejected: code={} msg={}", result.code, err_msg);
            // Correct the nonce per the reject class, mirroring the Python batch-reject handler
            // (`sign_and_send_batch`, quota → 429 → nonce → other) EXACTLY. Where a refresh is
            // mandatory, a persistent refresh failure PAUSES trading rather than continuing with a
            // known-bad counter (resync_nonce_or_pause) — silently ignoring it restarts the cascade.
            match classify_reject(&err_msg) {
                RejectKind::Quota => {
                    // Quota exhausted at the gateway: nonces not consumed. Roll back + resync.
                    ctx.derived.set_quota(Some(0));
                    rate.set_quota(Some(0));
                    ctx.nonce.rollback(reserved);
                    resync_nonce_or_pause(ctx, "reject_quota").await;
                }
                RejectKind::RateLimit => {
                    // 429: gateway rejected before consuming nonce → local rollback only (avoid
                    // hammering REST while rate-limited, matching the Python which does not refresh).
                    rate.trigger_global_backoff();
                    ctx.nonce.rollback(reserved);
                }
                RejectKind::Nonce => {
                    // Already desynced: re-sync from the API (authoritative). MUST succeed or we
                    // pause — there is no rollback here, so a failed refresh leaves a bad counter.
                    resync_nonce_or_pause(ctx, "reject_nonce").await;
                }
                RejectKind::Other => {
                    // Business rejection (post-only cross, margin, ...): consumption is ambiguous,
                    // so roll back the reservation AND resync from the API to be safe.
                    ctx.nonce.rollback(reserved);
                    resync_nonce_or_pause(ctx, "reject_other").await;
                }
            }
            // Reset slots for failed CREATEs so the hot task retries them (marked pending pre-send).
            // Modifies/cancels keep state; reconcile fixes them.
            clear_failed_creates(ctx, &signed.ops);
            // If consecutive rejections tripped the circuit breaker (or a refresh failure paused
            // us), pull ALL resting quotes now (we still hold sdk_lock, so this is serialized).
            if ctx.risk.lock().is_paused() {
                crate::orchestrator::cancel_all_and_verify(
                    &ctx.signer,
                    &ctx.nonce,
                    &ctx.rest,
                    ctx.nonce.api_key_index(),
                    ctx.account_index,
                    ctx.market.market_id,
                )
                .await;
            }
        }
        TxSendStatus::Unknown => {
            // A frame may have reached the exchange — do NOT retry. Re-sync the nonce from the API
            // FIRST (the frame may or may not have consumed it), then mark reconcile failed + pause
            // (blocks resume until a later reconcile re-confirms flat) + VERIFIED cancel-all
            // (retries + REST-confirms zero; we hold sdk_lock so it is serialized) + reconcile.
            // Mirrors `_handle_unknown_tx_outcome` (mark_reconcile(false) + hard-refresh + pause).
            tracing::warn!(
                "tx unknown outcome ({}); resync nonce + pause + verified cancel-all + reconcile",
                result.message
            );
            resync_nonce_or_pause(ctx, "tx_unknown").await;
            {
                let mut r = ctx.risk.lock();
                r.mark_reconcile(false, "tx_unknown_outcome");
                r.trigger_pause("unknown_tx_outcome");
            }
            crate::orchestrator::cancel_all_and_verify(
                &ctx.signer,
                &ctx.nonce,
                &ctx.rest,
                ctx.nonce.api_key_index(),
                ctx.account_index,
                ctx.market.market_id,
            )
            .await;
            ctx.reconcile.notify_one();
        }
        TxSendStatus::NotSent => {
            // No frame was written → the reserved nonces were definitely NOT consumed. Roll them
            // back locally (no REST round-trip) and let the hot task retry the failed creates.
            tracing::warn!(
                "tx not sent ({}); rolling back {} reserved nonce(s)",
                result.message,
                reserved
            );
            ctx.nonce.rollback(reserved);
            clear_failed_creates(ctx, &signed.ops);
        }
    }
    Ok(())
}

// silence unused import in case Side isn't referenced after edits
#[allow(unused_imports)]
use Side as _Side;
