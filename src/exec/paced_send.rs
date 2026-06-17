//! Paced order sender — port of `_paced_send` + `sign_and_send_batch`.
//!
//! Drains the freshest-wins quote-ops mailbox (`watch`), applies the rate-limit gate, signs
//! the batch (FFI in `spawn_blocking`), and sends via the tx WebSocket (REST free-slot
//! fallback when quota is exhausted). Results flow back to the decision task as LOSSLESS
//! `OrderEvent`s. The `Unknown` outcome NEVER triggers a same-batch retry — it pauses
//! trading, hard-refreshes the nonce, and requests reconciliation (codex invariant).

use crate::exec::rate_limit::{is_quota_error, is_transient_error, RateLimiter};
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
    pub derived: Arc<Derived>,
    pub risk: Arc<PMutex<RiskController>>,
    pub reconcile: Arc<Notify>,
    /// Serializes sign+send so the nonce sequence is atomic per batch (Python _sdk_write_lock).
    pub sdk_lock: Arc<tokio::sync::Mutex<()>>,
    /// Report order-state mutations back to the decision task (lossless).
    pub events: mpsc::UnboundedSender<OrderEvent>,
}

/// Run the sender loop (OWNS the rate limiter — it is the only task that touches it, so no
/// lock is needed and `write_slot` can await freely). Exits when the mailbox sender is dropped.
pub async fn run(ctx: SenderCtx, mut rate: RateLimiter, mut mailbox: watch::Receiver<Vec<BatchOp>>) {
    loop {
        if mailbox.changed().await.is_err() {
            break; // sender dropped -> shutdown
        }
        let ops = mailbox.borrow_and_update().clone();
        if ops.is_empty() {
            continue;
        }
        if ctx.risk.lock().is_paused() {
            continue;
        }
        if let Err(e) = send_once(&ctx, &mut rate, ops).await {
            tracing::warn!("paced_send: {e}");
        }
    }
}

fn cancel_only(ops: &[BatchOp]) -> bool {
    ops.iter().all(|o| o.action == OrderAction::Cancel)
}

async fn send_once(ctx: &SenderCtx, rate: &mut RateLimiter, ops: Vec<BatchOp>) -> anyhow::Result<()> {
    let cancel = cancel_only(&ops);
    let op_count = ops.len();

    // Rate-limit gate (owned limiter; safe to await).
    if !rate.write_slot(op_count, cancel).await {
        return Ok(());
    }

    // Serialize sign+send (atomic nonce window).
    let _guard = ctx.sdk_lock.lock().await;

    // Sign on a blocking thread (FFI is CPU-bound).
    let signer = ctx.signer.clone();
    let nonce = ctx.nonce.clone();
    let market = ctx.market.clone();
    let ops_for_sign = ops.clone();
    let signed = tokio::task::spawn_blocking(move || {
        sign_batch(&signer, &nonce, &market, &ops_for_sign)
    })
    .await??;

    // Send. Free-slot single-op REST path when quota is exhausted; else tx WebSocket batch.
    let quota = ctx.derived.quota();
    let free_slot = matches!(quota, Some(q) if q <= 0);
    let result = if free_slot && signed.tx_types.len() == 1 {
        match ctx.rest.send_tx(signed.tx_types[0], &signed.tx_infos[0]).await {
            Ok(tx) => crate::types::TxSendResult {
                status: if tx.code == 0 || tx.code == 200 { TxSendStatus::Ok } else { TxSendStatus::Rejected },
                code: tx.code,
                message: tx.message,
                quota_remaining: tx.volume_quota_remaining,
            },
            Err(e) => crate::types::TxSendResult::not_sent(format!("rest_err:{e}")),
        }
    } else {
        ctx.tx_ws.send_batch(&signed.tx_types, &signed.tx_infos).await
    };

    match result.status {
        TxSendStatus::Ok => {
            rate.record_ops_sent(op_count);
            if let Some(q) = result.quota_remaining {
                ctx.derived.set_quota(Some(q));
                rate.set_quota(Some(q));
            }
            ctx.risk.lock().record_success();
            rate.reset_global_backoff();
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
            ctx.risk.lock().record_rejection(&result.message);
            if is_quota_error(&result.message) {
                ctx.derived.set_quota(Some(0));
                rate.set_quota(Some(0));
            } else if is_transient_error(&result.message) {
                rate.trigger_global_backoff();
                let _ = ctx.nonce.hard_refresh(&ctx.rest).await;
            }
            tracing::warn!("order rejected: code={} msg={}", result.code, result.message);
        }
        TxSendStatus::Unknown => {
            // A frame may have reached the exchange — do NOT retry. Pause + refresh + reconcile.
            tracing::warn!("tx unknown outcome ({}); pausing + reconciling", result.message);
            ctx.risk.lock().trigger_pause("unknown_tx_outcome");
            let _ = ctx.nonce.hard_refresh(&ctx.rest).await;
            ctx.reconcile.notify_one();
        }
        TxSendStatus::NotSent => {
            tracing::debug!("tx not sent ({}); will retry next cycle", result.message);
        }
    }
    Ok(())
}

// silence unused import in case Side isn't referenced after edits
#[allow(unused_imports)]
use Side as _Side;
