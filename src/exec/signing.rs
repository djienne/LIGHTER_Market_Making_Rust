//! Bridge BatchOps -> signed transactions via the native signer (port of `_sign_ops_sync`).
//! Fully synchronous (atomic nonce + FFI) — call from `spawn_blocking`. On any sign error
//! the reserved nonce is rolled back, mirroring `acknowledge_failure`.

use crate::lighter::nonce::NonceManager;
use crate::lighter::signer::{Signer, ORDER_TYPE_LIMIT, TIF_POST_ONLY};
use crate::types::{BatchOp, MarketConfig, OrderAction};
use crate::util::to_raw;
use anyhow::{bail, Result};

/// Output of signing a batch: parallel arrays for the wire plus the signed ops (so the
/// caller can update order state / enqueue BindLive on success).
pub struct SignedBatch {
    pub tx_types: Vec<u8>,
    pub tx_infos: Vec<String>,
    pub ops: Vec<BatchOp>,
}

/// Sign every op in order. A single failure rolls back its nonce and aborts the batch
/// (matches the Python which rolls back and stops on the first sign error).
pub fn sign_batch(
    signer: &Signer,
    nonce: &NonceManager,
    market: &MarketConfig,
    ops: &[BatchOp],
) -> Result<SignedBatch> {
    let aki = nonce.api_key_index();
    let mut tx_types = Vec::with_capacity(ops.len());
    let mut tx_infos = Vec::with_capacity(ops.len());
    let mut signed = Vec::with_capacity(ops.len());

    // Number of nonces reserved so far this batch. On ANY abort we roll back ALL of them
    // (nothing was sent), so the local nonce counter stays consistent (codex review).
    let mut reserved: i64 = 0;
    macro_rules! rollback {
        () => {{
            for _ in 0..reserved {
                nonce.acknowledge_failure();
            }
        }};
    }

    for op in ops {
        let n = nonce.next();
        reserved += 1;
        let res = match op.action {
            OrderAction::Create => {
                let price_raw = to_raw(op.price, market.price_tick);
                // The create tx struct field `Price` is uint32 — guard before the c_int cast
                // so a high raw price can never wrap negative.
                if !(0..=u32::MAX as i64).contains(&price_raw) {
                    rollback!();
                    bail!("create price raw {price_raw} outside u32 range");
                }
                let amount_raw = to_raw(op.size, market.amount_tick);
                signer.sign_create_order(
                    market.market_id as i32,
                    op.client_order_id,
                    amount_raw,
                    (price_raw as u32) as i32, // preserve the u32 bit-pattern through c_int
                    op.side.is_ask(),
                    ORDER_TYPE_LIMIT,
                    TIF_POST_ONLY,
                    op.reduce_only,
                    0,  // trigger
                    -1, // 28-day expiry
                    n,
                    aki,
                )
            }
            OrderAction::Modify => {
                let exchange_id = match op.exchange_id {
                    Some(e) => e,
                    None => {
                        rollback!();
                        bail!("modify op missing exchange_id (client {})", op.client_order_id);
                    }
                };
                let price_raw = to_raw(op.price, market.price_tick);
                let amount_raw = to_raw(op.size, market.amount_tick);
                signer.sign_modify_order(market.market_id as i32, exchange_id, amount_raw, price_raw, 0, n, aki)
            }
            OrderAction::Cancel => {
                let exchange_id = match op.exchange_id {
                    Some(e) => e,
                    None => {
                        rollback!();
                        bail!("cancel op missing exchange_id (client {})", op.client_order_id);
                    }
                };
                signer.sign_cancel_order(market.market_id as i32, exchange_id, n, aki)
            }
        };

        match res {
            Ok(tx) => {
                tx_types.push(tx.tx_type);
                tx_infos.push(tx.tx_info);
                signed.push(op.clone());
            }
            Err(e) => {
                rollback!();
                bail!("sign {:?} failed: {e}", op.action);
            }
        }
    }

    Ok(SignedBatch { tx_types, tx_infos, ops: signed })
}
