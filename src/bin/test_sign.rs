//! P0 gate: verify the native signer FFI works and produces the SAME signature as the
//! Python SDK for identical fixed inputs (deterministic given key + fields + nonce).
//!
//! Usage:
//!   cargo run --bin test_sign -- [path/to/.env]
//! Reads API_KEY_PRIVATE_KEY, ACCOUNT_INDEX, API_KEY_INDEX from the env file (default
//! /home/ubuntu/lighter_MM/.env). Signs a fixed CreateOrder OFFLINE (no network, not
//! sent) and prints tx_type / tx_hash / tx_info for cross-checking against Python.

use anyhow::{Context, Result};
use lighter_mm::lighter::signer::{Signer, ORDER_TYPE_LIMIT, TIF_POST_ONLY};
use std::path::{Path, PathBuf};

const BASE_URL: &str = "https://mainnet.zklighter.elliot.ai";

// Fixed, deterministic inputs — must match the Python parity script exactly.
const MARKET_INDEX: i32 = 1;
const CLIENT_ORDER_INDEX: i64 = 1;
const BASE_AMOUNT: i64 = 1000;
const PRICE: i32 = 1_000_000;
const NONCE: i64 = 12345;

fn main() -> Result<()> {
    let env_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/home/ubuntu/lighter_MM/.env".to_string());
    let _ = dotenvy::from_path(&env_path);

    let api_priv = std::env::var("API_KEY_PRIVATE_KEY")
        .context("API_KEY_PRIVATE_KEY not set (pass an .env path)")?;
    let account_index: i64 = std::env::var("ACCOUNT_INDEX")
        .unwrap_or_else(|_| "0".into())
        .trim()
        .parse()
        .context("ACCOUNT_INDEX parse")?;
    let api_key_index: i32 = std::env::var("API_KEY_INDEX")
        .unwrap_or_else(|_| "0".into())
        .trim()
        .parse()
        .context("API_KEY_INDEX parse")?;

    let signers_dir = signers_dir();
    println!("signers_dir = {}", signers_dir.display());
    println!("account_index = {account_index}, api_key_index = {api_key_index}");

    let signer = Signer::load(&signers_dir, BASE_URL, &api_priv, api_key_index, account_index)
        .context("Signer::load")?;
    println!("CreateClient OK");

    let tx = signer
        .sign_create_order(
            MARKET_INDEX,
            CLIENT_ORDER_INDEX,
            BASE_AMOUNT,
            PRICE,
            false, // is_ask = bid
            ORDER_TYPE_LIMIT,
            TIF_POST_ONLY,
            false, // reduce_only
            0,     // trigger_price
            -1,    // order_expiry (28d default)
            NONCE,
            api_key_index,
        )
        .context("sign_create_order")?;

    println!("--- RUST SIGNED TX ---");
    println!("tx_type = {}", tx.tx_type);
    println!("tx_hash = {}", tx.tx_hash);
    println!("tx_info = {}", tx.tx_info);
    Ok(())
}

fn signers_dir() -> PathBuf {
    // Project-local signers/ next to Cargo.toml.
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("signers");
    if p.exists() {
        return p;
    }
    Path::new("signers").to_path_buf()
}
