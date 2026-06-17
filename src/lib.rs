//! lighter-mm: high-performance Rust market maker for the Lighter perpetuals exchange.
//!
//! Module layout mirrors PLAN.md: a synchronous, lock-free hot path (market data ->
//! signal -> quote -> order ops) and an async cold path (signing, sending,
//! reconciliation, accounting). Built up in phases; see PLAN.md.

pub mod account;
pub mod binance;
pub mod book;
pub mod config;
pub mod exec;
pub mod lighter;
pub mod logging;
pub mod metrics;
pub mod orchestrator;
pub mod risk;
pub mod shared;
pub mod strategy;
pub mod types;
pub mod util;
