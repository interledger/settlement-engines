//! # Interledger Settlement Engines
//!
//! Crate containing all the components for implementing the Settlement
//! Architecture for the Interledger Protocol. The crate is structured such that
//! an API is created by giving it an object which implements the
//! SettlementEngine trait. All settlement engines must be implemented under the
//! `engines` subdirectory, with a directory name describing their
//! functionality, e.g. ethereum_ledger, ethereum_unidirectional_channel,
//! xrp_ledger, etc.
#![recursion_limit = "128"]

mod backends;
pub mod engine;
pub mod utils;
#[cfg(feature = "redis")]
pub use engine::run_ethereum_engine;
