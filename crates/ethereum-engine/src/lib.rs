//! # Ethereum Engine (Layer 1)
//!
//! Settlement Engine which performs settlements to Ethereumn (or
//! Ethereum Virtual Machine-compatible)
//!
//! The engine connects to an Ethereum JSON-RPC endpoint (over HTTP) as well as the connector. Its
//! functions are exposed via the Settlement Engine API.
//!
//! It requires a `confirmations` security parameter which is used to ensure
//! that all transactions that get sent to the connector have sufficient
//! confirmations (suggested value: >6)
//!
//! All settlements made with this engine make on-chain Layer 1 Ethereum
//! transactions. This engine DOES NOT support payment channels.

//! **This API MUST NOT be
//! exposed to the network, sinc it would allow an attacker to perform arbitrary
//! settlements towards addresses of their choice.**
#![recursion_limit = "128"]

pub mod backends;
pub mod engine;
pub mod utils;
