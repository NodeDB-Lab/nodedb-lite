//! WebSocket sync client for edge ↔ Origin communication.

mod config;
mod delta;
mod handshake;
mod maintenance;
mod receive;
mod state;
mod token;

pub use config::{SyncConfig, SyncState, TokenProvider};
pub use state::SyncClient;
