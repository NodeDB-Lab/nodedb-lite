//! WebSocket transport — the runtime side of Lite ↔ Origin sync.
//!
//! Public surface is intentionally tiny: callers spawn [`run_sync_loop`]
//! once, after constructing a [`SyncDelegate`](crate::sync::SyncDelegate) that bridges the running
//! `NodeDbLite` to the transport's read/write callbacks. Everything else
//! (handshake, dispatch, per-engine push, ping keepalive) is private.
//!
//! Compiled only with the `sync` feature. The `SyncDelegate` trait lives in
//! [`crate::sync::delegate`] and compiles without it.
//!
//! Module map:
//!
//! - `connect`   — single-attempt connect + handshake
//! - `dispatch`  — inbound frame receive loop and message dispatch table
//! - `dispatch_kv` — `KvPushAck` handling
//! - `push`      — outbound delta + per-engine push loops, plus ping keepalive
//! - `run`       — the reconnecting sync loop

mod connect;
mod dispatch;
mod dispatch_acks;
mod dispatch_kv;
mod push;
mod run;

pub use run::run_sync_loop;
