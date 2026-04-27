//! Array CRDT synchronization for NodeDB-Lite.
//!
//! This module implements the embedded side of the sparse N-dimensional array
//! sync protocol. The design has three independent paths:
//!
//! 1. **Outbound** — local writes call [`ArrayOutbound`], which appends the op
//!    to a durable redb-backed [`RedbOpLog`] and enqueues it on
//!    [`PendingQueue`] for the transport layer to drain. The send path is
//!    fire-and-forget from the engine's perspective.
//!
//! 2. **Inbound** — wire messages from Origin enter [`ArrayInbound`], which
//!    deduplicates by HLC against the local op-log, applies the op via
//!    [`LiteApplyEngine`], and persists the resulting state. The receive path
//!    deliberately does *not* re-trigger outbound hooks, preventing echo
//!    loops by construction.
//!
//! 3. **Catch-up** — [`CatchupTracker`] persists the highest applied HLC per
//!    array so that, on reconnect, the transport can request only the ops it
//!    missed.
//!
//! Cross-cutting state:
//! - [`ReplicaState`] — stable replica identity + monotonic HLC generator.
//! - [`SchemaRegistry`] — per-array [`SchemaDoc`] snapshots, persisted as
//!   Loro documents.
//!
//! Backpressure is enforced at the [`PendingQueue`] (100k-op cap); overflow
//! surfaces as [`crate::error::LiteError::Backpressure`].
//!
//! Transport wiring (WebSocket/HTTP dispatch into `ArrayInbound` and out of
//! `PendingQueue`) is handled by a separate layer; this module is transport-
//! agnostic and exercised in tests by hand-constructed wire messages.

pub mod ack_sender;
pub mod catchup;
pub mod inbound;
pub mod op_log_redb;
pub mod outbound;
pub mod pending;
pub mod replica_state;
pub mod schema_registry;

pub use ack_sender::spawn as spawn_ack_sender;
pub use catchup::CatchupTracker;
pub use inbound::{ArrayInbound, InboundOutcome, LiteApplyEngine};
pub use op_log_redb::RedbOpLog;
pub use outbound::ArrayOutbound;
pub use pending::PendingQueue;
pub use replica_state::ReplicaState;
pub use schema_registry::SchemaRegistry;
