//! Inbound array CRDT message handler for NodeDB-Lite.
//!
//! [`ArrayInbound`] receives wire messages from Origin and applies them to the
//! local engine state. It holds no socket: the sync transport decodes frames
//! and reaches it through `SyncDelegate`.
//!
//! # Module layout
//!
//! - [`apply`] — [`LiteApplyEngine`] adapts engine state to the
//!   [`nodedb_array::sync::apply::ApplyEngine`] trait.
//! - [`outcome`] — [`InboundOutcome`] enum returned by every dispatcher method.
//! - [`dispatcher`] — [`ArrayInbound`] struct, snapshot assembly buffer, and
//!   shared helpers.
//! - [`delta`], [`snapshot`], [`schema`], [`reject`] — one `impl ArrayInbound`
//!   block per wire-message family, with colocated unit tests.
//! - [`resume`] — the catch-up requests a new sync session sends, and tracker
//!   access for the apply paths.
//!
//! # No outbound loop
//!
//! [`LiteApplyEngine`] operates at the engine layer below
//! `NodeDbLite::array_put_cell`, so the Phase D outbound hook
//! ([`crate::sync::array::ArrayOutbound`]) is never invoked for remotely-
//! received ops. The receive path is therefore loop-free by construction.

pub mod apply;
pub mod delta;
pub mod dispatcher;
pub mod outcome;
pub mod reject;
pub mod resume;
pub mod schema;
pub mod snapshot;

#[cfg(test)]
pub(crate) mod fixtures;

pub use apply::LiteApplyEngine;
pub use dispatcher::ArrayInbound;
pub use outcome::InboundOutcome;
