//! [`InboundOutcome`] — return type for every [`super::ArrayInbound`]
//! dispatcher method.

use nodedb_array::sync::apply::ApplyRejection;

/// Outcome returned by each [`super::ArrayInbound`] handler.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundOutcome {
    /// The op was applied to local engine state.
    Applied,
    /// The op was already present; no state was changed (idempotent replay).
    Idempotent,
    /// The op was rejected by the local apply engine.
    Rejected(ApplyRejection),
    /// A snapshot chunk was buffered; more chunks are expected.
    SnapshotPartial {
        /// Number of chunks received so far (including this one).
        received: u32,
        /// Total chunks declared in the snapshot header.
        total: u32,
    },
    /// A snapshot was fully assembled and all contained ops applied.
    SnapshotApplied {
        /// Number of ops applied from the assembled snapshot.
        ops_applied: u64,
    },
    /// A schema CRDT snapshot was imported into the local registry.
    SchemaImported,
    /// A reject message was processed and the offending op removed from the
    /// pending queue.
    RejectAcknowledged,
}
