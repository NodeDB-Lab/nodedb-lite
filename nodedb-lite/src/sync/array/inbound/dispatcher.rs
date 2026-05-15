//! [`ArrayInbound`] — dispatcher for inbound array CRDT wire messages.
//!
//! This file holds the struct, the constructor, the snapshot-assembly buffer,
//! and the small shared helpers used by the per-message-family `impl` blocks
//! in sibling modules ([`super::delta`], [`super::snapshot`], [`super::schema`],
//! [`super::reject`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use nodedb_array::sync::apply::ApplyOutcome;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::snapshot::{SnapshotChunk, SnapshotHeader};

use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;
use crate::sync::array::catchup::CatchupTracker;
use crate::sync::array::op_log_redb::RedbOpLog;
use crate::sync::array::pending::PendingQueue;
use crate::sync::array::replica_state::ReplicaState;
use crate::sync::array::schema_registry::SchemaRegistry;

use super::apply::LiteApplyEngine;
use super::outcome::InboundOutcome;

/// In-flight snapshot assembly state keyed by `(array_name, snapshot_hlc_bytes)`.
pub(super) struct SnapshotAssembly {
    pub(super) header: Option<SnapshotHeader>,
    pub(super) chunks: BTreeMap<u32, SnapshotChunk>,
}

impl SnapshotAssembly {
    pub(super) fn new() -> Self {
        Self {
            header: None,
            chunks: BTreeMap::new(),
        }
    }
}

/// Dispatcher for inbound array CRDT wire messages from Origin.
///
/// Each `handle_*` method (defined in sibling modules) is stateless from the
/// transport layer's perspective — callers pass the already-decoded wire
/// message and receive an [`InboundOutcome`].
///
/// Snapshot state is buffered internally in `snapshots` until all chunks for a
/// given `(array, snapshot_hlc)` have arrived.
pub struct ArrayInbound<S: StorageEngineSync> {
    pub(super) engine: Arc<LiteApplyEngine<S>>,
    pub(super) schemas: Arc<SchemaRegistry<S>>,
    pub(super) replica: Arc<ReplicaState>,
    pub(super) pending: Arc<PendingQueue<S>>,
    #[allow(dead_code)]
    pub(super) op_log: Arc<RedbOpLog<S>>,
    /// Catchup tracker — updated when Origin sends `RetentionFloor` rejects.
    pub(super) catchup: Arc<CatchupTracker<S>>,
    /// In-flight snapshot chunk buffers.
    /// Key: `(array_name, snapshot_hlc_bytes)`.
    pub(super) snapshots: Mutex<HashMap<(String, [u8; 18]), SnapshotAssembly>>,
}

impl<S: StorageEngineSync> ArrayInbound<S> {
    /// Construct from the component parts shared with `NodeDbLite`.
    pub fn new(
        engine: Arc<LiteApplyEngine<S>>,
        schemas: Arc<SchemaRegistry<S>>,
        replica: Arc<ReplicaState>,
        pending: Arc<PendingQueue<S>>,
        op_log: Arc<RedbOpLog<S>>,
        catchup: Arc<CatchupTracker<S>>,
    ) -> Self {
        Self {
            engine,
            schemas,
            replica,
            pending,
            op_log,
            catchup,
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    /// The stable replica identity for this Lite peer.
    ///
    /// Used by the transport layer to construct `ArrayAckMsg` bodies.
    pub fn replica_id(&self) -> u64 {
        self.replica.replica_id().as_u64()
    }

    /// Drive [`nodedb_array::sync::apply::apply_op`] on a borrowed
    /// [`LiteApplyEngine`].
    pub(super) fn apply_single_op(&self, op: &ArrayOp) -> Result<ApplyOutcome, LiteError> {
        let mut engine_ref: &LiteApplyEngine<S> = &self.engine;
        nodedb_array::sync::apply::apply_op(&mut engine_ref, op).map_err(|e| LiteError::Storage {
            detail: format!("apply: {e}"),
        })
    }
}

/// Convert an [`ApplyOutcome`] from `nodedb-array` into an [`InboundOutcome`].
pub(super) fn map_apply_outcome(outcome: ApplyOutcome) -> InboundOutcome {
    match outcome {
        ApplyOutcome::Applied => InboundOutcome::Applied,
        ApplyOutcome::Idempotent => InboundOutcome::Idempotent,
        ApplyOutcome::Rejected(r) => InboundOutcome::Rejected(r),
    }
}
