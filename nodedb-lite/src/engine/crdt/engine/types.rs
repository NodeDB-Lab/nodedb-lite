// SPDX-License-Identifier: BUSL-1.1

//! CrdtEngine type definitions: the engine struct, its pending-delta
//! record, field aliases, and storage key constants.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use loro::LoroValue;
use nodedb_crdt::CrdtState;

/// A single field in a CRDT operation: `(field_name, value)`.
pub type CrdtField<'a> = (&'a str, LoroValue);

/// A batch CRDT operation: `(collection, doc_id, fields)`.
pub type CrdtBatchOp<'a> = (&'a str, &'a str, &'a [CrdtField<'a>]);

/// Key prefix for delta blobs in the `Crdt` namespace.
pub(super) const DELTA_KEY_PREFIX: &[u8] = b"delta:";
/// Key for the Loro state snapshot in the `LoroState` namespace.
pub(super) const SNAPSHOT_KEY: &[u8] = b"loro_snapshot";
/// Key for the vector clock in the `Meta` namespace.
pub(super) const VCLOCK_KEY: &[u8] = b"vector_clock";

/// CRDT engine for edge devices.
///
/// Not `Send` — owned by a single task. The `NodeDbLite` wrapper handles
/// the async bridging via `spawn_blocking` or `Mutex` as needed.
pub struct CrdtEngine {
    pub(in crate::engine::crdt) state: CrdtState,
    /// Monotonically increasing mutation ID. Used as delta ordering key.
    pub(super) next_mutation_id: AtomicU64,
    /// Unsent deltas accumulated since last sync ACK.
    /// Each entry: `(mutation_id, collection, doc_id, delta_bytes)`.
    pub(in crate::engine::crdt) pending_deltas: Vec<PendingDelta>,
    /// Per-collection version: highest mutation_id that's been ACK'd by Origin.
    pub(super) acked_versions: HashMap<String, u64>,
    /// Conflict resolution policies per collection.
    /// Evaluated on sync when Origin rejects a delta.
    pub(in crate::engine::crdt) policies: nodedb_crdt::PolicyRegistry,
    /// Explicitly registered collection names for collections that exist in the
    /// catalog (e.g. bitemporal document collections) but have no Loro root-map
    /// entry yet (i.e. no document has been inserted).  Merged into
    /// `collection_names()` so that SQL SELECT works before the first insert.
    pub(super) registered_collections: std::collections::HashSet<String>,
    /// Version vector captured before the first deferred mutation.
    /// Used by `flush_deltas()` to export a single delta covering all
    /// deferred operations.
    pub(super) deferred_version: Option<loro::VersionVector>,
    /// Count of deferred mutations since last `flush_deltas()`.
    pub(super) deferred_count: usize,
}

/// A pending (unsent) delta waiting to be synced to Origin.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PendingDelta {
    /// Monotonic mutation ID (for ordering and dedup).
    pub mutation_id: u64,
    /// Collection this delta applies to.
    pub collection: String,
    /// Document/row ID affected.
    pub document_id: String,
    /// Loro delta bytes (compact binary).
    pub delta_bytes: Vec<u8>,
    /// Stable idempotent-producer seq for this delta. 0 = unassigned;
    /// assigned at first send and reused on reconnect re-send so Origin
    /// dedups instead of double-applying.
    #[serde(default)]
    pub seq: u64,
}
