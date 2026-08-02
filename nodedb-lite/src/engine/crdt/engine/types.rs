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
/// Key prefix for per-collection Loro snapshots in the `LoroState` namespace.
///
/// Each collection owns its own Loro document, so each gets its own entry:
/// `loro_snapshot:<collection>`.
pub(super) const SNAPSHOT_KEY: &[u8] = b"loro_snapshot:";
/// Key prefix for the incremental updates written on top of a collection's
/// snapshot in the `LoroState` namespace: `loro_delta:<collection>:<seq>`.
///
/// Distinct from [`DELTA_KEY_PREFIX`], which holds the unsent-to-Origin sync
/// queue. These entries are durability, not sync: they are replayed on open
/// and deleted when the base snapshot that contains them is rewritten,
/// whereas a sync delta is deleted when Origin acknowledges it.
pub(super) const STATE_DELTA_KEY: &[u8] = b"loro_delta:";
/// Key for the vector clock in the `Meta` namespace.
pub(super) const VCLOCK_KEY: &[u8] = b"vector_clock";

/// Rewrite a collection's base snapshot once its accumulated updates reach
/// this fraction of it — a ratio, so the bound holds at any collection size.
///
/// Restore replays every update written since the base, so this also bounds
/// the replay: open costs the base plus at most this fraction again.
pub(super) const DELTA_CHECKPOINT_RATIO: usize = 4;
/// Never rewrite the base for less than this many accumulated update bytes.
///
/// A fraction of a small document is a few hundred bytes, which would restore
/// the full-rewrite-per-flush behaviour for precisely the collections where
/// incremental writes cost least.
pub(super) const DELTA_CHECKPOINT_MIN_BYTES: usize = 64 * 1024;

/// CRDT engine for edge devices.
///
/// Not `Send` — owned by a single task. The `NodeDbLite` wrapper handles
/// the async bridging via `spawn_blocking` or `Mutex` as needed.
pub struct CrdtEngine {
    /// This device's base peer ID. Each collection's document derives its own
    /// Loro peer ID from it (see `CrdtEngine::collection_peer_id`).
    pub(super) peer_id: u64,
    /// One Loro document per collection.
    ///
    /// A delta must be self-contained: the receiver stores documents per
    /// collection, so it can only apply operations whose causal predecessors
    /// live in that same collection's document. With a single shared oplog,
    /// a delta exported for collection `A` causally depends on whatever was
    /// written to collection `B` in between — predecessors the receiver never
    /// gets, leaving the row permanently unapplied. Partitioning the oplog by
    /// collection makes every exported slice causally complete on its own.
    ///
    /// `BTreeMap` so `collection_names()` and snapshot export are
    /// deterministic across runs.
    pub(in crate::engine::crdt) states: std::collections::BTreeMap<String, CrdtState>,
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
    /// catalog (e.g. bitemporal document collections) but have no Loro document
    /// yet (i.e. no row has been inserted).  Merged into
    /// `collection_names()` so that SQL SELECT works before the first insert.
    pub(super) registered_collections: std::collections::HashSet<String>,
    /// Deferred writes awaiting `flush_deltas()`, in the order they were
    /// applied.
    pub(super) deferred: Vec<DeferredOp>,
    /// Oplog frontier each collection had when its snapshot was last written to
    /// storage.
    ///
    /// A snapshot export is O(document), not O(new operations), so exporting a
    /// collection whose frontier has not moved rewrites the identical bytes.
    /// Comparing against this map is what lets an idle store do no snapshot
    /// work at all.
    pub(in crate::engine::crdt) flushed_versions: HashMap<String, loro::VersionVector>,
    /// Size of each collection's base snapshot as last written.
    ///
    /// The denominator of the checkpoint decision: updates are folded back
    /// into the base once they reach a fraction of it.
    pub(in crate::engine::crdt) checkpoint_bytes: HashMap<String, usize>,
    /// Update bytes written on top of each collection's current base.
    pub(in crate::engine::crdt) delta_bytes: HashMap<String, usize>,
    /// Sequence the next update for each collection is stored under. Also the
    /// count of updates a checkpoint must delete.
    pub(in crate::engine::crdt) next_delta_seq: HashMap<String, u64>,
    /// Pending deltas whose stored form is not known to match the queue.
    ///
    /// The queue is append-only and each entry is written under its own key,
    /// so an entry already on disk does not need rewriting. Only the ones
    /// added — or edited, when a send assigns a `seq` — since the last flush
    /// do. Without this the whole outbox is rewritten every tick, which for a
    /// replica with no Origin to acknowledge it means an unbounded queue
    /// rewritten in full once per `auto_flush_ms`.
    pub(in crate::engine::crdt) unpersisted_deltas: std::collections::HashSet<u64>,
    /// Number of queue entries handed out for writing.
    ///
    /// Exposed through [`CrdtEngine::pending_delta_write_count`] so callers can
    /// assert on write volume directly: an idle store must not advance it.
    pub(in crate::engine::crdt) delta_writes: AtomicU64,
    /// Number of full snapshot exports performed for persistence.
    ///
    /// Exposed through [`CrdtEngine::snapshot_export_count`] so callers can
    /// assert on export volume directly instead of inferring it from timings.
    pub(in crate::engine::crdt) snapshot_exports: AtomicU64,
}

/// One deferred write awaiting `flush_deltas`, with the exact counter range
/// its operations occupy in its collection's document.
pub(super) struct DeferredOp {
    pub(super) collection: String,
    pub(super) document_id: String,
    pub(super) from_counter: i32,
    pub(super) to_counter: i32,
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
