//! `SyncDelegate` — the callback interface the transport uses to read pending
//! work from the owning `NodeDbLite` and to apply inbound state changes.
//!
//! Held as `Arc<dyn SyncDelegate>` by the transport so the runner does not
//! own the database. Splitting this trait into its own file keeps the
//! per-engine method blocks easy to scan and prevents the runtime modules
//! (`dispatch`, `push`) from drowning in trait surface.

use crate::engine::crdt::engine::PendingDelta;
use crate::sync::outbound::columnar::PendingColumnarBatch;
use crate::sync::outbound::fts::{PendingFtsDelete, PendingFtsIndex};
use crate::sync::outbound::spatial::{PendingSpatialDelete, PendingSpatialInsert};
use crate::sync::outbound::timeseries::PendingTimeseriesBatch;
use crate::sync::outbound::vector::{PendingVectorDelete, PendingVectorInsert};

/// Callback interface for the sync runner to read/write pending deltas
/// from the owning `NodeDbLite`. This avoids the runner owning the database.
///
/// **Runtime contract:** All methods are called from inside a Tokio runtime
/// (via `run_sync_loop`). Implementations MUST NOT use
/// `tokio::task::block_in_place` to call `&self` async methods — that
/// pattern panics on `current_thread` runtimes and is exactly what this
/// trait was redesigned to avoid. Async work (e.g. persisting a synced
/// function definition to storage) belongs on `import_definition`, which
/// is `async fn` for that reason. Sync methods that touch only in-memory
/// state may stay sync.
#[async_trait::async_trait]
pub trait SyncDelegate: Send + Sync + 'static {
    /// Get all pending CRDT deltas to push to Origin.
    fn pending_deltas(&self) -> Vec<PendingDelta>;
    /// Acknowledge deltas up to the given mutation_id.
    fn acknowledge(&self, mutation_id: u64);
    /// Reject a specific delta (rollback optimistic state).
    fn reject(&self, mutation_id: u64);
    /// Reject a delta with policy-aware resolution.
    /// Consults the PolicyRegistry before deciding how to handle the rejection.
    fn reject_with_policy(
        &self,
        mutation_id: u64,
        hint: &nodedb_types::sync::compensation::CompensationHint,
    );
    /// Import remote deltas from Origin into local CRDT state.
    fn import_remote(&self, data: &[u8]);
    /// Import a definition sync message (function/trigger/procedure) from Origin.
    /// Async because persisting the definition to storage involves
    /// KV store writes through `spawn_blocking`.
    async fn import_definition(&self, msg: &nodedb_types::sync::wire::DefinitionSyncMsg);

    /// Apply a single `ArrayDelta` frame from Origin.
    ///
    /// Returns the `ArrayAckMsg` to send back to Origin (advancing its GC
    /// frontier), or `None` if the frame was already applied (idempotent) or
    /// rejected (rejection is handled internally with a warning log).
    fn handle_array_delta(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg>;

    /// Apply a batch of `ArrayDelta` frames from Origin.
    ///
    /// Applies each op in order. Returns the `ArrayAckMsg` for the highest
    /// HLC successfully applied (or `None` if the batch was empty or all ops
    /// were idempotent/rejected).
    fn handle_array_delta_batch(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg>;

    /// Process an `ArrayReject` from Origin.
    ///
    /// Removes the rejected op from the Lite pending queue and marks the array
    /// for catch-up if the reason is `RetentionFloor`.
    fn handle_array_reject(&self, msg: &nodedb_types::sync::wire::ArrayRejectMsg);

    // ── Columnar ─────────────────────────────────────────────────────────────
    fn pending_columnar_batches(&self) -> Vec<PendingColumnarBatch>;
    fn acknowledge_columnar_batch(&self, batch_id: u64);
    fn reject_columnar_batch(&self, batch: PendingColumnarBatch);

    // ── Vector ───────────────────────────────────────────────────────────────
    fn pending_vector_inserts(&self) -> Vec<PendingVectorInsert>;
    fn acknowledge_vector_insert(&self, batch_id: u64);
    fn reject_vector_insert(&self, entry: PendingVectorInsert);

    fn pending_vector_deletes(&self) -> Vec<PendingVectorDelete>;
    fn acknowledge_vector_delete(&self, batch_id: u64);
    fn reject_vector_delete(&self, entry: PendingVectorDelete);

    // ── FTS ──────────────────────────────────────────────────────────────────
    fn pending_fts_indexes(&self) -> Vec<PendingFtsIndex>;
    fn acknowledge_fts_index(&self, batch_id: u64);
    fn reject_fts_index(&self, entry: PendingFtsIndex);

    fn pending_fts_deletes(&self) -> Vec<PendingFtsDelete>;
    fn acknowledge_fts_delete(&self, batch_id: u64);
    fn reject_fts_delete(&self, entry: PendingFtsDelete);

    // ── Spatial ──────────────────────────────────────────────────────────────
    fn pending_spatial_inserts(&self) -> Vec<PendingSpatialInsert>;
    fn acknowledge_spatial_insert(&self, batch_id: u64);
    fn reject_spatial_insert(&self, entry: PendingSpatialInsert);

    fn pending_spatial_deletes(&self) -> Vec<PendingSpatialDelete>;
    fn acknowledge_spatial_delete(&self, batch_id: u64);
    fn reject_spatial_delete(&self, entry: PendingSpatialDelete);

    // ── Timeseries ───────────────────────────────────────────────────────────
    fn pending_timeseries_batches(&self) -> Vec<PendingTimeseriesBatch>;
    /// Acknowledge all pending batches for a collection (Origin confirmed receipt).
    fn acknowledge_timeseries_collection(&self, collection: &str);
    fn reject_timeseries_batch(&self, batch: PendingTimeseriesBatch);
}
