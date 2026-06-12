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
    /// Assign a stable stream seq to a pending CRDT delta (first-send only).
    ///
    /// No-op if the delta already has a non-zero seq, so the same seq is
    /// reused on reconnect re-sends and Origin can deduplicate.
    async fn set_pending_delta_seq(&self, mutation_id: u64, seq: u64);
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
    /// Drain up to `PUSH_DRAIN_LIMIT` pending batches from durable storage,
    /// skipping any currently in-flight entries.
    ///
    /// Returns `(durable_key, batch)` pairs. On successful send, call
    /// `mark_columnar_batch_in_flight`. The durable entry is deleted only when
    /// Origin's ack arrives via `ack_columnar_batch_in_flight`.
    async fn pending_columnar_batches(&self) -> Vec<(Vec<u8>, PendingColumnarBatch)>;
    /// Record that a columnar batch has been sent and is awaiting Origin ack.
    async fn mark_columnar_batch_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_columnar_batch_in_flight(&self, batch_id: u64);
    /// Delete the durable entry for a confirmed batch directly (used for
    /// un-encodable entries that must be discarded at send time).
    async fn acknowledge_columnar_batch(&self, durable_key: Vec<u8>);

    // ── Vector ───────────────────────────────────────────────────────────────
    /// Drain up to `PUSH_DRAIN_LIMIT` pending insert entries, skipping in-flight.
    async fn pending_vector_inserts(&self) -> Vec<(Vec<u8>, PendingVectorInsert)>;
    /// Record that a vector insert has been sent and is awaiting Origin ack.
    async fn mark_vector_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_vector_insert_in_flight(&self, batch_id: u64);
    /// Delete the durable insert entry directly (for un-encodable entries).
    async fn acknowledge_vector_insert(&self, durable_key: Vec<u8>);

    /// Drain up to `PUSH_DRAIN_LIMIT` pending delete entries, skipping in-flight.
    async fn pending_vector_deletes(&self) -> Vec<(Vec<u8>, PendingVectorDelete)>;
    /// Record that a vector delete has been sent and is awaiting Origin ack.
    async fn mark_vector_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_vector_delete_in_flight(&self, batch_id: u64);
    /// Delete the durable delete entry directly (for un-encodable entries).
    async fn acknowledge_vector_delete(&self, durable_key: Vec<u8>);

    // ── FTS ──────────────────────────────────────────────────────────────────
    /// Drain up to `PUSH_DRAIN_LIMIT` pending index entries, skipping in-flight.
    async fn pending_fts_indexes(&self) -> Vec<(Vec<u8>, PendingFtsIndex)>;
    /// Record that an FTS index entry has been sent and is awaiting Origin ack.
    async fn mark_fts_index_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_fts_index_in_flight(&self, batch_id: u64);
    /// Delete the durable index entry directly (for un-encodable entries).
    async fn acknowledge_fts_index(&self, durable_key: Vec<u8>);

    /// Drain up to `PUSH_DRAIN_LIMIT` pending FTS delete entries, skipping in-flight.
    async fn pending_fts_deletes(&self) -> Vec<(Vec<u8>, PendingFtsDelete)>;
    /// Record that an FTS delete entry has been sent and is awaiting Origin ack.
    async fn mark_fts_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_fts_delete_in_flight(&self, batch_id: u64);
    /// Delete the durable delete entry directly (for un-encodable entries).
    async fn acknowledge_fts_delete(&self, durable_key: Vec<u8>);

    // ── Spatial ──────────────────────────────────────────────────────────────
    /// Drain up to `PUSH_DRAIN_LIMIT` pending insert entries, skipping in-flight.
    async fn pending_spatial_inserts(&self) -> Vec<(Vec<u8>, PendingSpatialInsert)>;
    /// Record that a spatial insert has been sent and is awaiting Origin ack.
    async fn mark_spatial_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_spatial_insert_in_flight(&self, batch_id: u64);
    /// Delete the durable insert entry directly (for un-encodable entries).
    async fn acknowledge_spatial_insert(&self, durable_key: Vec<u8>);

    /// Drain up to `PUSH_DRAIN_LIMIT` pending spatial delete entries, skipping in-flight.
    async fn pending_spatial_deletes(&self) -> Vec<(Vec<u8>, PendingSpatialDelete)>;
    /// Record that a spatial delete has been sent and is awaiting Origin ack.
    async fn mark_spatial_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_spatial_delete_in_flight(&self, batch_id: u64);
    /// Delete the durable delete entry directly (for un-encodable entries).
    async fn acknowledge_spatial_delete(&self, durable_key: Vec<u8>);

    // ── Timeseries ───────────────────────────────────────────────────────────
    /// Drain up to `PUSH_DRAIN_LIMIT` pending batches, skipping in-flight entries.
    async fn pending_timeseries_batches(&self) -> Vec<(Vec<u8>, PendingTimeseriesBatch)>;
    /// Record that a timeseries batch has been sent and is awaiting Origin ack.
    ///
    /// Keyed by `stream_seq` because `TimeseriesAckMsg` echoes `applied_seq`
    /// but not `batch_id`.
    async fn mark_timeseries_batch_in_flight(&self, stream_seq: u64, durable_key: Vec<u8>);
    /// On Origin ack: delete all durable entries whose seq ≤ `applied_seq`.
    async fn ack_timeseries_batches_through_seq(&self, applied_seq: u64);
    /// Delete the durable entry directly (for empty/un-encodable batches).
    async fn acknowledge_timeseries_batch(&self, durable_key: Vec<u8>);

    // ── Stable seq persistence ────────────────────────────────────────────────

    /// Persist an assigned stream seq into the durable columnar entry at `key`.
    ///
    /// Must be called before sending the frame. If this returns an error the
    /// caller must NOT send — it should retain the entry for the next drain tick.
    async fn persist_columnar_seq(
        &self,
        key: &[u8],
        batch: &PendingColumnarBatch,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable timeseries entry at `key`.
    async fn persist_timeseries_seq(
        &self,
        key: &[u8],
        batch: &PendingTimeseriesBatch,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable vector insert entry at `key`.
    async fn persist_vector_insert_seq(
        &self,
        key: &[u8],
        insert: &PendingVectorInsert,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable vector delete entry at `key`.
    async fn persist_vector_delete_seq(
        &self,
        key: &[u8],
        delete: &PendingVectorDelete,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable FTS index entry at `key`.
    async fn persist_fts_index_seq(
        &self,
        key: &[u8],
        entry: &PendingFtsIndex,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable FTS delete entry at `key`.
    async fn persist_fts_delete_seq(
        &self,
        key: &[u8],
        entry: &PendingFtsDelete,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable spatial insert entry at `key`.
    async fn persist_spatial_insert_seq(
        &self,
        key: &[u8],
        insert: &PendingSpatialInsert,
    ) -> Result<(), crate::error::LiteError>;

    /// Persist an assigned stream seq into the durable spatial delete entry at `key`.
    async fn persist_spatial_delete_seq(
        &self,
        key: &[u8],
        delete: &PendingSpatialDelete,
    ) -> Result<(), crate::error::LiteError>;

    // ── Reconnect ────────────────────────────────────────────────────────────

    /// Clear all engine in-flight maps on reconnect.
    ///
    /// The durable entries are still in storage and will be re-drained on the
    /// next push tick. Origin's idempotent gate deduplicates re-sent batches.
    async fn clear_engine_in_flight(&self);

    // ── Producer state ───────────────────────────────────────────────────────

    /// Durably persist the server-assigned `producer_id` and `accepted_epoch`
    /// so they survive process restart and can be reloaded on reconnect.
    ///
    /// Called after every successful handshake acknowledgement.
    async fn persist_producer_state(&self, producer_id: u64, accepted_epoch: u64);

    /// Load the last-persisted `producer_id` and `accepted_epoch`.
    ///
    /// Returns `(0, 0)` if no state has been persisted yet (first run).
    /// Called at the start of each connection attempt so the client has its
    /// identity available before the outbound handshake is built.
    async fn load_producer_state(&self) -> (u64, u64);

    // ── Stream sequence ──────────────────────────────────────────────────────

    /// Assign the next monotonic sequence number for the given `stream_id`.
    ///
    /// Delegates to `StreamSeqTracker::next_seq` which persists before
    /// returning (persist-before-send invariant). On storage error, logs a
    /// warning and returns `0` — a zero seq applies unconditionally on Origin,
    /// so this is safe degradation with no data loss.
    async fn next_stream_seq(&self, stream_id: u64) -> u64;

    /// Record that Origin has applied `applied_seq` for `stream_id`.
    ///
    /// Delegates to `StreamSeqTracker::record_ack`. Errors are logged and
    /// ignored — the last_assigned frontier already prevents re-sending
    /// un-acked seqs, so this is a refinement of the acknowledged frontier.
    async fn record_stream_ack(&self, stream_id: u64, applied_seq: u64);
}
