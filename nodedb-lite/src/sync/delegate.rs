//! `SyncDelegate` — the callback interface the transport uses to read pending
//! work from the owning `NodeDbLite` and to apply inbound state changes.
//!
//! Held as `Arc<dyn SyncDelegate>` by the transport so the runner does not
//! own the database.
//!
//! The trait compiles without the `sync` feature. `NodeDbLite` implements it
//! in every native build, so inbound-apply logic stays testable and usable
//! by an embedder-provided transport. Only the WebSocket transport that
//! drives it is feature-gated.

use crate::engine::crdt::engine::PendingDelta;
use crate::sync::array::PendingArrayOp;
use crate::sync::outbound::columnar::PendingColumnarBatch;
use crate::sync::outbound::fts::{PendingFtsDelete, PendingFtsIndex};
use crate::sync::outbound::kv::PendingKvWrite;
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
    /// This instance's durable identity: `lite_id`, `epoch`, and the Loro peer
    /// id local operations are authored under.
    ///
    /// Read for every frame rather than cached by the transport. All three
    /// change while a session is alive — a `peer_id_collision` refusal rotates
    /// the peer id, a reported fork replaces the whole identity — and a frame
    /// stamped from a stale copy carries exactly the identity Origin refused.
    fn sync_identity(&self) -> crate::identity::LiteIdentity;

    /// Replace this instance's producer identity after Origin reports a fork,
    /// re-authoring local state under it.
    ///
    /// Async because it persists the new identity and flushes the rebuilt
    /// documents; a rotation that is not durable resumes the forked identity
    /// on the next open.
    async fn regenerate_identity(&self);

    /// Adopt a new Loro peer id after Origin refuses the current one as
    /// another replica's, re-authoring local state under it.
    async fn rotate_peer_id(&self);

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
    /// Import remote deltas from Origin into `collection`'s local CRDT state.
    ///
    /// The collection travels with the update because each collection has its
    /// own Loro document; the update bytes alone do not identify the target.
    fn import_remote(&self, collection: &str, data: &[u8]);

    /// Apply a server-originated row post-image (opcode `0x15`).
    ///
    /// Distinct from [`Self::import_remote`], which takes Loro update bytes.
    /// Origin sends this for writes with no client-authored CRDT operation to
    /// replicate — SQL DML, and DDL-managed system rows such as retention
    /// policies and alerts — carrying the row's full post-image (or, for a
    /// delete, an empty payload and [`RowOp::Delete`]).
    ///
    /// A KV collection's post-image is the `{key, value…}` row every KV read
    /// returns. A payload that is not a row map is a serialization error,
    /// and nothing is applied.
    ///
    /// Async because applying the row writes through to local storage.
    ///
    /// [`RowOp::Delete`]: nodedb_types::sync::wire::RowOp::Delete
    async fn apply_remote_row(
        &self,
        msg: &nodedb_types::sync::wire::RowPushMsg,
    ) -> Result<(), nodedb_types::error::NodeDbError>;
    /// Import a definition sync message (function/trigger/procedure) from Origin.
    /// Async because persisting the definition to storage involves
    /// KV store writes through `spawn_blocking`.
    async fn import_definition(&self, msg: &nodedb_types::sync::wire::DefinitionSyncMsg);

    /// Materialize a collection announced by a sync peer (opcode `0x13`).
    ///
    /// Create-if-absent registers the collection from the descriptor and
    /// persists authoritative metadata so the SQL catalog surfaces the real
    /// engine, bitemporal flag, and columns. Async because it performs storage
    /// reads/writes. Errors are logged and swallowed at the impl boundary so a
    /// bad announcement never kills the sync session.
    async fn import_collection_schema(
        &self,
        msg: &nodedb_types::sync::wire::CollectionSchemaSyncMsg,
    );

    /// Apply a single `ArrayDelta` frame from Origin.
    ///
    /// An applied or already-present op advances the array's catch-up
    /// high-water mark to its HLC. Returns the `ArrayAckMsg` to send back to
    /// Origin (advancing its GC frontier), or `None` if the frame was already
    /// applied (idempotent) or rejected (logged as a warning).
    ///
    /// Async because recording the high-water mark writes to storage.
    async fn handle_array_delta(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg>;

    /// Apply a batch of `ArrayDelta` frames from Origin.
    ///
    /// Applies each op in order. The catch-up high-water mark advances to
    /// the last op before the first rejected op, so a rejected op is asked
    /// for again on the next catch-up. Returns the `ArrayAckMsg` for the
    /// highest HLC applied (or `None` if the batch was empty or all ops were
    /// idempotent/rejected).
    async fn handle_array_delta_batch(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg>;

    /// Process an `ArrayReject` from Origin.
    ///
    /// Applies the reason's [`RejectDisposition`]: a `Drop` removes the op
    /// from the pending queue, and every other disposition keeps it. For
    /// `RetentionFloor`, persists the array's catch-up flag and returns the
    /// catch-up request to send: it starts at `Hlc::ZERO`, so Origin answers
    /// with a snapshot. Returns `None` for every other reason.
    ///
    /// [`RejectDisposition`]: crate::sync::array::RejectDisposition
    async fn handle_array_reject(
        &self,
        msg: &nodedb_types::sync::wire::ArrayRejectMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayCatchupRequestMsg>;

    /// Buffer an `ArraySnapshot` header from Origin. Errors are logged.
    async fn handle_array_snapshot(&self, msg: &nodedb_types::sync::wire::ArraySnapshotMsg);

    /// Buffer an `ArraySnapshotChunk` from Origin.
    ///
    /// On the last chunk the snapshot is assembled and applied: the array's
    /// catch-up flag is cleared, its high-water mark advances to the
    /// snapshot HLC, and the `ArrayAckMsg` carrying that HLC is returned.
    /// Returns `None` while chunks remain. An assembly or apply error is
    /// logged and flags the array, so the next session asks for a snapshot
    /// again.
    async fn handle_array_snapshot_chunk(
        &self,
        msg: &nodedb_types::sync::wire::ArraySnapshotChunkMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg>;

    /// Start the array catch-up round for a newly connected session.
    ///
    /// Returns one `ArrayCatchupRequestMsg` per array this replica holds,
    /// each starting at the array's catch-up high-water mark (or `Hlc::ZERO`
    /// when flagged for a snapshot). Drops partial snapshot assemblies from
    /// the previous session. Returns an empty list when no arrays exist.
    fn begin_array_catchup(&self) -> Vec<nodedb_types::sync::wire::ArrayCatchupRequestMsg>;

    // ── Array push ───────────────────────────────────────────────────────────
    /// Up to `limit` queued local array ops with an HLC strictly greater than
    /// `after`, in HLC order. `None` starts at the head of the queue.
    async fn pending_array_ops(
        &self,
        after: Option<nodedb_array::sync::hlc::Hlc>,
        limit: usize,
    ) -> Result<Vec<PendingArrayOp>, crate::error::LiteError>;

    /// Persist `op.seq` into the queued op before its first send.
    ///
    /// On an error the caller does not send, and the op waits for a later
    /// tick.
    async fn persist_array_op_seq(
        &self,
        op: &PendingArrayOp,
    ) -> Result<(), crate::error::LiteError>;

    /// Remove the queued array ops named in `hlcs`: ops Origin applied, and
    /// ops the push loop cannot encode. Ops no longer queued are skipped.
    async fn retire_array_ops(
        &self,
        hlcs: &[nodedb_array::sync::hlc::Hlc],
    ) -> Result<(), crate::error::LiteError>;

    /// The `ArraySchema` body announcing `array`'s current schema to Origin:
    /// the schema HLC and Loro snapshot, read together. Returns `None` when
    /// `array` has no local schema.
    fn array_schema_announce(
        &self,
        array: &str,
    ) -> Result<Option<nodedb_types::sync::wire::ArraySchemaSyncMsg>, crate::error::LiteError>;

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
    async fn pending_fts_indexes(
        &self,
    ) -> Result<Vec<(Vec<u8>, PendingFtsIndex)>, crate::error::LiteError>;
    /// Record that an FTS index entry has been sent and is awaiting Origin ack.
    async fn mark_fts_index_in_flight(&self, batch_id: u64, durable_key: Vec<u8>);
    /// On Origin ack: remove in-flight record and delete the durable entry.
    async fn ack_fts_index_in_flight(&self, batch_id: u64);
    /// Delete the durable index entry directly (for un-encodable entries).
    async fn acknowledge_fts_index(&self, durable_key: Vec<u8>);

    /// Drain up to `PUSH_DRAIN_LIMIT` pending FTS delete entries, skipping in-flight.
    async fn pending_fts_deletes(
        &self,
    ) -> Result<Vec<(Vec<u8>, PendingFtsDelete)>, crate::error::LiteError>;
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
    /// Indexed by `stream_seq` for the cumulative `applied_seq` success path,
    /// and carrying `batch_id` so a terminally rejected batch — which never
    /// advances that frontier — can still be named and retired.
    async fn mark_timeseries_batch_in_flight(
        &self,
        stream_seq: u64,
        batch_id: u64,
        durable_key: Vec<u8>,
    );
    /// On Origin ack: delete all durable entries whose seq ≤ `applied_seq`.
    async fn ack_timeseries_batches_through_seq(&self, applied_seq: u64);
    /// On terminal rejection: delete the durable entry for exactly `batch_id`.
    ///
    /// Required alongside the sweep above because a rejected batch's seq sits
    /// *above* `applied_seq` (it never applied), so the cumulative path can
    /// never retire it and the push loop would re-send it indefinitely.
    async fn ack_timeseries_batch_by_id(&self, batch_id: u64);
    /// Delete the durable entry directly (for empty/un-encodable batches).
    async fn acknowledge_timeseries_batch(&self, durable_key: Vec<u8>);

    // ── KV ───────────────────────────────────────────────────────────────────
    /// Up to `PUSH_DRAIN_LIMIT` pending KV writes in queue order, skipping
    /// the ones in flight. Returns `(durable_key, write)` pairs.
    async fn pending_kv_writes(
        &self,
    ) -> Result<Vec<(Vec<u8>, PendingKvWrite)>, crate::error::LiteError>;
    /// Persist the seq assigned to the KV write stored under `key`.
    ///
    /// Called before the frame is sent. On an error the caller does not
    /// send, and the write waits for the next tick.
    async fn persist_kv_write_seq(
        &self,
        key: &[u8],
        write: &PendingKvWrite,
    ) -> Result<(), crate::error::LiteError>;
    /// Record that the KV write with `batch_id` was sent and awaits its ack.
    async fn mark_kv_write_in_flight(&self, batch_id: u64);
    /// Retire the KV write with `batch_id` after Origin's final answer:
    /// forget it is in flight and delete its durable entry.
    async fn retire_kv_write(&self, batch_id: u64) -> Result<(), crate::error::LiteError>;

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

    // ── Collection schema (outbound announce) ───────────────────────────────

    /// Look up a collection's persisted metadata by name, for building the
    /// outbound `CollectionSchema` (opcode `0x13`) announce sent before a
    /// collection's first CRDT delta in a sync session.
    ///
    /// Returns `None` if the collection has no explicitly persisted metadata
    /// (e.g. an implicit CRDT-only collection created without DDL) — the
    /// caller skips announcing it and lets Origin infer a schemaless
    /// collection from the first delta, matching pre-announce behavior.
    async fn get_collection_meta(
        &self,
        name: &str,
    ) -> Option<crate::nodedb::collection::CollectionMeta>;
}
