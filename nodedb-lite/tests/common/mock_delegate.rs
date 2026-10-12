//! Recording `SyncDelegate` used by the transport integration tests.
//!
//! Every callback the transport can invoke is implemented; the handful the
//! dispatch/push tests assert on record into a `std::sync::Mutex` (not
//! tokio's) so assertions can read them from outside an async context.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use nodedb_lite::LiteError;
use nodedb_lite::engine::crdt::engine::PendingDelta;
use nodedb_lite::nodedb::CollectionMeta;
use nodedb_lite::sync::array::PendingArrayOp;
use nodedb_lite::sync::{
    PendingColumnarBatch, PendingFtsDelete, PendingFtsIndex, PendingKvWrite, PendingSpatialDelete,
    PendingSpatialInsert, PendingTimeseriesBatch, PendingVectorDelete, PendingVectorInsert,
    SyncDelegate,
};

pub struct MockDelegate {
    acked_up_to: AtomicU64,
    rejected: std::sync::Mutex<Vec<u64>>,
    imported: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    applied_rows: std::sync::Mutex<Vec<(String, String, nodedb_types::sync::wire::RowOp)>>,
    imported_schemas: std::sync::Mutex<Vec<String>>,
    pending: std::sync::Mutex<Vec<PendingDelta>>,
    collection_metas: std::sync::Mutex<std::collections::HashMap<String, CollectionMeta>>,
    identity: std::sync::Mutex<nodedb_lite::identity::LiteIdentity>,
    identity_changes: AtomicU64,
    peer_id_rotations: AtomicU64,
    refuse_rows: AtomicBool,
    kv_pending: std::sync::Mutex<Vec<(Vec<u8>, PendingKvWrite)>>,
    kv_in_flight: std::sync::Mutex<Vec<u64>>,
    kv_retired: std::sync::Mutex<Vec<u64>>,
    stream_acks: std::sync::Mutex<Vec<(u64, u64)>>,
    in_flight_clears: AtomicU64,
    /// Array catch-up, snapshot, and push-queue state.
    pub array: super::mock_array::MockArrayState,
}

impl Default for MockDelegate {
    fn default() -> Self {
        Self::new()
    }
}

impl MockDelegate {
    pub fn new() -> Self {
        Self {
            acked_up_to: AtomicU64::new(0),
            rejected: std::sync::Mutex::new(Vec::new()),
            imported: std::sync::Mutex::new(Vec::new()),
            applied_rows: std::sync::Mutex::new(Vec::new()),
            imported_schemas: std::sync::Mutex::new(Vec::new()),
            pending: std::sync::Mutex::new(Vec::new()),
            collection_metas: std::sync::Mutex::new(std::collections::HashMap::new()),
            identity: std::sync::Mutex::new(nodedb_lite::identity::LiteIdentity {
                lite_id: "mock-lite".to_string(),
                epoch: 1,
                peer_id: nodedb_lite::identity::mint_peer_id(),
            }),
            identity_changes: AtomicU64::new(0),
            peer_id_rotations: AtomicU64::new(0),
            refuse_rows: AtomicBool::new(false),
            kv_pending: std::sync::Mutex::new(Vec::new()),
            kv_in_flight: std::sync::Mutex::new(Vec::new()),
            kv_retired: std::sync::Mutex::new(Vec::new()),
            stream_acks: std::sync::Mutex::new(Vec::new()),
            in_flight_clears: AtomicU64::new(0),
            array: super::mock_array::MockArrayState::default(),
        }
    }

    /// Make `apply_remote_row` refuse every row with a serialization error.
    pub fn refuse_rows(&self) {
        self.refuse_rows.store(true, Ordering::Relaxed);
    }

    /// Queue KV writes for the push loop, as `(durable_key, write)` pairs.
    pub fn set_pending_kv(&self, writes: Vec<(Vec<u8>, PendingKvWrite)>) {
        *self.kv_pending.lock().expect("kv_pending lock") = writes;
    }

    /// Batch ids passed to `retire_kv_write`, in order.
    pub fn retired_kv(&self) -> Vec<u64> {
        self.kv_retired.lock().expect("kv_retired lock").clone()
    }

    /// `(stream_id, applied_seq)` pairs passed to `record_stream_ack`.
    pub fn stream_acks(&self) -> Vec<(u64, u64)> {
        self.stream_acks.lock().expect("stream_acks lock").clone()
    }

    /// How many times `clear_engine_in_flight` was invoked.
    pub fn in_flight_clears(&self) -> u64 {
        self.in_flight_clears.load(Ordering::Relaxed)
    }

    /// The Loro peer id this delegate currently reports.
    pub fn peer_id(&self) -> u64 {
        self.identity.lock().expect("identity lock").peer_id
    }

    /// How many times `rotate_peer_id` was invoked.
    pub fn peer_id_rotations(&self) -> u64 {
        self.peer_id_rotations.load(Ordering::Relaxed)
    }

    /// How many times `regenerate_identity` was invoked.
    pub fn identity_changes(&self) -> u64 {
        self.identity_changes.load(Ordering::Relaxed)
    }

    /// Highest mutation id passed to `acknowledge`, or 0 if none.
    pub fn acked_up_to(&self) -> u64 {
        self.acked_up_to.load(Ordering::Relaxed)
    }

    /// Mutation ids passed to `reject` / `reject_with_policy`, in order.
    pub fn rejected(&self) -> Vec<u64> {
        self.rejected.lock().expect("rejected lock").clone()
    }

    /// `(collection, bytes)` pairs passed to `import_remote`, in order.
    pub fn imported(&self) -> Vec<(String, Vec<u8>)> {
        self.imported.lock().expect("imported lock").clone()
    }

    /// Snapshot of rows applied so far, in application order.
    pub fn applied_rows(&self) -> Vec<(String, String, nodedb_types::sync::wire::RowOp)> {
        self.applied_rows.lock().expect("applied_rows lock").clone()
    }

    /// Names of the collection schemas imported, in order.
    pub fn imported_schemas(&self) -> Vec<String> {
        self.imported_schemas
            .lock()
            .expect("imported_schemas lock")
            .clone()
    }

    pub fn set_pending(&self, deltas: Vec<PendingDelta>) {
        *self.pending.lock().expect("pending lock") = deltas;
    }

    pub fn set_collection_meta(&self, name: &str, meta: CollectionMeta) {
        self.collection_metas
            .lock()
            .expect("collection_metas lock")
            .insert(name.to_string(), meta);
    }
}

#[async_trait::async_trait]
impl SyncDelegate for MockDelegate {
    fn sync_identity(&self) -> nodedb_lite::identity::LiteIdentity {
        self.identity.lock().expect("identity lock").clone()
    }
    async fn regenerate_identity(&self) {
        let mut identity = self.identity.lock().expect("identity lock");
        identity.lite_id = format!("{}-regenerated", identity.lite_id);
        identity.epoch = 1;
        identity.peer_id = nodedb_lite::identity::mint_peer_id();
        self.identity_changes.fetch_add(1, Ordering::Relaxed);
    }
    async fn rotate_peer_id(&self) {
        let mut identity = self.identity.lock().expect("identity lock");
        identity.peer_id = nodedb_lite::identity::mint_peer_id();
        self.peer_id_rotations.fetch_add(1, Ordering::Relaxed);
    }
    fn pending_deltas(&self) -> Vec<PendingDelta> {
        self.pending.lock().expect("pending lock").clone()
    }
    fn acknowledge(&self, mutation_id: u64) {
        self.acked_up_to.store(mutation_id, Ordering::Relaxed);
    }
    async fn set_pending_delta_seq(&self, _mutation_id: u64, _seq: u64) {}
    fn reject(&self, mutation_id: u64) {
        self.rejected
            .lock()
            .expect("rejected lock")
            .push(mutation_id);
    }
    fn reject_with_policy(
        &self,
        mutation_id: u64,
        _hint: &nodedb_types::sync::compensation::CompensationHint,
    ) {
        self.rejected
            .lock()
            .expect("rejected lock")
            .push(mutation_id);
    }
    async fn apply_remote_row(
        &self,
        msg: &nodedb_types::sync::wire::RowPushMsg,
    ) -> Result<(), nodedb_types::error::NodeDbError> {
        if self.refuse_rows.load(Ordering::Relaxed) {
            return Err(nodedb_types::error::NodeDbError::serialization(
                "msgpack",
                "mock refuses every row",
            ));
        }
        self.applied_rows.lock().expect("applied_rows lock").push((
            msg.collection.clone(),
            msg.document_id.clone(),
            msg.op,
        ));
        Ok(())
    }

    fn import_remote(&self, collection: &str, data: &[u8]) {
        self.imported
            .lock()
            .expect("imported lock")
            .push((collection.to_string(), data.to_vec()));
    }
    async fn import_definition(&self, _msg: &nodedb_types::sync::wire::DefinitionSyncMsg) {}
    async fn import_collection_schema(
        &self,
        msg: &nodedb_types::sync::wire::CollectionSchemaSyncMsg,
    ) {
        self.imported_schemas
            .lock()
            .expect("imported_schemas lock")
            .push(msg.descriptor.name.clone());
    }
    async fn handle_array_delta(
        &self,
        _msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        None
    }
    async fn handle_array_delta_batch(
        &self,
        _msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        None
    }
    async fn handle_array_reject(
        &self,
        msg: &nodedb_types::sync::wire::ArrayRejectMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayCatchupRequestMsg> {
        self.array.reject(msg)
    }
    async fn handle_array_snapshot(&self, msg: &nodedb_types::sync::wire::ArraySnapshotMsg) {
        self.array.record_header(msg);
    }
    async fn handle_array_snapshot_chunk(
        &self,
        msg: &nodedb_types::sync::wire::ArraySnapshotChunkMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        self.array.record_chunk(msg)
    }
    fn begin_array_catchup(&self) -> Vec<nodedb_types::sync::wire::ArrayCatchupRequestMsg> {
        self.array.begin_catchup()
    }
    async fn pending_array_ops(
        &self,
        after: Option<nodedb_array::sync::hlc::Hlc>,
        limit: usize,
    ) -> Result<Vec<PendingArrayOp>, LiteError> {
        Ok(self.array.pending_after(after, limit))
    }
    async fn persist_array_op_seq(&self, op: &PendingArrayOp) -> Result<(), LiteError> {
        self.array.persist_seq(op);
        Ok(())
    }
    async fn retire_array_ops(
        &self,
        hlcs: &[nodedb_array::sync::hlc::Hlc],
    ) -> Result<(), LiteError> {
        self.array.retire(hlcs);
        Ok(())
    }
    fn array_schema_announce(
        &self,
        array: &str,
    ) -> Result<Option<nodedb_types::sync::wire::ArraySchemaSyncMsg>, LiteError> {
        Ok(self.array.schema(array))
    }

    async fn pending_columnar_batches(&self) -> Vec<(Vec<u8>, PendingColumnarBatch)> {
        Vec::new()
    }
    async fn mark_columnar_batch_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_columnar_batch_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_columnar_batch(&self, _durable_key: Vec<u8>) {}

    async fn pending_vector_inserts(&self) -> Vec<(Vec<u8>, PendingVectorInsert)> {
        Vec::new()
    }
    async fn mark_vector_insert_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_vector_insert_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_vector_insert(&self, _durable_key: Vec<u8>) {}

    async fn pending_vector_deletes(&self) -> Vec<(Vec<u8>, PendingVectorDelete)> {
        Vec::new()
    }
    async fn mark_vector_delete_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_vector_delete_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_vector_delete(&self, _durable_key: Vec<u8>) {}

    async fn pending_fts_indexes(&self) -> Result<Vec<(Vec<u8>, PendingFtsIndex)>, LiteError> {
        Ok(Vec::new())
    }
    async fn mark_fts_index_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_fts_index_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_fts_index(&self, _durable_key: Vec<u8>) {}

    async fn pending_fts_deletes(&self) -> Result<Vec<(Vec<u8>, PendingFtsDelete)>, LiteError> {
        Ok(Vec::new())
    }
    async fn mark_fts_delete_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_fts_delete_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_fts_delete(&self, _durable_key: Vec<u8>) {}

    async fn pending_spatial_inserts(&self) -> Vec<(Vec<u8>, PendingSpatialInsert)> {
        Vec::new()
    }
    async fn mark_spatial_insert_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_spatial_insert_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_spatial_insert(&self, _durable_key: Vec<u8>) {}

    async fn pending_spatial_deletes(&self) -> Vec<(Vec<u8>, PendingSpatialDelete)> {
        Vec::new()
    }
    async fn mark_spatial_delete_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_spatial_delete_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_spatial_delete(&self, _durable_key: Vec<u8>) {}

    async fn pending_timeseries_batches(&self) -> Vec<(Vec<u8>, PendingTimeseriesBatch)> {
        Vec::new()
    }
    async fn mark_timeseries_batch_in_flight(
        &self,
        _stream_seq: u64,
        _batch_id: u64,
        _durable_key: Vec<u8>,
    ) {
    }
    async fn ack_timeseries_batches_through_seq(&self, _applied_seq: u64) {}
    async fn ack_timeseries_batch_by_id(&self, _batch_id: u64) {}
    async fn acknowledge_timeseries_batch(&self, _durable_key: Vec<u8>) {}
    async fn clear_engine_in_flight(&self) {
        self.in_flight_clears.fetch_add(1, Ordering::Relaxed);
        self.kv_in_flight.lock().expect("kv_in_flight lock").clear();
    }

    async fn pending_kv_writes(&self) -> Result<Vec<(Vec<u8>, PendingKvWrite)>, LiteError> {
        let in_flight = self.kv_in_flight.lock().expect("kv_in_flight lock").clone();
        let retired = self.kv_retired.lock().expect("kv_retired lock").clone();
        Ok(self
            .kv_pending
            .lock()
            .expect("kv_pending lock")
            .iter()
            .filter(|(key, _)| {
                let id = u64::from_be_bytes(key.as_slice().try_into().expect("8-byte key"));
                !in_flight.contains(&id) && !retired.contains(&id)
            })
            .cloned()
            .collect())
    }
    async fn persist_kv_write_seq(
        &self,
        key: &[u8],
        write: &PendingKvWrite,
    ) -> Result<(), LiteError> {
        let mut pending = self.kv_pending.lock().expect("kv_pending lock");
        if let Some(entry) = pending.iter_mut().find(|(k, _)| k.as_slice() == key) {
            entry.1 = write.clone();
        }
        Ok(())
    }
    async fn mark_kv_write_in_flight(&self, batch_id: u64) {
        self.kv_in_flight
            .lock()
            .expect("kv_in_flight lock")
            .push(batch_id);
    }
    async fn retire_kv_write(&self, batch_id: u64) -> Result<(), LiteError> {
        self.kv_in_flight
            .lock()
            .expect("kv_in_flight lock")
            .retain(|id| *id != batch_id);
        self.kv_retired
            .lock()
            .expect("kv_retired lock")
            .push(batch_id);
        Ok(())
    }

    async fn persist_producer_state(&self, _producer_id: u64, _accepted_epoch: u64) {}
    async fn load_producer_state(&self) -> (u64, u64) {
        (0, 0)
    }
    async fn next_stream_seq(&self, _stream_id: u64) -> u64 {
        0
    }
    async fn record_stream_ack(&self, stream_id: u64, applied_seq: u64) {
        self.stream_acks
            .lock()
            .expect("stream_acks lock")
            .push((stream_id, applied_seq));
    }

    async fn get_collection_meta(&self, name: &str) -> Option<CollectionMeta> {
        self.collection_metas
            .lock()
            .expect("collection_metas lock")
            .get(name)
            .cloned()
    }

    async fn persist_columnar_seq(
        &self,
        _key: &[u8],
        _batch: &PendingColumnarBatch,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_timeseries_seq(
        &self,
        _key: &[u8],
        _batch: &PendingTimeseriesBatch,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_vector_insert_seq(
        &self,
        _key: &[u8],
        _insert: &PendingVectorInsert,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_vector_delete_seq(
        &self,
        _key: &[u8],
        _delete: &PendingVectorDelete,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_fts_index_seq(
        &self,
        _key: &[u8],
        _entry: &PendingFtsIndex,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_fts_delete_seq(
        &self,
        _key: &[u8],
        _entry: &PendingFtsDelete,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_spatial_insert_seq(
        &self,
        _key: &[u8],
        _insert: &PendingSpatialInsert,
    ) -> Result<(), LiteError> {
        Ok(())
    }
    async fn persist_spatial_delete_seq(
        &self,
        _key: &[u8],
        _delete: &PendingSpatialDelete,
    ) -> Result<(), LiteError> {
        Ok(())
    }
}
