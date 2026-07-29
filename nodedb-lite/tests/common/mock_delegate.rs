//! Recording `SyncDelegate` used by the transport integration tests.
//!
//! Every callback the transport can invoke is implemented; the handful the
//! dispatch/push tests assert on record into a `std::sync::Mutex` (not
//! tokio's) so assertions can read them from outside an async context.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_lite::LiteError;
use nodedb_lite::engine::crdt::engine::PendingDelta;
use nodedb_lite::nodedb::CollectionMeta;
use nodedb_lite::sync::{
    PendingColumnarBatch, PendingFtsDelete, PendingFtsIndex, PendingSpatialDelete,
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
        }
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
    async fn apply_remote_row(&self, msg: &nodedb_types::sync::wire::RowPushMsg) {
        self.applied_rows.lock().expect("applied_rows lock").push((
            msg.collection.clone(),
            msg.document_id.clone(),
            msg.op,
        ));
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
    fn handle_array_delta(
        &self,
        _msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        None
    }
    fn handle_array_delta_batch(
        &self,
        _msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        None
    }
    fn handle_array_reject(&self, _msg: &nodedb_types::sync::wire::ArrayRejectMsg) {}

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

    async fn pending_fts_indexes(&self) -> Vec<(Vec<u8>, PendingFtsIndex)> {
        Vec::new()
    }
    async fn mark_fts_index_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_fts_index_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_fts_index(&self, _durable_key: Vec<u8>) {}

    async fn pending_fts_deletes(&self) -> Vec<(Vec<u8>, PendingFtsDelete)> {
        Vec::new()
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
    async fn clear_engine_in_flight(&self) {}

    async fn persist_producer_state(&self, _producer_id: u64, _accepted_epoch: u64) {}
    async fn load_producer_state(&self) -> (u64, u64) {
        (0, 0)
    }
    async fn next_stream_seq(&self, _stream_id: u64) -> u64 {
        0
    }
    async fn record_stream_ack(&self, _stream_id: u64, _applied_seq: u64) {}

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
