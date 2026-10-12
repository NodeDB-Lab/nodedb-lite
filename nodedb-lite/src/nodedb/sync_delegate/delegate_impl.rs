//! `SyncDelegate` trait implementation for `NodeDbLite` — bridges the sync
//! transport to NodeDbLite's engines. Split out of `mod.rs` to keep that file
//! to module declarations only (a single trait impl cannot be split across
//! multiple `impl` blocks, so it lives here as one file).

#[cfg(not(target_arch = "wasm32"))]
use crate::storage::engine::StorageEngine;

#[cfg(not(target_arch = "wasm32"))]
use super::super::core::NodeDbLite;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl<S: StorageEngine> crate::sync::SyncDelegate for NodeDbLite<S> {
    fn sync_identity(&self) -> crate::identity::LiteIdentity {
        NodeDbLite::sync_identity(self)
    }

    async fn regenerate_identity(&self) {
        if let Err(e) = NodeDbLite::regenerate_identity(self).await {
            // The instance keeps authoring under a producer identity Origin has
            // already rejected as forked, so every push from here is refused.
            // Nothing local can repair that, and continuing quietly is how the
            // replica ends up permanently desynced without a signal.
            tracing::error!(
                error = %e,
                "SyncDelegate: fork recovery failed — this replica cannot sync until its \
                 producer identity is replaced"
            );
        }
    }

    async fn rotate_peer_id(&self) {
        if let Err(e) = NodeDbLite::rotate_peer_id(self).await {
            tracing::error!(
                error = %e,
                "SyncDelegate: peer-id rotation failed — this replica cannot sync until its \
                 Loro peer id is replaced"
            );
        }
    }

    fn pending_deltas(&self) -> Vec<crate::engine::crdt::engine::PendingDelta> {
        self.pending_crdt_deltas().unwrap_or_default()
    }

    async fn set_pending_delta_seq(&self, mutation_id: u64, seq: u64) {
        if let Err(e) = self.set_crdt_pending_delta_seq(mutation_id, seq) {
            tracing::warn!(
                mutation_id,
                seq,
                error = %e,
                "SyncDelegate: set_pending_delta_seq failed"
            );
        }
    }

    fn acknowledge(&self, mutation_id: u64) {
        if let Err(e) = self.acknowledge_deltas(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: acknowledge failed");
        }
    }

    fn reject(&self, mutation_id: u64) {
        if let Err(e) = self.reject_delta(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: reject failed");
        }
    }

    fn reject_with_policy(
        &self,
        mutation_id: u64,
        hint: &nodedb_types::sync::compensation::CompensationHint,
    ) {
        if let Err(error) = super::reject::handle_reject_with_policy_impl(self, mutation_id, hint) {
            tracing::error!(mutation_id, %error, "delta rejection maintenance returned an error");
        }
    }

    fn import_remote(&self, collection: &str, data: &[u8]) {
        if let Err(e) = self.import_remote_deltas(collection, data) {
            tracing::warn!(
                collection = %collection,
                error = %e,
                "SyncDelegate: import_remote failed"
            );
        }
    }

    async fn apply_remote_row(
        &self,
        msg: &nodedb_types::sync::wire::RowPushMsg,
    ) -> Result<(), nodedb_types::error::NodeDbError> {
        let delete = matches!(msg.op, nodedb_types::sync::wire::RowOp::Delete);
        self.apply_remote_row(&msg.collection, &msg.document_id, &msg.payload, delete)
            .await
    }

    async fn handle_array_delta(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        super::array::handle_array_delta_impl(self, msg).await
    }

    async fn handle_array_delta_batch(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        super::array::handle_array_delta_batch_impl(self, msg).await
    }

    async fn handle_array_reject(
        &self,
        msg: &nodedb_types::sync::wire::ArrayRejectMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayCatchupRequestMsg> {
        super::array::handle_array_reject_impl(self, msg).await
    }

    async fn handle_array_snapshot(&self, msg: &nodedb_types::sync::wire::ArraySnapshotMsg) {
        super::array::handle_array_snapshot_impl(self, msg).await
    }

    async fn handle_array_snapshot_chunk(
        &self,
        msg: &nodedb_types::sync::wire::ArraySnapshotChunkMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        super::array::handle_array_snapshot_chunk_impl(self, msg).await
    }

    fn begin_array_catchup(&self) -> Vec<nodedb_types::sync::wire::ArrayCatchupRequestMsg> {
        super::array::begin_array_catchup_impl(self)
    }

    async fn pending_array_ops(
        &self,
        after: Option<nodedb_array::sync::hlc::Hlc>,
        limit: usize,
    ) -> Result<Vec<crate::sync::array::PendingArrayOp>, crate::error::LiteError> {
        super::array::pending_array_ops_impl(self, after, limit).await
    }

    async fn persist_array_op_seq(
        &self,
        op: &crate::sync::array::PendingArrayOp,
    ) -> Result<(), crate::error::LiteError> {
        super::array::persist_array_op_seq_impl(self, op).await
    }

    async fn retire_array_ops(
        &self,
        hlcs: &[nodedb_array::sync::hlc::Hlc],
    ) -> Result<(), crate::error::LiteError> {
        super::array::retire_array_ops_impl(self, hlcs).await
    }

    fn array_schema_announce(
        &self,
        array: &str,
    ) -> Result<Option<nodedb_types::sync::wire::ArraySchemaSyncMsg>, crate::error::LiteError> {
        super::array::array_schema_announce_impl(self, array)
    }

    async fn pending_columnar_batches(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::columnar::PendingColumnarBatch,
    )> {
        super::columnar_handlers::pending_columnar_batches_impl(self).await
    }

    async fn mark_columnar_batch_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::columnar_handlers::mark_columnar_batch_in_flight_impl(self, batch_id, durable_key)
            .await
    }

    async fn ack_columnar_batch_in_flight(&self, batch_id: u64) {
        super::columnar_handlers::ack_columnar_batch_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_columnar_batch(&self, durable_key: Vec<u8>) {
        super::columnar_handlers::acknowledge_columnar_batch_impl(self, durable_key).await
    }

    async fn pending_vector_inserts(
        &self,
    ) -> Vec<(Vec<u8>, crate::sync::outbound::vector::PendingVectorInsert)> {
        super::vector_handlers::pending_vector_inserts_impl(self).await
    }

    async fn mark_vector_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::vector_handlers::mark_vector_insert_in_flight_impl(self, batch_id, durable_key).await
    }

    async fn ack_vector_insert_in_flight(&self, batch_id: u64) {
        super::vector_handlers::ack_vector_insert_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_vector_insert(&self, durable_key: Vec<u8>) {
        super::vector_handlers::acknowledge_vector_insert_impl(self, durable_key).await
    }

    async fn pending_vector_deletes(
        &self,
    ) -> Vec<(Vec<u8>, crate::sync::outbound::vector::PendingVectorDelete)> {
        super::vector_handlers::pending_vector_deletes_impl(self).await
    }

    async fn mark_vector_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::vector_handlers::mark_vector_delete_in_flight_impl(self, batch_id, durable_key).await
    }

    async fn ack_vector_delete_in_flight(&self, batch_id: u64) {
        super::vector_handlers::ack_vector_delete_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_vector_delete(&self, durable_key: Vec<u8>) {
        super::vector_handlers::acknowledge_vector_delete_impl(self, durable_key).await
    }

    async fn pending_fts_indexes(
        &self,
    ) -> Result<Vec<(Vec<u8>, crate::sync::outbound::fts::PendingFtsIndex)>, crate::error::LiteError>
    {
        super::fts_handlers::pending_fts_indexes_impl(self).await
    }

    async fn mark_fts_index_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::fts_handlers::mark_fts_index_in_flight_impl(self, batch_id, durable_key).await
    }

    async fn ack_fts_index_in_flight(&self, batch_id: u64) {
        super::fts_handlers::ack_fts_index_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_fts_index(&self, durable_key: Vec<u8>) {
        super::fts_handlers::acknowledge_fts_index_impl(self, durable_key).await
    }

    async fn pending_fts_deletes(
        &self,
    ) -> Result<Vec<(Vec<u8>, crate::sync::outbound::fts::PendingFtsDelete)>, crate::error::LiteError>
    {
        super::fts_handlers::pending_fts_deletes_impl(self).await
    }

    async fn mark_fts_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::fts_handlers::mark_fts_delete_in_flight_impl(self, batch_id, durable_key).await
    }

    async fn ack_fts_delete_in_flight(&self, batch_id: u64) {
        super::fts_handlers::ack_fts_delete_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_fts_delete(&self, durable_key: Vec<u8>) {
        super::fts_handlers::acknowledge_fts_delete_impl(self, durable_key).await
    }

    async fn pending_spatial_inserts(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::spatial::PendingSpatialInsert,
    )> {
        super::spatial_handlers::pending_spatial_inserts_impl(self).await
    }

    async fn mark_spatial_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::spatial_handlers::mark_spatial_insert_in_flight_impl(self, batch_id, durable_key)
            .await
    }

    async fn ack_spatial_insert_in_flight(&self, batch_id: u64) {
        super::spatial_handlers::ack_spatial_insert_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_spatial_insert(&self, durable_key: Vec<u8>) {
        super::spatial_handlers::acknowledge_spatial_insert_impl(self, durable_key).await
    }

    async fn pending_spatial_deletes(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::spatial::PendingSpatialDelete,
    )> {
        super::spatial_handlers::pending_spatial_deletes_impl(self).await
    }

    async fn mark_spatial_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        super::spatial_handlers::mark_spatial_delete_in_flight_impl(self, batch_id, durable_key)
            .await
    }

    async fn ack_spatial_delete_in_flight(&self, batch_id: u64) {
        super::spatial_handlers::ack_spatial_delete_in_flight_impl(self, batch_id).await
    }

    async fn acknowledge_spatial_delete(&self, durable_key: Vec<u8>) {
        super::spatial_handlers::acknowledge_spatial_delete_impl(self, durable_key).await
    }

    async fn pending_timeseries_batches(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::timeseries::PendingTimeseriesBatch,
    )> {
        super::timeseries_handlers::pending_timeseries_batches_impl(self).await
    }

    async fn mark_timeseries_batch_in_flight(
        &self,
        stream_seq: u64,
        batch_id: u64,
        durable_key: Vec<u8>,
    ) {
        super::timeseries_handlers::mark_timeseries_batch_in_flight_impl(
            self,
            stream_seq,
            batch_id,
            durable_key,
        )
        .await
    }

    async fn ack_timeseries_batches_through_seq(&self, applied_seq: u64) {
        super::timeseries_handlers::ack_timeseries_batches_through_seq_impl(self, applied_seq).await
    }

    async fn ack_timeseries_batch_by_id(&self, batch_id: u64) {
        super::timeseries_handlers::ack_timeseries_batch_by_id_impl(self, batch_id).await
    }

    async fn acknowledge_timeseries_batch(&self, durable_key: Vec<u8>) {
        super::timeseries_handlers::acknowledge_timeseries_batch_impl(self, durable_key).await
    }

    async fn pending_kv_writes(
        &self,
    ) -> Result<Vec<(Vec<u8>, crate::sync::outbound::kv::PendingKvWrite)>, crate::error::LiteError>
    {
        super::kv_handlers::pending_kv_writes_impl(self).await
    }

    async fn persist_kv_write_seq(
        &self,
        key: &[u8],
        write: &crate::sync::outbound::kv::PendingKvWrite,
    ) -> Result<(), crate::error::LiteError> {
        super::kv_handlers::persist_kv_write_seq_impl(self, key, write).await
    }

    async fn mark_kv_write_in_flight(&self, batch_id: u64) {
        super::kv_handlers::mark_kv_write_in_flight_impl(self, batch_id).await
    }

    async fn retire_kv_write(&self, batch_id: u64) -> Result<(), crate::error::LiteError> {
        super::kv_handlers::retire_kv_write_impl(self, batch_id).await
    }

    async fn clear_engine_in_flight(&self) {
        super::outbound_state::clear_engine_in_flight_impl(self).await
    }

    async fn next_stream_seq(&self, stream_id: u64) -> u64 {
        super::outbound_state::next_stream_seq_impl(self, stream_id).await
    }

    async fn record_stream_ack(&self, stream_id: u64, applied_seq: u64) {
        super::outbound_state::record_stream_ack_impl(self, stream_id, applied_seq).await
    }

    async fn persist_producer_state(&self, producer_id: u64, accepted_epoch: u64) {
        super::producer_state::persist_producer_state_impl(self, producer_id, accepted_epoch).await
    }

    async fn load_producer_state(&self) -> (u64, u64) {
        super::producer_state::load_producer_state_impl(self).await
    }

    async fn import_definition(&self, msg: &nodedb_types::sync::wire::DefinitionSyncMsg) {
        if let Err(e) = super::definition_apply::apply_definition_sync(self, msg).await {
            tracing::warn!(
                definition_type = %msg.definition_type,
                name = %msg.name,
                error = %e,
                "definition sync failed"
            );
        }
    }

    async fn import_collection_schema(
        &self,
        msg: &nodedb_types::sync::wire::CollectionSchemaSyncMsg,
    ) {
        if let Err(e) = self
            .register_collection_from_descriptor(&msg.descriptor)
            .await
        {
            tracing::warn!(
                collection = %msg.descriptor.name,
                error = %e,
                "collection schema sync failed"
            );
        }
    }

    async fn get_collection_meta(
        &self,
        name: &str,
    ) -> Option<crate::nodedb::collection::CollectionMeta> {
        super::producer_state::get_collection_meta_impl(self, name).await
    }

    // ── Stable seq persistence ────────────────────────────────────────────────

    async fn persist_columnar_seq(
        &self,
        key: &[u8],
        batch: &crate::sync::outbound::columnar::PendingColumnarBatch,
    ) -> Result<(), crate::error::LiteError> {
        super::columnar_handlers::persist_columnar_seq_impl(self, key, batch).await
    }

    async fn persist_timeseries_seq(
        &self,
        key: &[u8],
        batch: &crate::sync::outbound::timeseries::PendingTimeseriesBatch,
    ) -> Result<(), crate::error::LiteError> {
        super::timeseries_handlers::persist_timeseries_seq_impl(self, key, batch).await
    }

    async fn persist_vector_insert_seq(
        &self,
        key: &[u8],
        insert: &crate::sync::outbound::vector::PendingVectorInsert,
    ) -> Result<(), crate::error::LiteError> {
        super::vector_handlers::persist_vector_insert_seq_impl(self, key, insert).await
    }

    async fn persist_vector_delete_seq(
        &self,
        key: &[u8],
        delete: &crate::sync::outbound::vector::PendingVectorDelete,
    ) -> Result<(), crate::error::LiteError> {
        super::vector_handlers::persist_vector_delete_seq_impl(self, key, delete).await
    }

    async fn persist_fts_index_seq(
        &self,
        key: &[u8],
        entry: &crate::sync::outbound::fts::PendingFtsIndex,
    ) -> Result<(), crate::error::LiteError> {
        super::fts_handlers::persist_fts_index_seq_impl(self, key, entry).await
    }

    async fn persist_fts_delete_seq(
        &self,
        key: &[u8],
        entry: &crate::sync::outbound::fts::PendingFtsDelete,
    ) -> Result<(), crate::error::LiteError> {
        super::fts_handlers::persist_fts_delete_seq_impl(self, key, entry).await
    }

    async fn persist_spatial_insert_seq(
        &self,
        key: &[u8],
        insert: &crate::sync::outbound::spatial::PendingSpatialInsert,
    ) -> Result<(), crate::error::LiteError> {
        super::spatial_handlers::persist_spatial_insert_seq_impl(self, key, insert).await
    }

    async fn persist_spatial_delete_seq(
        &self,
        key: &[u8],
        delete: &crate::sync::outbound::spatial::PendingSpatialDelete,
    ) -> Result<(), crate::error::LiteError> {
        super::spatial_handlers::persist_spatial_delete_seq_impl(self, key, delete).await
    }
}
