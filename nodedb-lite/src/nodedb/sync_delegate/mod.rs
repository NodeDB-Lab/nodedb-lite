//! `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.

mod array_handlers;
mod definition_apply;
mod import_collection_schema;

#[cfg(not(target_arch = "wasm32"))]
use crate::storage::engine::StorageEngine;

/// Durable storage key for the Origin-assigned producer ID.
#[cfg(not(target_arch = "wasm32"))]
const META_SYNC_PRODUCER_ID: &[u8] = b"sync.producer_id";

/// Durable storage key for the Origin-echoed accepted epoch.
#[cfg(not(target_arch = "wasm32"))]
const META_SYNC_ACCEPTED_EPOCH: &[u8] = b"sync.accepted_epoch";

#[cfg(not(target_arch = "wasm32"))]
use super::core::NodeDbLite;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl<S: StorageEngine> crate::sync::SyncDelegate for NodeDbLite<S> {
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
        array_handlers::handle_reject_with_policy_impl(self, mutation_id, hint);
    }

    fn import_remote(&self, data: &[u8]) {
        if let Err(e) = self.import_remote_deltas(data) {
            tracing::warn!(error = %e, "SyncDelegate: import_remote failed");
        }
    }

    fn handle_array_delta(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        array_handlers::handle_array_delta_impl(self, msg)
    }

    fn handle_array_delta_batch(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        array_handlers::handle_array_delta_batch_impl(self, msg)
    }

    fn handle_array_reject(&self, msg: &nodedb_types::sync::wire::ArrayRejectMsg) {
        array_handlers::handle_array_reject_impl(self, msg);
    }

    async fn pending_columnar_batches(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::columnar::PendingColumnarBatch,
    )> {
        match &self.columnar_outbound {
            Some(q) => q
                .drain_batch(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_columnar_batch_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.columnar_outbound {
            q.mark_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_columnar_batch_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.columnar_outbound
            && let Some(key) = q.ack_in_flight(batch_id).await
            && let Err(e) = q.ack_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "columnar in-flight ack_keys failed");
        }
    }

    async fn acknowledge_columnar_batch(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.columnar_outbound
            && let Err(e) = q.ack_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "columnar outbound ack_keys failed");
        }
    }

    async fn pending_vector_inserts(
        &self,
    ) -> Vec<(Vec<u8>, crate::sync::outbound::vector::PendingVectorInsert)> {
        match &self.vector_outbound {
            Some(q) => q
                .drain_inserts(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_vector_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.vector_outbound {
            q.mark_insert_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_vector_insert_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.vector_outbound
            && let Some(key) = q.ack_insert_in_flight(batch_id).await
            && let Err(e) = q.ack_insert_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "vector insert in-flight ack_keys failed");
        }
    }

    async fn acknowledge_vector_insert(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.vector_outbound
            && let Err(e) = q.ack_insert_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "vector insert outbound ack_keys failed");
        }
    }

    async fn pending_vector_deletes(
        &self,
    ) -> Vec<(Vec<u8>, crate::sync::outbound::vector::PendingVectorDelete)> {
        match &self.vector_outbound {
            Some(q) => q
                .drain_deletes(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_vector_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.vector_outbound {
            q.mark_delete_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_vector_delete_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.vector_outbound
            && let Some(key) = q.ack_delete_in_flight(batch_id).await
            && let Err(e) = q.ack_delete_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "vector delete in-flight ack_keys failed");
        }
    }

    async fn acknowledge_vector_delete(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.vector_outbound
            && let Err(e) = q.ack_delete_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "vector delete outbound ack_keys failed");
        }
    }

    async fn pending_fts_indexes(
        &self,
    ) -> Vec<(Vec<u8>, crate::sync::outbound::fts::PendingFtsIndex)> {
        match &self.fts_outbound {
            Some(q) => q
                .drain_indexes(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_fts_index_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.fts_outbound {
            q.mark_index_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_fts_index_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.fts_outbound
            && let Some(key) = q.ack_index_in_flight(batch_id).await
            && let Err(e) = q.ack_index_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "fts index in-flight ack_keys failed");
        }
    }

    async fn acknowledge_fts_index(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.fts_outbound
            && let Err(e) = q.ack_index_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "fts index outbound ack_keys failed");
        }
    }

    async fn pending_fts_deletes(
        &self,
    ) -> Vec<(Vec<u8>, crate::sync::outbound::fts::PendingFtsDelete)> {
        match &self.fts_outbound {
            Some(q) => q
                .drain_deletes(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_fts_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.fts_outbound {
            q.mark_delete_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_fts_delete_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.fts_outbound
            && let Some(key) = q.ack_delete_in_flight(batch_id).await
            && let Err(e) = q.ack_delete_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "fts delete in-flight ack_keys failed");
        }
    }

    async fn acknowledge_fts_delete(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.fts_outbound
            && let Err(e) = q.ack_delete_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "fts delete outbound ack_keys failed");
        }
    }

    async fn pending_spatial_inserts(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::spatial::PendingSpatialInsert,
    )> {
        match &self.spatial_outbound {
            Some(q) => q
                .drain_inserts(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_spatial_insert_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.spatial_outbound {
            q.mark_insert_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_spatial_insert_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.spatial_outbound
            && let Some(key) = q.ack_insert_in_flight(batch_id).await
            && let Err(e) = q.ack_insert_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "spatial insert in-flight ack_keys failed");
        }
    }

    async fn acknowledge_spatial_insert(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.spatial_outbound
            && let Err(e) = q.ack_insert_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "spatial insert outbound ack_keys failed");
        }
    }

    async fn pending_spatial_deletes(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::spatial::PendingSpatialDelete,
    )> {
        match &self.spatial_outbound {
            Some(q) => q
                .drain_deletes(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_spatial_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.spatial_outbound {
            q.mark_delete_in_flight(batch_id, durable_key).await;
        }
    }

    async fn ack_spatial_delete_in_flight(&self, batch_id: u64) {
        if let Some(q) = &self.spatial_outbound
            && let Some(key) = q.ack_delete_in_flight(batch_id).await
            && let Err(e) = q.ack_delete_keys(&[key]).await
        {
            tracing::warn!(batch_id, error = %e, "spatial delete in-flight ack_keys failed");
        }
    }

    async fn acknowledge_spatial_delete(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.spatial_outbound
            && let Err(e) = q.ack_delete_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "spatial delete outbound ack_keys failed");
        }
    }

    async fn pending_timeseries_batches(
        &self,
    ) -> Vec<(
        Vec<u8>,
        crate::sync::outbound::timeseries::PendingTimeseriesBatch,
    )> {
        match &self.timeseries_outbound {
            Some(q) => q
                .drain_batch(crate::sync::PUSH_DRAIN_LIMIT)
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    async fn mark_timeseries_batch_in_flight(&self, stream_seq: u64, durable_key: Vec<u8>) {
        if let Some(q) = &self.timeseries_outbound {
            q.mark_in_flight_by_seq(stream_seq, durable_key).await;
        }
    }

    async fn ack_timeseries_batches_through_seq(&self, applied_seq: u64) {
        if let Some(q) = &self.timeseries_outbound {
            let keys = q.ack_in_flight_through_seq(applied_seq).await;
            for key in keys {
                if let Err(e) = q.ack_keys(&[key]).await {
                    tracing::warn!(applied_seq, error = %e, "timeseries in-flight ack_keys failed");
                }
            }
        }
    }

    async fn acknowledge_timeseries_batch(&self, durable_key: Vec<u8>) {
        if let Some(q) = &self.timeseries_outbound
            && let Err(e) = q.ack_keys(&[durable_key]).await
        {
            tracing::warn!(error = %e, "timeseries outbound ack_keys failed");
        }
    }

    async fn clear_engine_in_flight(&self) {
        if let Some(q) = &self.columnar_outbound {
            q.clear_in_flight().await;
        }
        if let Some(q) = &self.timeseries_outbound {
            q.clear_in_flight().await;
        }
        if let Some(q) = &self.vector_outbound {
            q.clear_in_flight().await;
        }
        if let Some(q) = &self.fts_outbound {
            q.clear_in_flight().await;
        }
        if let Some(q) = &self.spatial_outbound {
            q.clear_in_flight().await;
        }
    }

    async fn next_stream_seq(&self, stream_id: u64) -> u64 {
        match self.stream_seq.next_seq(stream_id).await {
            Ok(seq) => seq,
            Err(e) => {
                tracing::warn!(
                    stream_id,
                    error = %e,
                    "SyncDelegate::next_stream_seq: persist failed; using sentinel 0"
                );
                0
            }
        }
    }

    async fn record_stream_ack(&self, stream_id: u64, applied_seq: u64) {
        if let Err(e) = self.stream_seq.record_ack(stream_id, applied_seq).await {
            tracing::warn!(
                stream_id,
                applied_seq,
                error = %e,
                "SyncDelegate::record_stream_ack: persist failed; ignoring"
            );
        }
    }

    async fn persist_producer_state(&self, producer_id: u64, accepted_epoch: u64) {
        let ns = nodedb_types::Namespace::Meta;
        if let Err(e) = self
            .storage
            .put(ns, META_SYNC_PRODUCER_ID, &producer_id.to_be_bytes())
            .await
        {
            tracing::warn!(error = %e, "SyncDelegate: persist_producer_state: producer_id write failed");
        }
        if let Err(e) = self
            .storage
            .put(ns, META_SYNC_ACCEPTED_EPOCH, &accepted_epoch.to_be_bytes())
            .await
        {
            tracing::warn!(error = %e, "SyncDelegate: persist_producer_state: accepted_epoch write failed");
        }
    }

    async fn load_producer_state(&self) -> (u64, u64) {
        let ns = nodedb_types::Namespace::Meta;
        let producer_id = match self.storage.get(ns, META_SYNC_PRODUCER_ID).await {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                u64::from_be_bytes(bytes.try_into().unwrap_or([0; 8]))
            }
            _ => 0,
        };
        let accepted_epoch = match self.storage.get(ns, META_SYNC_ACCEPTED_EPOCH).await {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                u64::from_be_bytes(bytes.try_into().unwrap_or([0; 8]))
            }
            _ => 0,
        };
        (producer_id, accepted_epoch)
    }

    async fn import_definition(&self, msg: &nodedb_types::sync::wire::DefinitionSyncMsg) {
        if let Err(e) = definition_apply::apply_definition_sync(self, msg).await {
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

    // ── Stable seq persistence ────────────────────────────────────────────────

    async fn persist_columnar_seq(
        &self,
        key: &[u8],
        batch: &crate::sync::outbound::columnar::PendingColumnarBatch,
    ) -> Result<(), crate::error::LiteError> {
        match &self.columnar_outbound {
            Some(q) => q.update_entry(key, batch).await,
            None => Ok(()),
        }
    }

    async fn persist_timeseries_seq(
        &self,
        key: &[u8],
        batch: &crate::sync::outbound::timeseries::PendingTimeseriesBatch,
    ) -> Result<(), crate::error::LiteError> {
        match &self.timeseries_outbound {
            Some(q) => q.update_entry(key, batch).await,
            None => Ok(()),
        }
    }

    async fn persist_vector_insert_seq(
        &self,
        key: &[u8],
        insert: &crate::sync::outbound::vector::PendingVectorInsert,
    ) -> Result<(), crate::error::LiteError> {
        match &self.vector_outbound {
            Some(q) => q.update_insert_entry(key, insert).await,
            None => Ok(()),
        }
    }

    async fn persist_vector_delete_seq(
        &self,
        key: &[u8],
        delete: &crate::sync::outbound::vector::PendingVectorDelete,
    ) -> Result<(), crate::error::LiteError> {
        match &self.vector_outbound {
            Some(q) => q.update_delete_entry(key, delete).await,
            None => Ok(()),
        }
    }

    async fn persist_fts_index_seq(
        &self,
        key: &[u8],
        entry: &crate::sync::outbound::fts::PendingFtsIndex,
    ) -> Result<(), crate::error::LiteError> {
        match &self.fts_outbound {
            Some(q) => q.update_index_entry(key, entry).await,
            None => Ok(()),
        }
    }

    async fn persist_fts_delete_seq(
        &self,
        key: &[u8],
        entry: &crate::sync::outbound::fts::PendingFtsDelete,
    ) -> Result<(), crate::error::LiteError> {
        match &self.fts_outbound {
            Some(q) => q.update_delete_entry(key, entry).await,
            None => Ok(()),
        }
    }

    async fn persist_spatial_insert_seq(
        &self,
        key: &[u8],
        insert: &crate::sync::outbound::spatial::PendingSpatialInsert,
    ) -> Result<(), crate::error::LiteError> {
        match &self.spatial_outbound {
            Some(q) => q.update_insert_entry(key, insert).await,
            None => Ok(()),
        }
    }

    async fn persist_spatial_delete_seq(
        &self,
        key: &[u8],
        delete: &crate::sync::outbound::spatial::PendingSpatialDelete,
    ) -> Result<(), crate::error::LiteError> {
        match &self.spatial_outbound {
            Some(q) => q.update_delete_entry(key, delete).await,
            None => Ok(()),
        }
    }
}
