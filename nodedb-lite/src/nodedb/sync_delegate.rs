//! `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.

#[cfg(not(target_arch = "wasm32"))]
use crate::storage::engine::{StorageEngine, StorageEngineSync};

#[cfg(not(target_arch = "wasm32"))]
use super::core::NodeDbLite;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl<S: StorageEngine + StorageEngineSync> crate::sync::SyncDelegate for NodeDbLite<S> {
    fn pending_deltas(&self) -> Vec<crate::engine::crdt::engine::PendingDelta> {
        self.pending_crdt_deltas().unwrap_or_default()
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
        use super::lock_ext::LockExt;

        let mut crdt = self.crdt.lock_or_recover();
        match crdt.reject_delta_with_policy(mutation_id, hint) {
            Some(nodedb_crdt::PolicyResolution::AutoResolved(action)) => {
                tracing::info!(
                    mutation_id,
                    action = ?action,
                    "SyncDelegate: delta auto-resolved by policy"
                );
            }
            Some(nodedb_crdt::PolicyResolution::Deferred {
                retry_after_ms,
                attempt,
            }) => {
                tracing::info!(
                    mutation_id,
                    retry_after_ms,
                    attempt,
                    "SyncDelegate: delta deferred for retry"
                );
            }
            Some(nodedb_crdt::PolicyResolution::Escalate) => {
                tracing::warn!(mutation_id, "SyncDelegate: delta escalated to DLQ (policy)");
            }
            Some(nodedb_crdt::PolicyResolution::WebhookRequired { webhook_url, .. }) => {
                tracing::warn!(
                    mutation_id,
                    webhook_url,
                    "SyncDelegate: delta requires webhook (not supported on Lite)"
                );
                // Fallback: treat as escalate.
                let _ = crdt.reject_delta(mutation_id);
            }
            None => {
                tracing::debug!(
                    mutation_id,
                    "SyncDelegate: reject_with_policy — delta not found"
                );
            }
        }
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
        use crate::sync::array::inbound::outcome::InboundOutcome;
        use nodedb_array::sync::op_codec;

        // Decode op to extract HLC for the ack before calling the inbound handler.
        let op = match op_codec::decode_op(&msg.op_payload) {
            Ok(op) => op,
            Err(e) => {
                tracing::warn!(
                    array = %msg.array,
                    error = %e,
                    "SyncDelegate::handle_array_delta: decode failed"
                );
                return None;
            }
        };
        let op_hlc = op.header.hlc;
        let replica_id = self.array_inbound.replica_id();

        match self.array_inbound.handle_delta(msg) {
            Ok(InboundOutcome::Applied) => Some(nodedb_types::sync::wire::ArrayAckMsg {
                array: msg.array.clone(),
                replica_id,
                ack_hlc_bytes: op_hlc.to_bytes(),
            }),
            Ok(_) => None,
            Err(e) => {
                tracing::warn!(
                    array = %msg.array,
                    error = %e,
                    "SyncDelegate::handle_array_delta: apply failed"
                );
                None
            }
        }
    }

    fn handle_array_delta_batch(
        &self,
        msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        use crate::sync::array::inbound::outcome::InboundOutcome;
        use nodedb_array::sync::op_codec;

        // Decode all ops upfront to extract HLCs.
        let ops: Vec<_> = msg
            .op_payloads
            .iter()
            .filter_map(|payload| match op_codec::decode_op(payload) {
                Ok(op) => Some(op),
                Err(e) => {
                    tracing::warn!(
                        array = %msg.array,
                        error = %e,
                        "SyncDelegate::handle_array_delta_batch: decode failed; skipping op"
                    );
                    None
                }
            })
            .collect();

        let replica_id = self.array_inbound.replica_id();

        match self.array_inbound.handle_delta_batch(msg) {
            Ok(outcomes) => {
                // Find the highest HLC among successfully applied ops.
                let mut latest_hlc = None;
                for (outcome, op) in outcomes.iter().zip(ops.iter()) {
                    if *outcome == InboundOutcome::Applied {
                        let hlc = op.header.hlc;
                        match latest_hlc {
                            None => latest_hlc = Some(hlc),
                            Some(prev) if hlc > prev => latest_hlc = Some(hlc),
                            _ => {}
                        }
                    }
                }
                latest_hlc.map(|hlc| nodedb_types::sync::wire::ArrayAckMsg {
                    array: msg.array.clone(),
                    replica_id,
                    ack_hlc_bytes: hlc.to_bytes(),
                })
            }
            Err(e) => {
                tracing::warn!(
                    array = %msg.array,
                    error = %e,
                    "SyncDelegate::handle_array_delta_batch: apply failed"
                );
                None
            }
        }
    }

    fn handle_array_reject(&self, msg: &nodedb_types::sync::wire::ArrayRejectMsg) {
        if let Err(e) = self.array_inbound.handle_reject(msg) {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "SyncDelegate::handle_array_reject: failed"
            );
        }
    }

    fn pending_columnar_batches(
        &self,
    ) -> Vec<crate::sync::outbound::columnar::PendingColumnarBatch> {
        self.columnar_outbound
            .as_ref()
            .map(|q| q.drain_pending())
            .unwrap_or_default()
    }

    fn acknowledge_columnar_batch(&self, batch_id: u64) {
        if let Some(q) = &self.columnar_outbound {
            q.acknowledge_batch(batch_id);
        }
    }

    fn reject_columnar_batch(&self, batch: crate::sync::outbound::columnar::PendingColumnarBatch) {
        if let Some(q) = &self.columnar_outbound {
            q.requeue_batch(batch);
        }
    }

    fn pending_vector_inserts(&self) -> Vec<crate::sync::outbound::vector::PendingVectorInsert> {
        self.vector_outbound
            .as_ref()
            .map(|q| q.drain_inserts())
            .unwrap_or_default()
    }

    fn acknowledge_vector_insert(&self, batch_id: u64) {
        if let Some(q) = &self.vector_outbound {
            q.acknowledge_insert(batch_id);
        }
    }

    fn reject_vector_insert(&self, entry: crate::sync::outbound::vector::PendingVectorInsert) {
        if let Some(q) = &self.vector_outbound {
            q.requeue_insert(entry);
        }
    }

    fn pending_vector_deletes(&self) -> Vec<crate::sync::outbound::vector::PendingVectorDelete> {
        self.vector_outbound
            .as_ref()
            .map(|q| q.drain_deletes())
            .unwrap_or_default()
    }

    fn acknowledge_vector_delete(&self, batch_id: u64) {
        if let Some(q) = &self.vector_outbound {
            q.acknowledge_delete(batch_id);
        }
    }

    fn reject_vector_delete(&self, entry: crate::sync::outbound::vector::PendingVectorDelete) {
        if let Some(q) = &self.vector_outbound {
            q.requeue_delete(entry);
        }
    }

    fn pending_fts_indexes(&self) -> Vec<crate::sync::outbound::fts::PendingFtsIndex> {
        self.fts_outbound
            .as_ref()
            .map(|q| q.drain_indexes())
            .unwrap_or_default()
    }

    fn acknowledge_fts_index(&self, batch_id: u64) {
        if let Some(q) = &self.fts_outbound {
            q.acknowledge_index(batch_id);
        }
    }

    fn reject_fts_index(&self, entry: crate::sync::outbound::fts::PendingFtsIndex) {
        if let Some(q) = &self.fts_outbound {
            q.requeue_index(entry);
        }
    }

    fn pending_fts_deletes(&self) -> Vec<crate::sync::outbound::fts::PendingFtsDelete> {
        self.fts_outbound
            .as_ref()
            .map(|q| q.drain_deletes())
            .unwrap_or_default()
    }

    fn acknowledge_fts_delete(&self, batch_id: u64) {
        if let Some(q) = &self.fts_outbound {
            q.acknowledge_delete(batch_id);
        }
    }

    fn reject_fts_delete(&self, entry: crate::sync::outbound::fts::PendingFtsDelete) {
        if let Some(q) = &self.fts_outbound {
            q.requeue_delete(entry);
        }
    }

    fn pending_spatial_inserts(&self) -> Vec<crate::sync::outbound::spatial::PendingSpatialInsert> {
        self.spatial_outbound
            .as_ref()
            .map(|q| q.drain_inserts())
            .unwrap_or_default()
    }

    fn acknowledge_spatial_insert(&self, batch_id: u64) {
        if let Some(q) = &self.spatial_outbound {
            q.acknowledge_insert(batch_id);
        }
    }

    fn reject_spatial_insert(&self, entry: crate::sync::outbound::spatial::PendingSpatialInsert) {
        if let Some(q) = &self.spatial_outbound {
            q.requeue_insert(entry);
        }
    }

    fn pending_spatial_deletes(&self) -> Vec<crate::sync::outbound::spatial::PendingSpatialDelete> {
        self.spatial_outbound
            .as_ref()
            .map(|q| q.drain_deletes())
            .unwrap_or_default()
    }

    fn acknowledge_spatial_delete(&self, batch_id: u64) {
        if let Some(q) = &self.spatial_outbound {
            q.acknowledge_delete(batch_id);
        }
    }

    fn reject_spatial_delete(&self, entry: crate::sync::outbound::spatial::PendingSpatialDelete) {
        if let Some(q) = &self.spatial_outbound {
            q.requeue_delete(entry);
        }
    }

    fn pending_timeseries_batches(
        &self,
    ) -> Vec<crate::sync::outbound::timeseries::PendingTimeseriesBatch> {
        self.timeseries_outbound
            .as_ref()
            .map(|q| q.drain_pending())
            .unwrap_or_default()
    }

    fn acknowledge_timeseries_collection(&self, collection: &str) {
        if let Some(q) = &self.timeseries_outbound {
            q.acknowledge_collection(collection);
        }
    }

    fn reject_timeseries_batch(
        &self,
        batch: crate::sync::outbound::timeseries::PendingTimeseriesBatch,
    ) {
        if let Some(q) = &self.timeseries_outbound {
            q.requeue_batch(batch);
        }
    }

    async fn import_definition(&self, msg: &nodedb_types::sync::wire::DefinitionSyncMsg) {
        use super::definitions::*;

        let result = match (msg.definition_type.as_str(), msg.action.as_str()) {
            ("function", "put") => match sonic_rs::from_slice::<LiteStoredFunction>(&msg.payload) {
                Ok(func) => self.put_function(&func).await,
                Err(e) => {
                    tracing::warn!(name = %msg.name, error = %e, "failed to deserialize function");
                    return;
                }
            },
            ("function", "delete") => self.delete_function(&msg.name).await,
            ("trigger", "put") => match sonic_rs::from_slice::<LiteStoredTrigger>(&msg.payload) {
                Ok(trigger) => self.put_trigger(&trigger).await,
                Err(e) => {
                    tracing::warn!(name = %msg.name, error = %e, "failed to deserialize trigger");
                    return;
                }
            },
            ("trigger", "delete") => self.delete_trigger(&msg.name).await,
            ("procedure", "put") => {
                match sonic_rs::from_slice::<LiteStoredProcedure>(&msg.payload) {
                    Ok(p) => self.put_procedure(&p).await,
                    Err(e) => {
                        tracing::warn!(name = %msg.name, error = %e, "failed to deserialize procedure");
                        return;
                    }
                }
            }
            ("procedure", "delete") => self.delete_procedure(&msg.name).await,
            _ => {
                tracing::warn!(
                    definition_type = %msg.definition_type,
                    action = %msg.action,
                    "unknown definition type/action"
                );
                return;
            }
        };

        if let Err(e) = result {
            tracing::warn!(
                definition_type = %msg.definition_type,
                name = %msg.name,
                error = %e,
                "definition sync failed"
            );
        }
    }
}
