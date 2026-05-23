// SPDX-License-Identifier: Apache-2.0
//! MetaOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::MetaOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::meta_ops;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &MetaOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        MetaOp::CreateSnapshot => Ok(Box::pin(async move {
            meta_ops::handle_create_snapshot(engine).await
        })),
        MetaOp::Compact => Ok(Box::pin(
            async move { meta_ops::handle_compact(engine).await },
        )),
        MetaOp::Checkpoint => Ok(Box::pin(async move {
            meta_ops::handle_checkpoint(engine).await
        })),
        MetaOp::UnregisterCollection {
            tenant_id,
            name,
            purge_lsn,
        } => {
            let tid = *tenant_id;
            let n = name.clone();
            let lsn = *purge_lsn;
            Ok(Box::pin(async move {
                meta_ops::handle_unregister_collection(engine, tid, &n, lsn).await
            }))
        }
        MetaOp::UnregisterMaterializedView { tenant_id, name } => {
            let tid = *tenant_id;
            let n = name.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_unregister_materialized_view(engine, tid, &n).await
            }))
        }
        MetaOp::RenameCollection {
            tenant_id,
            old_collection,
            new_collection,
        } => {
            let tid = *tenant_id;
            let old = old_collection.clone();
            let new = new_collection.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_rename_collection(engine, tid, &old, &new).await
            }))
        }
        MetaOp::ConvertCollection {
            collection,
            target_type,
            schema_json,
        } => {
            let col = collection.clone();
            let tt = target_type.clone();
            let sj = schema_json.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_convert_collection(engine, &col, &tt, &sj).await
            }))
        }
        MetaOp::RegisterContinuousAggregate { def } => {
            let d = def.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_register_continuous_aggregate(engine, d).await
            }))
        }
        MetaOp::UnregisterContinuousAggregate { name } => {
            let n = name.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_unregister_continuous_aggregate(engine, &n).await
            }))
        }
        MetaOp::ListContinuousAggregates => Ok(Box::pin(async move {
            meta_ops::handle_list_continuous_aggregates(engine).await
        })),
        MetaOp::ApplyContinuousAggRetention => Ok(Box::pin(async move {
            meta_ops::handle_apply_continuous_agg_retention(engine).await
        })),
        MetaOp::QueryAggregateWatermark { aggregate_name } => {
            let n = aggregate_name.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_query_aggregate_watermark(engine, &n).await
            }))
        }
        MetaOp::QueryLastValues { collection } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_query_aggregate_last_values(engine, &col).await
            }))
        }
        MetaOp::QueryLastValue {
            collection,
            series_id,
        } => {
            let col = collection.clone();
            let sid = *series_id;
            Ok(Box::pin(async move {
                meta_ops::handle_query_aggregate_last_value(engine, &col, sid).await
            }))
        }
        MetaOp::TemporalPurgeEdgeStore {
            tenant_id,
            collection,
            cutoff_system_ms,
        } => {
            let tid = *tenant_id;
            let col = collection.clone();
            let cut = *cutoff_system_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_temporal_purge_edge_store(engine, tid, &col, cut).await
            }))
        }
        MetaOp::TemporalPurgeDocumentStrict {
            tenant_id,
            collection,
            cutoff_system_ms,
        } => {
            let tid = *tenant_id;
            let col = collection.clone();
            let cut = *cutoff_system_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_temporal_purge_document_strict(engine, tid, &col, cut).await
            }))
        }
        MetaOp::TemporalPurgeColumnar {
            tenant_id,
            collection,
            cutoff_system_ms,
        } => {
            let tid = *tenant_id;
            let col = collection.clone();
            let cut = *cutoff_system_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_temporal_purge_columnar(engine, tid, &col, cut).await
            }))
        }
        MetaOp::TemporalPurgeCrdt {
            tenant_id,
            collection,
            cutoff_system_ms,
        } => {
            let tid = *tenant_id;
            let col = collection.clone();
            let cut = *cutoff_system_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_temporal_purge_crdt(engine, tid, &col, cut).await
            }))
        }
        MetaOp::TemporalPurgeArray {
            tenant_id,
            array_id,
            cutoff_system_ms,
        } => {
            let tid = *tenant_id;
            let aid = array_id.clone();
            let cut = *cutoff_system_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_temporal_purge_array(engine, tid, &aid, cut).await
            }))
        }
        MetaOp::EnforceTimeseriesRetention {
            collection,
            max_age_ms,
        } => {
            let col = collection.clone();
            let age = *max_age_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_enforce_timeseries_retention(engine, &col, age).await
            }))
        }
        MetaOp::AlterArray {
            array_id,
            audit_retain_ms,
            minimum_audit_retain_ms,
        } => {
            let aid = array_id.clone();
            let arm = *audit_retain_ms;
            let marm = *minimum_audit_retain_ms;
            Ok(Box::pin(async move {
                meta_ops::handle_alter_array(engine, &aid, arm, marm).await
            }))
        }
        MetaOp::PutSynonymGroup {
            tenant_id,
            record_json,
        } => {
            let tid = *tenant_id;
            let rj = record_json.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_put_synonym_group(engine, tid, &rj).await
            }))
        }
        MetaOp::DeleteSynonymGroup { tenant_id, name } => {
            let tid = *tenant_id;
            let n = name.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_delete_synonym_group(engine, tid, &n).await
            }))
        }
        MetaOp::RebuildIndex {
            collection,
            index_name,
            concurrent,
        } => {
            let col = collection.clone();
            let idx = index_name.clone();
            let conc = *concurrent;
            Ok(Box::pin(async move {
                meta_ops::handle_rebuild_index(engine, &col, idx.as_deref(), conc).await
            }))
        }
        MetaOp::QueryCollectionSize { tenant_id, name } => {
            let tid = *tenant_id;
            let n = name.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_query_collection_size(engine, tid, &n).await
            }))
        }
        // ── Distributed ops implemented on Lite ─────────────────────────────
        MetaOp::WalAppend { payload } => {
            let bytes = payload.clone();
            let storage = engine.storage.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_wal_append(&storage, &bytes).await
            }))
        }
        MetaOp::Cancel { target_request_id } => {
            let rid = *target_request_id;
            let registry = engine.cancellation.clone();
            Ok(Box::pin(
                async move { meta_ops::handle_cancel(&registry, rid) },
            ))
        }
        MetaOp::TransactionBatch { plans } => {
            let plans = plans.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_txn_batch(engine, &plans).await
            }))
        }
        MetaOp::CalvinExecuteStatic { plans, .. } => {
            let plans = plans.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_calvin_static(engine, &plans).await
            }))
        }
        MetaOp::CalvinExecutePassive { .. } => {
            Ok(Box::pin(
                async move { meta_ops::handle_calvin_passive().await },
            ))
        }
        MetaOp::CalvinExecuteActive { plans, .. } => {
            let plans = plans.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_calvin_active(engine, &plans).await
            }))
        }
        // ── Origin-only ops that Lite's plan converter never emits ───────────
        MetaOp::RawResponse { .. } => {
            meta_ops::handle_raw_response();
        }
        MetaOp::CreateTenantSnapshot { tenant_id } => {
            let tid = *tenant_id;
            let storage = engine.storage.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_create_tenant_snapshot(&*storage, tid).await
            }))
        }
        MetaOp::RestoreTenantSnapshot {
            tenant_id,
            snapshot,
        } => {
            let tid = *tenant_id;
            let snap = snapshot.clone();
            let storage = engine.storage.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_restore_tenant_snapshot(&*storage, tid, &snap).await
            }))
        }
        MetaOp::PurgeTenant { tenant_id } => {
            let tid = *tenant_id;
            let storage = engine.storage.clone();
            Ok(Box::pin(async move {
                meta_ops::handle_purge_tenant(&*storage, tid).await
            }))
        }
    }
}
