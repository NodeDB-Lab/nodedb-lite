// SPDX-License-Identifier: Apache-2.0
//! DocumentOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::RlsWriteCheck;

use crate::error::LiteError;
use crate::query::document_ops;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;
use super::policy::deny_policy;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &DocumentOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        DocumentOp::PointGet {
            collection,
            document_id,
            rls_filters,
            ..
        } => {
            // PointGet carries no write-check slot: it never writes.
            deny_policy(
                "DocumentOp::PointGet",
                None,
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            let col = collection.clone();
            let doc_id = document_id.clone();
            Ok(Box::pin(async move {
                document_ops::reads::point_get(engine, col.as_str(), &doc_id).await
            }))
        }

        DocumentOp::Scan {
            collection,
            limit,
            offset,
            ..
        } => {
            let col = collection.clone();
            let limit = *limit;
            let offset = *offset;
            Ok(Box::pin(async move {
                document_ops::reads::scan(engine, col.as_str(), limit, offset).await
            }))
        }

        DocumentOp::RangeScan {
            collection,
            lower,
            upper,
            limit,
            rls_filters,
            ..
        } => {
            // RangeScan carries no write-check slot: it never writes.
            deny_policy(
                "DocumentOp::RangeScan",
                None,
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            let col = collection.clone();
            let lower = lower.clone();
            let upper = upper.clone();
            let limit = *limit;
            Ok(Box::pin(async move {
                document_ops::reads::range_scan(
                    engine,
                    col.as_str(),
                    lower.as_deref(),
                    upper.as_deref(),
                    limit,
                )
                .await
            }))
        }

        DocumentOp::IndexedFetch {
            collection,
            path,
            value,
            limit,
            offset,
            ..
        } => {
            let col = collection.clone();
            let path = path.clone();
            let value = value.clone();
            let limit = *limit;
            let offset = *offset;
            Ok(Box::pin(async move {
                document_ops::reads::indexed_fetch(
                    engine,
                    col.as_str(),
                    &path,
                    &value,
                    limit,
                    offset,
                )
                .await
            }))
        }

        DocumentOp::IndexLookup {
            collection,
            path,
            value,
        } => {
            let col = collection.clone();
            let path = path.clone();
            let value = value.clone();
            Ok(Box::pin(async move {
                document_ops::reads::index_lookup(engine, col.as_str(), &path, &value).await
            }))
        }

        DocumentOp::EstimateCount { collection, .. } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::reads::estimate_count(engine, col.as_str()).await
            }))
        }

        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            returning,
            rls_filters,
            ..
        } => {
            // PointPut has no rls_write_check slot: unconditional-overwrite
            // upsert semantics carry no write gate.
            deny_policy(
                "DocumentOp::PointPut",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            let col = collection.clone();
            let doc_id = document_id.clone();
            let val = value.clone();
            Ok(Box::pin(async move {
                document_ops::writes::point_put(engine, col.as_str(), &doc_id, &val).await
            }))
        }

        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            returning,
            rls_filters,
            ..
        } => {
            // PointInsert has no rls_write_check slot: an unconditional
            // first write carries no write gate.
            deny_policy(
                "DocumentOp::PointInsert",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            let col = collection.clone();
            let doc_id = document_id.clone();
            let val = value.clone();
            let if_absent = *if_absent;
            Ok(Box::pin(async move {
                document_ops::writes::point_insert(engine, col.as_str(), &doc_id, &val, if_absent)
                    .await
            }))
        }

        DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            ..
        } => {
            deny_policy(
                "DocumentOp::PointUpdate",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            let doc_id = document_id.clone();
            let updates = updates.clone();
            Ok(Box::pin(async move {
                document_ops::writes::point_update(engine, col.as_str(), &doc_id, &updates).await
            }))
        }

        DocumentOp::PointDelete {
            collection,
            document_id,
            returning,
            rls_filters,
            rls_write_check,
            ..
        } => {
            deny_policy(
                "DocumentOp::PointDelete",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            let doc_id = document_id.clone();
            Ok(Box::pin(async move {
                document_ops::writes::point_delete(engine, col.as_str(), &doc_id).await
            }))
        }

        DocumentOp::BatchInsert {
            collection,
            documents,
            returning,
            rls_filters,
            ..
        } => {
            // BatchInsert has no rls_write_check slot: an unconditional
            // batch of first writes carries no write gate.
            deny_policy(
                "DocumentOp::BatchInsert",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            let col = collection.clone();
            let docs = documents.clone();
            Ok(Box::pin(async move {
                document_ops::writes::batch_insert(engine, col.as_str(), &docs).await
            }))
        }

        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
            ..
        } => {
            deny_policy(
                "DocumentOp::Upsert",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            let doc_id = document_id.clone();
            let val = value.clone();
            let conflict_updates = on_conflict_updates.clone();
            Ok(Box::pin(async move {
                document_ops::writes::upsert(engine, col.as_str(), &doc_id, &val, &conflict_updates)
                    .await
            }))
        }

        DocumentOp::Truncate { collection, .. } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::writes::truncate(engine, col.as_str()).await
            }))
        }

        DocumentOp::BulkUpdate {
            collection,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            ..
        } => {
            deny_policy(
                "DocumentOp::BulkUpdate",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            let updates = updates.clone();
            Ok(Box::pin(async move {
                document_ops::writes::bulk_update(engine, col.as_str(), &updates).await
            }))
        }

        DocumentOp::BulkDelete {
            collection,
            returning,
            rls_filters,
            rls_write_check,
            ..
        } => {
            deny_policy(
                "DocumentOp::BulkDelete",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::writes::bulk_delete(engine, col.as_str()).await
            }))
        }

        DocumentOp::Register {
            collection,
            storage_mode,
            ..
        } => {
            let col = collection.clone();
            let mode = storage_mode.clone();
            Ok(Box::pin(async move {
                document_ops::indexes::register(engine, col.as_str(), &mode).await
            }))
        }

        DocumentOp::DropIndex { collection, field } => {
            let col = collection.clone();
            let field = field.clone();
            Ok(Box::pin(async move {
                document_ops::indexes::drop_index(engine, col.as_str(), &field).await
            }))
        }

        DocumentOp::BackfillIndex {
            collection, path, ..
        } => {
            let col = collection.clone();
            let path = path.clone();
            Ok(Box::pin(async move {
                document_ops::indexes::backfill_index(engine, col.as_str(), &path).await
            }))
        }

        DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_limit,
            ..
        } => {
            let target = target_collection.clone();
            let source = source_collection.clone();
            let limit = *source_limit;
            Ok(Box::pin(async move {
                document_ops::sets::insert_select(engine, target.as_str(), source.as_str(), limit)
                    .await
            }))
        }

        DocumentOp::UpdateFromJoin {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            ..
        } => {
            deny_policy(
                "DocumentOp::UpdateFromJoin",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let target = target_collection.clone();
            let source = source_collection.clone();
            let alias = source_alias.clone();
            let target_join = target_join_col.clone();
            let source_join = source_join_col.clone();
            let updates = updates.clone();
            Ok(Box::pin(async move {
                document_ops::sets::update_from_join(
                    engine,
                    target.as_str(),
                    source.as_str(),
                    &alias,
                    &target_join,
                    &source_join,
                    &updates,
                )
                .await
            }))
        }

        DocumentOp::Merge {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            clauses,
            returning,
            rls_filters,
            rls_write_check,
            ..
        } => {
            deny_policy(
                "DocumentOp::Merge",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let target = target_collection.clone();
            let source = source_collection.clone();
            let alias = source_alias.clone();
            let target_join = target_join_col.clone();
            let source_join = source_join_col.clone();
            let clauses = clauses.clone();
            Ok(Box::pin(async move {
                document_ops::sets::merge(
                    engine,
                    target.as_str(),
                    source.as_str(),
                    &alias,
                    &target_join,
                    &source_join,
                    &clauses,
                )
                .await
            }))
        }

        DocumentOp::MaterializeScan {
            collection,
            cursor,
            count,
            ..
        } => {
            let col = collection.clone();
            let cursor = cursor.clone();
            let count = *count;
            Ok(Box::pin(async move {
                document_ops::sets::materialize_scan(engine, col.as_str(), &cursor, count).await
            }))
        }

        // A materialized-sum binding splits its write across the source row and
        // the target balance, which Origin homes on separate vShards and commits
        // through Calvin. Lite has no binding maintenance, so a plan carrying
        // this op would apply the source write and silently lose the balance.
        DocumentOp::ApplyBalanceDelta {
            collection, column, ..
        } => Err(LiteError::Unsupported {
            detail: format!(
                "ApplyBalanceDelta on {collection}.{column}: materialized-sum \
                 bindings are maintained by the Origin data plane and are \
                 unsupported on the Lite engine"
            ),
        }),

        // ResolveWrite/ResolvedWrite split a governed write into a resolve
        // pass and a replay pass so every replica applies the same decision.
        // Lite has no replica set to keep in sync, so its SQL visitor and CRDT
        // sync execute Merge/UpdateFromJoin/PointUpdate/etc. directly and
        // never wrap them in this pair.
        DocumentOp::ResolveWrite(_) => Err(LiteError::Unsupported {
            detail: "DocumentOp::ResolveWrite is the resolve pass of a governed \
                     write Origin replays across replicas; Lite's single-node \
                     engine executes writes directly and never emits it"
                .into(),
        }),

        DocumentOp::ResolvedWrite { .. } => Err(LiteError::Unsupported {
            detail: "DocumentOp::ResolvedWrite replays a decision made by \
                     Origin's Raft leader, which has no equivalent on the \
                     single-node Lite engine"
                .into(),
        }),
    }
}
