// SPDX-License-Identifier: Apache-2.0
//! DocumentOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::DocumentOp;

use crate::error::LiteError;
use crate::query::document_ops;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &DocumentOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        DocumentOp::PointGet {
            collection,
            document_id,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            Ok(Box::pin(async move {
                document_ops::reads::point_get(engine, &col, &doc_id).await
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
                document_ops::reads::scan(engine, &col, limit, offset).await
            }))
        }

        DocumentOp::RangeScan {
            collection,
            lower,
            upper,
            limit,
            ..
        } => {
            let col = collection.clone();
            let lower = lower.clone();
            let upper = upper.clone();
            let limit = *limit;
            Ok(Box::pin(async move {
                document_ops::reads::range_scan(
                    engine,
                    &col,
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
                document_ops::reads::indexed_fetch(engine, &col, &path, &value, limit, offset).await
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
                document_ops::reads::index_lookup(engine, &col, &path, &value).await
            }))
        }

        DocumentOp::EstimateCount { collection, .. } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::reads::estimate_count(engine, &col).await
            }))
        }

        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let val = value.clone();
            Ok(Box::pin(async move {
                document_ops::writes::point_put(engine, &col, &doc_id, &val).await
            }))
        }

        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let val = value.clone();
            let if_absent = *if_absent;
            Ok(Box::pin(async move {
                document_ops::writes::point_insert(engine, &col, &doc_id, &val, if_absent).await
            }))
        }

        DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let updates = updates.clone();
            Ok(Box::pin(async move {
                document_ops::writes::point_update(engine, &col, &doc_id, &updates).await
            }))
        }

        DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            Ok(Box::pin(async move {
                document_ops::writes::point_delete(engine, &col, &doc_id).await
            }))
        }

        DocumentOp::BatchInsert {
            collection,
            documents,
            ..
        } => {
            let col = collection.clone();
            let docs = documents.clone();
            Ok(Box::pin(async move {
                document_ops::writes::batch_insert(engine, &col, &docs).await
            }))
        }

        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let val = value.clone();
            let conflict_updates = on_conflict_updates.clone();
            Ok(Box::pin(async move {
                document_ops::writes::upsert(engine, &col, &doc_id, &val, &conflict_updates).await
            }))
        }

        DocumentOp::Truncate { collection, .. } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::writes::truncate(engine, &col).await
            }))
        }

        DocumentOp::BulkUpdate {
            collection,
            updates,
            ..
        } => {
            let col = collection.clone();
            let updates = updates.clone();
            Ok(Box::pin(async move {
                document_ops::writes::bulk_update(engine, &col, &updates).await
            }))
        }

        DocumentOp::BulkDelete { collection, .. } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::writes::bulk_delete(engine, &col).await
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
                document_ops::indexes::register(engine, &col, &mode).await
            }))
        }

        DocumentOp::DropIndex { collection, field } => {
            let col = collection.clone();
            let field = field.clone();
            Ok(Box::pin(async move {
                document_ops::indexes::drop_index(engine, &col, &field).await
            }))
        }

        DocumentOp::BackfillIndex {
            collection, path, ..
        } => {
            let col = collection.clone();
            let path = path.clone();
            Ok(Box::pin(async move {
                document_ops::indexes::backfill_index(engine, &col, &path).await
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
                document_ops::sets::insert_select(engine, &target, &source, limit).await
            }))
        }

        DocumentOp::UpdateFromJoin {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            updates,
            ..
        } => {
            let target = target_collection.clone();
            let source = source_collection.clone();
            let alias = source_alias.clone();
            let target_join = target_join_col.clone();
            let source_join = source_join_col.clone();
            let updates = updates.clone();
            Ok(Box::pin(async move {
                document_ops::sets::update_from_join(
                    engine,
                    &target,
                    &source,
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
            ..
        } => {
            let target = target_collection.clone();
            let source = source_collection.clone();
            let alias = source_alias.clone();
            let target_join = target_join_col.clone();
            let source_join = source_join_col.clone();
            let clauses = clauses.clone();
            Ok(Box::pin(async move {
                document_ops::sets::merge(
                    engine,
                    &target,
                    &source,
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
                document_ops::sets::materialize_scan(engine, &col, &cursor, count).await
            }))
        }
    }
}
