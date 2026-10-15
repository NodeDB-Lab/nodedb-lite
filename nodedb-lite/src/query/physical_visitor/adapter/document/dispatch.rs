// SPDX-License-Identifier: Apache-2.0
//! DocumentOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::RlsWriteCheck;

use crate::error::LiteError;
use crate::query::document_ops;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use crate::query::physical_visitor::adapter::LitePhysicalFut;
use crate::query::physical_visitor::adapter::document_index;
use crate::query::physical_visitor::adapter::policy::deny_policy;

pub(in crate::query::physical_visitor::adapter) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
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
            document_index::indexed_fetch(engine, collection.as_str(), path, value, *limit, *offset)
        }

        DocumentOp::IndexLookup {
            collection,
            path,
            value,
        } => document_index::index_lookup(engine, collection.as_str(), path, value),

        DocumentOp::EstimateCount { collection, .. } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                document_ops::reads::estimate_count(engine, col.as_str()).await
            }))
        }

        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. } => super::writes::dispatch(engine, permit, op),
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
            document_index::drop_index(engine, collection.as_str(), field)
        }

        DocumentOp::BackfillIndex {
            collection,
            path,
            is_array,
            unique,
            case_insensitive,
            predicate,
        } => document_index::backfill_index(
            engine,
            collection.as_str(),
            document_index::BackfillFlags {
                path,
                is_array: *is_array,
                unique: *unique,
                case_insensitive: *case_insensitive,
                predicate: predicate.as_deref(),
            },
        ),

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
