// SPDX-License-Identifier: Apache-2.0
//! ColumnarOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::ColumnarOp;

use crate::error::LiteError;
use crate::query::columnar_ops;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;
use super::policy::deny_policy;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &ColumnarOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        ColumnarOp::Scan {
            collection,
            projection,
            limit,
            filters,
            sort_keys,
            system_time,
            valid_at_ms,
            prefilter,
            computed_columns,
            ..
        } => {
            use nodedb_types::SystemTimeScope;
            // Columnar does not implement all-versions audit in Lite.
            if system_time.is_all_versions() {
                return Err(LiteError::Unsupported {
                    detail: "AS OF SYSTEM TIME NULL (all-versions) is not supported on \
                             the columnar engine in Lite"
                        .into(),
                });
            }
            let col = collection.clone();
            let proj = projection.clone();
            let lim = *limit;
            let filt = filters.clone();
            let sort = sort_keys.clone();
            // Only an explicit `AS OF SYSTEM TIME <ts>` narrows the read; every
            // other scope (`Current`, and the all-versions case already rejected
            // above) means "no system-time filter" → read the latest version.
            let system_as_of_ms: Option<i64> = match system_time {
                SystemTimeScope::AsOf(ms) => Some(*ms),
                _ => None,
            };
            let valid_at = *valid_at_ms;
            let pf = prefilter.clone();
            let cc = computed_columns.clone();
            Ok(Box::pin(async move {
                columnar_ops::reads::scan(
                    engine,
                    col.as_str(),
                    columnar_ops::reads::ScanParams {
                        projection: proj,
                        limit: lim,
                        filters_bytes: filt,
                        sort_keys: sort,
                        system_as_of_ms,
                        valid_at_ms: valid_at,
                        prefilter: pf,
                        computed_columns: cc,
                    },
                )
                .await
            }))
        }

        ColumnarOp::Insert {
            collection,
            payload,
            format,
            intent,
            on_conflict_updates,
            surrogates,
            schema_bytes,
            wal_lsn: _,
            provenance: _,
            rls_write_check,
            returning,
            rls_filters,
        } => {
            deny_policy(
                "ColumnarOp::Insert",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            let pay = payload.clone();
            let fmt = format.clone();
            let int = *intent;
            let ocu = on_conflict_updates.clone();
            let surr = surrogates.clone();
            let sb = schema_bytes.clone();
            Ok(Box::pin(async move {
                // `inserted_rows` feeds outbound sync, which is compiled out on wasm32.
                #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
                let (result, inserted_rows) = columnar_ops::writes::insert(
                    engine,
                    col.as_str(),
                    columnar_ops::writes::InsertParams {
                        payload: &pay,
                        format: &fmt,
                        intent: int,
                        on_conflict_updates: &ocu,
                        surrogates: &surr,
                        schema_bytes: &sb,
                    },
                )?;
                #[cfg(not(target_arch = "wasm32"))]
                if !inserted_rows.is_empty() {
                    crate::sync::reconcile_outbound_enqueue(
                        engine
                            .columnar
                            .enqueue_outbound(col.as_str(), &inserted_rows)
                            .await,
                        "columnar insert",
                        col.as_str(),
                        "",
                    )?;
                }
                Ok(result)
            }))
        }

        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check,
        } => {
            deny_policy("ColumnarOp::Update", None, &[], rls_write_check)?;
            let col = collection.clone();
            let filt = filters.clone();
            let upd = updates.clone();
            Ok(Box::pin(async move {
                columnar_ops::writes::update(engine, col.as_str(), &filt, &upd)
            }))
        }

        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check,
        } => {
            deny_policy("ColumnarOp::Delete", None, &[], rls_write_check)?;
            let col = collection.clone();
            let filt = filters.clone();
            Ok(Box::pin(async move {
                columnar_ops::writes::delete(engine, col.as_str(), &filt)
            }))
        }

        ColumnarOp::MaterializeScan {
            collection,
            cursor,
            count,
            system_as_of_ms,
        } => {
            let col = collection.clone();
            let cur = cursor.clone();
            let cnt = *count;
            let sys_as_of = *system_as_of_ms;
            Ok(Box::pin(async move {
                columnar_ops::reads::materialize_scan(engine, col.as_str(), &cur, cnt, sys_as_of)
                    .await
            }))
        }

        // ResolvedUpdate/ResolvedDelete/ResolveDml are the resolve-before-propose
        // wire shape Origin uses so a follower applies exactly the rows a Raft
        // leader already decided against a live write identity. Lite is
        // single-node with no Raft replay, so its SQL visitor and CRDT sync
        // resolve predicate UPDATE/DELETE directly and never emit these.
        ColumnarOp::ResolvedUpdate { collection, .. } => Err(LiteError::Unsupported {
            detail: format!(
                "ColumnarOp::ResolvedUpdate on {collection}: replays a decision \
                 made by Origin's Raft leader, which has no equivalent on the \
                 single-node Lite engine"
            ),
        }),

        ColumnarOp::ResolvedDelete { collection, .. } => Err(LiteError::Unsupported {
            detail: format!(
                "ColumnarOp::ResolvedDelete on {collection}: replays a decision \
                 made by Origin's Raft leader, which has no equivalent on the \
                 single-node Lite engine"
            ),
        }),

        ColumnarOp::ResolveDml { collection, .. } => Err(LiteError::Unsupported {
            detail: format!(
                "ColumnarOp::ResolveDml on {collection}: is the resolve pass of \
                 Origin's cross-vshard write path, which Lite's single-node \
                 engine never emits or needs to interpret"
            ),
        }),
    }
}
