// SPDX-License-Identifier: Apache-2.0
//! ColumnarOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::ColumnarOp;

use crate::error::LiteError;
use crate::query::columnar_ops;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + StorageEngineSync + 'a>(
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
            system_as_of_ms,
            valid_at_ms,
            prefilter,
            computed_columns,
            ..
        } => {
            let col = collection.clone();
            let proj = projection.clone();
            let lim = *limit;
            let filt = filters.clone();
            let sort = sort_keys.clone();
            let sys_as_of = *system_as_of_ms;
            let valid_at = *valid_at_ms;
            let pf = prefilter.clone();
            let cc = computed_columns.clone();
            Ok(Box::pin(async move {
                columnar_ops::reads::scan(
                    engine,
                    &col,
                    columnar_ops::reads::ScanParams {
                        projection: proj,
                        limit: lim,
                        filters_bytes: filt,
                        sort_keys: sort,
                        system_as_of_ms: sys_as_of,
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
        } => {
            let col = collection.clone();
            let pay = payload.clone();
            let fmt = format.clone();
            let int = *intent;
            let ocu = on_conflict_updates.clone();
            let surr = surrogates.clone();
            let sb = schema_bytes.clone();
            Ok(Box::pin(async move {
                columnar_ops::writes::insert(
                    engine,
                    &col,
                    columnar_ops::writes::InsertParams {
                        payload: &pay,
                        format: &fmt,
                        intent: int,
                        on_conflict_updates: &ocu,
                        surrogates: &surr,
                        schema_bytes: &sb,
                    },
                )
            }))
        }

        ColumnarOp::Update {
            collection,
            filters,
            updates,
        } => {
            let col = collection.clone();
            let filt = filters.clone();
            let upd = updates.clone();
            Ok(Box::pin(async move {
                columnar_ops::writes::update(engine, &col, &filt, &upd)
            }))
        }

        ColumnarOp::Delete {
            collection,
            filters,
        } => {
            let col = collection.clone();
            let filt = filters.clone();
            Ok(Box::pin(async move {
                columnar_ops::writes::delete(engine, &col, &filt)
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
                columnar_ops::reads::materialize_scan(engine, &col, &cur, cnt, sys_as_of).await
            }))
        }
    }
}
