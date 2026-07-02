// SPDX-License-Identifier: Apache-2.0
//! TimeseriesOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::timeseries_ops;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &TimeseriesOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        TimeseriesOp::Scan {
            collection,
            time_range,
            projection,
            limit,
            filters,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            rls_filters,
            system_time,
            valid_at_ms,
        } => {
            use nodedb_types::SystemTimeScope;
            // Timeseries does not implement all-versions audit in Lite.
            if system_time.is_all_versions() {
                return Err(LiteError::Unsupported {
                    detail: "AS OF SYSTEM TIME NULL (all-versions) is not supported on \
                             the timeseries engine in Lite"
                        .into(),
                });
            }
            // Only an explicit `AS OF SYSTEM TIME <ts>` narrows the read; every
            // other scope (`Current`, and the all-versions case already rejected
            // above) means "no system-time filter" → read the latest version.
            let system_as_of_ms: Option<i64> = match system_time {
                SystemTimeScope::AsOf(ms) => Some(*ms),
                _ => None,
            };
            let col = collection.clone();
            let tr = *time_range;
            let proj = projection.clone();
            let lim = *limit;
            let filt = filters.clone();
            let bucket_ms = *bucket_interval_ms;
            let grp = group_by.clone();
            let aggs = aggregates.clone();
            let gf = gap_fill.clone();
            let cc = computed_columns.clone();
            let rls = rls_filters.clone();
            let valid_at = *valid_at_ms;
            Ok(Box::pin(async move {
                timeseries_ops::reads::scan(
                    engine,
                    &col,
                    timeseries_ops::reads::ScanParams {
                        time_range: tr,
                        projection: proj,
                        limit: lim,
                        filters: filt,
                        bucket_interval_ms: bucket_ms,
                        group_by: grp,
                        aggregates: aggs,
                        gap_fill: gf,
                        computed_columns: cc,
                        rls_filters: rls,
                        system_as_of_ms,
                        valid_at_ms: valid_at,
                    },
                )
            }))
        }

        TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            wal_lsn,
            surrogates,
            provenance: _,
        } => {
            let col = collection.clone();
            let pay = payload.clone();
            let fmt = format.clone();
            let lsn = *wal_lsn;
            let surr = surrogates.clone();
            Ok(Box::pin(async move {
                // `samples` feeds outbound sync, which is compiled out on wasm32.
                #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
                let (result, samples) =
                    timeseries_ops::writes::ingest(engine, &col, &pay, &fmt, lsn, &surr)?;
                #[cfg(not(target_arch = "wasm32"))]
                if !samples.is_empty() {
                    let col_names: Option<Vec<String>> = engine
                        .columnar
                        .schema(&col)
                        .map(|s| s.columns.into_iter().map(|c| c.name).collect());
                    if let Some(col_names) = col_names {
                        let rows = timeseries_ops::writes::samples_to_rows(&samples, &col_names);
                        if !rows.is_empty() {
                            crate::sync::reconcile_outbound_enqueue(
                                engine.columnar.enqueue_outbound(&col, &rows).await,
                                "timeseries insert",
                                &col,
                                "",
                            )?;
                        }
                    }
                }
                Ok(result)
            }))
        }
    }
}
