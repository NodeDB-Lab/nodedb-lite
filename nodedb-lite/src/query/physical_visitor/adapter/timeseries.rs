// SPDX-License-Identifier: Apache-2.0
//! TimeseriesOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::timeseries_ops;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;
use super::policy::deny_policy;

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
            sort_keys,
        } => {
            use nodedb_types::SystemTimeScope;
            // Lite's timeseries scan has no ordering stage — `ScanParams`
            // carries no sort keys, so rows come back in the engine's natural
            // order. Empty `sort_keys` means exactly that and is fine; anything
            // else must be REFUSED rather than dropped, or an `ORDER BY` would
            // silently return correctly-filtered rows in the wrong order. This
            // is the seam to implement ordering at if Lite ever grows it.
            if !sort_keys.is_empty() {
                return Err(LiteError::Unsupported {
                    detail: "ORDER BY is not supported on the timeseries engine in Lite".into(),
                });
            }
            // `linear` and `next` interpolate between buckets, which needs a
            // forward pass the single-pass engine does not have. It used to
            // emit NULL for both, so a caller who asked for interpolation got
            // silent gaps instead; refuse at dispatch and keep the strategies
            // that do work (`prev`, `null`/`none`/"" and numeric literals).
            if matches!(gap_fill.as_str(), "linear" | "next") {
                return Err(LiteError::Unsupported {
                    detail: "gap_fill='linear'/'next' is not implemented on the timeseries \
                             engine in Lite; use 'prev', 'null' or a literal value"
                        .into(),
                });
            }
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
                    col.as_str(),
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
            rls_write_check,
            returning,
            rls_filters,
        } => {
            deny_policy(
                "TimeseriesOp::Ingest",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            let col = collection.clone();
            let pay = payload.clone();
            let fmt = format.clone();
            let lsn = *wal_lsn;
            let surr = surrogates.clone();
            Ok(Box::pin(async move {
                // `samples` feeds outbound sync, which is compiled out on wasm32.
                #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
                let (result, samples) =
                    timeseries_ops::writes::ingest(engine, col.as_str(), &pay, &fmt, lsn, &surr)?;
                #[cfg(not(target_arch = "wasm32"))]
                if !samples.is_empty() {
                    let time_key = timeseries_ops::writes::declared_time_key(engine, col.as_str());
                    if let Some(schema) = engine.columnar.schema(col.as_str()) {
                        let rows = timeseries_ops::writes::samples_to_rows(
                            &samples,
                            &schema.columns,
                            time_key.as_deref(),
                        )?;
                        if !rows.is_empty() {
                            crate::sync::reconcile_outbound_enqueue(
                                engine.columnar.enqueue_outbound(col.as_str(), &rows).await,
                                "timeseries insert",
                                col.as_str(),
                                "",
                            )?;
                        }
                    }
                }
                Ok(result)
            }))
        }

        TimeseriesOp::Truncate {
            collection,
            restart_identity,
        } => {
            let col = collection.clone();
            let restart = *restart_identity;
            Ok(Box::pin(async move {
                let result = timeseries_ops::truncate::truncate(engine, col.as_str()).await?;
                crate::query::truncate::restart_identity(engine, col.as_str(), restart);
                Ok(result)
            }))
        }

        // Origin resolves a cross-vshard ingest before proposing it. Lite's
        // single-node engine resolves every write directly, so it never emits
        // this shape and cannot interpret one.
        TimeseriesOp::ResolveIngest(_) => Err(LiteError::Unsupported {
            detail: "TimeseriesOp::ResolveIngest is the resolve-before-propose wire \
                     shape of Origin's cross-vshard write path, which Lite's \
                     single-node engine never emits or needs to interpret"
                .into(),
        }),
    }
}
