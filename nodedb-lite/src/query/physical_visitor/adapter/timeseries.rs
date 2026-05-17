// SPDX-License-Identifier: Apache-2.0
//! TimeseriesOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::timeseries_ops;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + StorageEngineSync + 'a>(
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
            system_as_of_ms,
            valid_at_ms,
        } => {
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
            let sys_as_of = *system_as_of_ms;
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
                        system_as_of_ms: sys_as_of,
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
        } => {
            let col = collection.clone();
            let pay = payload.clone();
            let fmt = format.clone();
            let lsn = *wal_lsn;
            let surr = surrogates.clone();
            Ok(Box::pin(async move {
                timeseries_ops::writes::ingest(engine, &col, &pay, &fmt, lsn, &surr)
            }))
        }
    }
}
