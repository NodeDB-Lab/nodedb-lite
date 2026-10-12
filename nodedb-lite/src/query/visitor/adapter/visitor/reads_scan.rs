// SPDX-License-Identifier: Apache-2.0

//! Direct scans and lookups: constant_result/scan/point_get/
//! document_index_lookup/range_scan/spatial_scan/timeseries_scan/
//! recursive_scan/recursive_value/lateral_top_k/lateral_loop.

use nodedb_sql::types::SqlValue;
use nodedb_sql::types::query::EngineType;
use nodedb_sql::{
    DocumentIndexLookupVisitArgs, LateralLoopVisitArgs, LateralTopKVisitArgs,
    RecursiveScanVisitArgs, RecursiveValueVisitArgs, ScanVisitArgs, SpatialScanVisitArgs,
    TimeseriesScanVisitArgs,
};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::adapter::basic::{lower_constant_result, lower_point_get, lower_scan};
use crate::query::visitor::lateral::{lower_lateral_loop, lower_lateral_top_k};
use crate::query::visitor::queries::{lower_document_index_lookup, lower_range_scan};
use crate::query::visitor::recursive::{lower_recursive_scan, lower_recursive_value};
use crate::query::visitor::search::lower_spatial_scan;
use crate::query::visitor::timeseries::lower_timeseries_scan;
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn constant_result<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    columns: &[String],
    values: &[SqlValue],
) -> Result<LiteFut<'a>, LiteError> {
    lower_constant_result(engine, columns, values)
}

pub(super) fn scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: ScanVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_scan(engine, &args)
}

pub(super) fn point_get<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    _alias: Option<&str>,
    engine_type: EngineType,
    key_column: &str,
    key_value: &SqlValue,
) -> Result<LiteFut<'a>, LiteError> {
    lower_point_get(engine, collection, engine_type, key_column, key_value)
}

pub(super) fn document_index_lookup<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: DocumentIndexLookupVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_document_index_lookup(engine, args)
}

pub(super) fn range_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    lower: Option<&SqlValue>,
    upper: Option<&SqlValue>,
    limit: usize,
) -> Result<LiteFut<'a>, LiteError> {
    lower_range_scan(engine, collection, field, lower, upper, limit)
}

pub(super) fn spatial_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: SpatialScanVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_spatial_scan(engine, args)
}

pub(super) fn timeseries_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: TimeseriesScanVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_timeseries_scan(engine, args)
}

pub(super) fn recursive_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: RecursiveScanVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_recursive_scan(engine, args)
}

pub(super) fn recursive_value<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: RecursiveValueVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_recursive_value(engine, args)
}

pub(super) fn lateral_top_k<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: LateralTopKVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_lateral_top_k(engine, permit, args)
}

pub(super) fn lateral_loop<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: LateralLoopVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_lateral_loop(engine, permit, args)
}
