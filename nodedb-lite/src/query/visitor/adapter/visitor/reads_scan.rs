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
    _key_column: &str,
    key_value: &SqlValue,
) -> Result<LiteFut<'a>, LiteError> {
    lower_point_get(engine, collection, engine_type, key_value)
}

pub(super) fn document_index_lookup<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: DocumentIndexLookupVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let DocumentIndexLookupVisitArgs {
        collection,
        alias,
        engine: engine_type,
        field,
        value,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        case_insensitive,
        temporal,
    } = args;
    lower_document_index_lookup(
        engine,
        collection,
        alias,
        engine_type,
        field,
        value,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        case_insensitive,
        temporal,
    )
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
    let SpatialScanVisitArgs {
        collection,
        field,
        predicate,
        query_geometry,
        distance_meters,
        attribute_filters,
        limit,
        projection,
    } = args;
    lower_spatial_scan(
        engine,
        collection,
        field,
        predicate,
        query_geometry,
        distance_meters,
        attribute_filters,
        limit,
        projection,
    )
}

pub(super) fn timeseries_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: TimeseriesScanVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let TimeseriesScanVisitArgs {
        collection,
        time_range,
        bucket_interval_ms,
        group_by,
        aggregates,
        filters,
        projection,
        gap_fill,
        limit,
        tiered,
        temporal,
        sort_keys,
    } = args;
    lower_timeseries_scan(
        engine,
        collection,
        time_range,
        bucket_interval_ms,
        group_by,
        aggregates,
        filters,
        projection,
        gap_fill,
        limit,
        tiered,
        temporal,
        sort_keys,
    )
}

pub(super) fn recursive_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: RecursiveScanVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let RecursiveScanVisitArgs {
        collection,
        base_filters,
        recursive_filters,
        join_link,
        max_iterations,
        distinct,
        limit,
    } = args;
    lower_recursive_scan(
        engine,
        collection,
        base_filters,
        recursive_filters,
        join_link,
        max_iterations,
        distinct,
        limit,
    )
}

pub(super) fn recursive_value<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: RecursiveValueVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let RecursiveValueVisitArgs {
        cte_name,
        columns,
        init_exprs,
        step_exprs,
        condition,
        max_depth,
        distinct,
    } = args;
    lower_recursive_value(
        engine, cte_name, columns, init_exprs, step_exprs, condition, max_depth, distinct,
    )
}

pub(super) fn lateral_top_k<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: LateralTopKVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let LateralTopKVisitArgs {
        outer,
        outer_alias,
        inner_collection,
        inner_filters,
        inner_order_by,
        inner_limit,
        correlation_keys,
        lateral_alias,
        projection,
        left_join,
    } = args;
    lower_lateral_top_k(
        engine,
        outer,
        outer_alias,
        inner_collection,
        inner_filters,
        inner_order_by,
        inner_limit,
        correlation_keys,
        lateral_alias,
        projection,
        left_join,
    )
}

pub(super) fn lateral_loop<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: LateralLoopVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let LateralLoopVisitArgs {
        outer,
        outer_alias,
        inner,
        correlation_predicates,
        lateral_alias,
        projection,
        outer_row_cap,
        left_join,
    } = args;
    lower_lateral_loop(
        engine,
        outer,
        outer_alias,
        inner,
        correlation_predicates,
        lateral_alias,
        projection,
        outer_row_cap,
        left_join,
    )
}
