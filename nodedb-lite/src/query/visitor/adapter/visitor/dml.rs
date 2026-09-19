// SPDX-License-Identifier: Apache-2.0

//! Insert/upsert/update/delete/insert_select/merge/update_from/
//! timeseries_ingest: destructures each `*VisitArgs` struct and forwards to
//! the matching `lower_*` function.

use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::EngineType;
use nodedb_sql::types_expr::SqlExpr;
use nodedb_sql::{InsertVisitArgs, MergeVisitArgs, UpdateFromVisitArgs, UpsertVisitArgs};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::adapter::basic::{lower_delete, lower_insert, lower_update};
use crate::query::visitor::dml::{lower_insert_select, lower_merge, lower_update_from};
use crate::query::visitor::timeseries::lower_timeseries_ingest;
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: InsertVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let InsertVisitArgs {
        collection,
        engine: engine_type,
        route,
        rows,
        if_absent,
        column_schema: _column_schema,
        primary_key,
    } = args;
    lower_insert(
        engine,
        collection,
        engine_type,
        route,
        rows,
        if_absent,
        primary_key,
    )
}

pub(super) fn upsert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: UpsertVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let UpsertVisitArgs {
        collection,
        engine: engine_type,
        route,
        rows,
        on_conflict_updates: _on_conflict_updates,
        column_schema: _column_schema,
        primary_key,
    } = args;
    lower_insert(
        engine,
        collection,
        engine_type,
        route,
        rows,
        true,
        primary_key,
    )
}

pub(super) fn update<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    assignments: &[(String, SqlExpr)],
    _filters: &[Filter],
    target_keys: &[SqlValue],
    _returning: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_update(engine, collection, engine_type, assignments, target_keys)
}

pub(super) fn delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    _filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<LiteFut<'a>, LiteError> {
    lower_delete(engine, collection, engine_type, target_keys)
}

pub(super) fn insert_select<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    target: &str,
    source: &nodedb_sql::types::SqlPlan,
    limit: usize,
    column_map: &[(String, SqlExpr)],
) -> Result<LiteFut<'a>, LiteError> {
    lower_insert_select(engine, target, source, limit, column_map)
}

pub(super) fn update_from<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: UpdateFromVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let UpdateFromVisitArgs {
        collection,
        engine: engine_type,
        source,
        target_join_col,
        source_join_col,
        assignments,
        target_filters,
        returning,
    } = args;
    lower_update_from(
        engine,
        collection,
        engine_type,
        source,
        target_join_col,
        source_join_col,
        assignments,
        target_filters,
        returning,
    )
}

pub(super) fn merge<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: MergeVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let MergeVisitArgs {
        target,
        engine: engine_type,
        source,
        target_join_col,
        source_join_col,
        source_alias,
        clauses,
        returning,
    } = args;
    lower_merge(
        engine,
        target,
        engine_type,
        source,
        target_join_col,
        source_join_col,
        source_alias,
        clauses,
        returning,
    )
}

pub(super) fn timeseries_ingest<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
) -> Result<LiteFut<'a>, LiteError> {
    lower_timeseries_ingest(engine, collection, rows)
}
