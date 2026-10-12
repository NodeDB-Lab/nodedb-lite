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
use crate::query::visitor::kv_dml::{lower_kv_delete, lower_kv_update};
use crate::query::visitor::timeseries::lower_timeseries_ingest;
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    args: InsertVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_insert(engine, permit, args)
}

pub(super) fn upsert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    args: UpsertVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_insert(
        engine,
        permit,
        InsertVisitArgs {
            collection: args.collection,
            engine: args.engine,
            route: args.route,
            rows: args.rows,
            if_absent: true,
            column_schema: args.column_schema,
            primary_key: args.primary_key,
        },
    )
}

pub(super) struct UpdateRequest<'a> {
    pub collection: &'a str,
    pub engine_type: EngineType,
    pub assignments: &'a [(String, SqlExpr)],
    pub filters: &'a [Filter],
    pub target_keys: &'a [SqlValue],
}

pub(super) fn update<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    request: UpdateRequest<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let UpdateRequest {
        collection,
        engine_type,
        assignments,
        filters,
        target_keys,
    } = request;
    match engine_type {
        EngineType::KeyValue => {
            lower_kv_update(engine, collection, assignments, filters, target_keys)
        }
        EngineType::DocumentSchemaless
        | EngineType::DocumentStrict
        | EngineType::Columnar
        | EngineType::Timeseries
        | EngineType::Spatial
        | EngineType::Array => lower_update(
            engine,
            permit,
            collection,
            engine_type,
            assignments,
            filters,
            target_keys,
        ),
    }
}

pub(super) fn delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    engine_type: EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
) -> Result<LiteFut<'a>, LiteError> {
    match engine_type {
        EngineType::KeyValue => lower_kv_delete(engine, collection, filters, target_keys),
        EngineType::DocumentSchemaless
        | EngineType::DocumentStrict
        | EngineType::Columnar
        | EngineType::Timeseries
        | EngineType::Spatial
        | EngineType::Array => lower_delete(
            engine,
            permit,
            collection,
            engine_type,
            filters,
            target_keys,
        ),
    }
}

pub(super) fn insert_select<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    target: &str,
    source: &nodedb_sql::types::SqlPlan,
    limit: usize,
    column_map: &[(String, SqlExpr)],
) -> Result<LiteFut<'a>, LiteError> {
    lower_insert_select(engine, permit, target, source, limit, column_map)
}

pub(super) fn update_from<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    args: UpdateFromVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_update_from(engine, permit, args)
}

pub(super) fn merge<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    args: MergeVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_merge(engine, permit, args)
}

pub(super) fn timeseries_ingest<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: &'a crate::engine::fts::coordinator::TextMutationPermit,
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
) -> Result<LiteFut<'a>, LiteError> {
    lower_timeseries_ingest(engine, permit, collection, rows)
}
