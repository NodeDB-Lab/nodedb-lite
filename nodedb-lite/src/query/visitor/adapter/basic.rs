// SPDX-License-Identifier: Apache-2.0

//! Lowerings for direct-to-engine CRUD ops: `Scan`, `PointGet`, `Insert`,
//! `Upsert`, `Update`, `Delete`, `Truncate`, `ConstantResult`, `CreateIndex`,
//! `DropIndex`. These dispatch straight to `LiteQueryEngine` methods or the
//! `LiteDataPlaneVisitor` without intermediate planning helpers.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::{EngineType, SortKey, WindowSpec};
use nodedb_sql::types_expr::SqlExpr;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::visitor::scan_post::apply_scan_post_processing;
use crate::storage::engine::StorageEngine;

use super::visitor::LiteFut;

pub(super) fn lower_constant_result<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    columns: &[String],
    values: &[SqlValue],
) -> Result<LiteFut<'a>, LiteError> {
    let columns = columns.to_vec();
    let values = values.to_vec();
    Ok(Box::pin(async move {
        engine.execute_constant_result(&columns, &values).await
    }))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn lower_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    filters: &[Filter],
    sort_keys: &[SortKey],
    limit: Option<usize>,
    offset: usize,
    distinct: bool,
    window_functions: &[WindowSpec],
    _temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    let collection = collection.to_string();
    let filters = filters.to_vec();
    let sort_keys = sort_keys.to_vec();
    let window_functions = window_functions.to_vec();
    Ok(Box::pin(async move {
        let raw = engine.execute_scan(&collection, &engine_type).await?;
        apply_scan_post_processing(
            raw,
            &filters,
            &sort_keys,
            &window_functions,
            limit,
            offset,
            distinct,
        )
    }))
}

pub(super) fn lower_point_get<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    key_value: &SqlValue,
) -> Result<LiteFut<'a>, LiteError> {
    let collection = collection.to_string();
    let key_value = key_value.clone();
    Ok(Box::pin(async move {
        engine
            .execute_point_get(&collection, &engine_type, &key_value)
            .await
    }))
}

pub(super) fn lower_insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    rows: &[Vec<(String, SqlValue)>],
    if_absent: bool,
    primary_key: Option<&str>,
) -> Result<LiteFut<'a>, LiteError> {
    let collection = collection.to_string();
    let rows = rows.to_vec();
    let primary_key = primary_key.map(str::to_string);
    Ok(Box::pin(async move {
        engine
            .execute_insert(
                &collection,
                &engine_type,
                &rows,
                if_absent,
                primary_key.as_deref(),
            )
            .await
    }))
}

pub(super) fn lower_update<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    assignments: &[(String, SqlExpr)],
    target_keys: &[SqlValue],
) -> Result<LiteFut<'a>, LiteError> {
    let collection = collection.to_string();
    let assignments = assignments.to_vec();
    let target_keys = target_keys.to_vec();
    Ok(Box::pin(async move {
        engine
            .execute_update(&collection, &engine_type, &assignments, &target_keys)
            .await
    }))
}

pub(super) fn lower_delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
    target_keys: &[SqlValue],
) -> Result<LiteFut<'a>, LiteError> {
    let collection = collection.to_string();
    let target_keys = target_keys.to_vec();
    Ok(Box::pin(async move {
        engine
            .execute_delete(&collection, &engine_type, &target_keys)
            .await
    }))
}

pub(super) fn lower_truncate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
) -> Result<LiteFut<'a>, LiteError> {
    let collection = collection.to_string();
    Ok(Box::pin(async move {
        engine.execute_truncate(&collection).await
    }))
}

pub(super) fn lower_create_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    unique: bool,
    case_insensitive: bool,
) -> Result<LiteFut<'a>, LiteError> {
    use nodedb_physical::physical_plan::document::DocumentOp;
    let op = DocumentOp::BackfillIndex {
        collection: collection.to_string(),
        path: field.to_string(),
        is_array: false,
        unique,
        case_insensitive,
        predicate: None,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    phys.document(&op)
}

/// Lite persists index entries under `collection:field:value:id`. Without a
/// catalog lookup the field name is not known from the index name alone, so
/// the caller must supply the collection via the ON clause. The drop is
/// best-effort at field-level granularity using the index-name as the field.
pub(super) fn lower_drop_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    collection: Option<&str>,
) -> Result<LiteFut<'a>, LiteError> {
    use nodedb_physical::physical_plan::document::DocumentOp;
    let op = DocumentOp::DropIndex {
        collection: collection.unwrap_or("").to_string(),
        field: index_name.to_string(),
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    phys.document(&op)
}
