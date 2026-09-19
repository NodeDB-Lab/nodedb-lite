// SPDX-License-Identifier: Apache-2.0

//! Array data ops: insert_array/delete_array/array_slice/array_project/
//! array_agg/array_elementwise/array_flush/array_compact.

use nodedb_sql::temporal::TemporalScope;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::array::{
    lower_array_agg, lower_array_compact, lower_array_elementwise, lower_array_flush,
    lower_array_project, lower_array_slice, lower_delete_array, lower_insert_array,
};
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn insert_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    rows: &[nodedb_sql::types_array::ArrayInsertRow],
) -> Result<LiteFut<'a>, LiteError> {
    lower_insert_array(engine, name, rows)
}

pub(super) fn delete_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    coords: &[Vec<nodedb_sql::types_array::ArrayCoordLiteral>],
) -> Result<LiteFut<'a>, LiteError> {
    lower_delete_array(engine, name, coords)
}

pub(super) fn array_slice<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    slice: &nodedb_sql::types_array::ArraySliceAst,
    attr_projection: &[String],
    limit: u32,
    temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    lower_array_slice(engine, name, slice, attr_projection, limit, temporal)
}

pub(super) fn array_project<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    attr_projection: &[String],
) -> Result<LiteFut<'a>, LiteError> {
    lower_array_project(engine, name, attr_projection)
}

pub(super) fn array_agg<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    attr: &str,
    reducer: &nodedb_sql::types_array::ArrayReducerAst,
    group_by_dim: Option<&str>,
    temporal: &TemporalScope,
) -> Result<LiteFut<'a>, LiteError> {
    lower_array_agg(engine, name, attr, reducer, group_by_dim, temporal)
}

pub(super) fn array_elementwise<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &str,
    right: &str,
    op: nodedb_sql::types_array::ArrayBinaryOpAst,
    attr: &str,
) -> Result<LiteFut<'a>, LiteError> {
    lower_array_elementwise(engine, left, right, op, attr)
}

pub(super) fn array_flush<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
) -> Result<LiteFut<'a>, LiteError> {
    lower_array_flush(engine, name)
}

pub(super) fn array_compact<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
) -> Result<LiteFut<'a>, LiteError> {
    lower_array_compact(engine, name)
}
