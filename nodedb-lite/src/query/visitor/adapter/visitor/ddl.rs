// SPDX-License-Identifier: Apache-2.0

//! Array DDL family: create_array/drop_array/alter_array.

use nodedb_sql::CreateArrayVisitArgs;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::array::{lower_alter_array, lower_create_array, lower_drop_array};
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn create_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: CreateArrayVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let CreateArrayVisitArgs {
        name,
        dims,
        attrs,
        tile_extents,
        cell_order,
        tile_order,
        prefix_bits,
        audit_retain_ms,
        minimum_audit_retain_ms,
    } = args;
    lower_create_array(
        engine,
        name,
        dims,
        attrs,
        tile_extents,
        cell_order,
        tile_order,
        prefix_bits,
        audit_retain_ms,
        minimum_audit_retain_ms,
    )
}

pub(super) fn drop_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    if_exists: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_drop_array(engine, name, if_exists)
}

pub(super) fn alter_array<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    name: &str,
    audit_retain_ms: Option<Option<i64>>,
    minimum_audit_retain_ms: Option<u64>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_alter_array(engine, name, audit_retain_ms, minimum_audit_retain_ms)
}
