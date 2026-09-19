// SPDX-License-Identifier: Apache-2.0

//! Collection admin: truncate/create_index/drop_index.

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::adapter::basic::{lower_create_index, lower_drop_index, lower_truncate};
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn truncate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    _restart_identity: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_truncate(engine, collection)
}

pub(super) fn create_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    _index_name: Option<&str>,
    collection: &str,
    field: &str,
    unique: bool,
    _if_not_exists: bool,
    case_insensitive: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_create_index(engine, collection, field, unique, case_insensitive)
}

pub(super) fn drop_index<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    index_name: &str,
    collection: Option<&str>,
    _if_exists: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_drop_index(engine, index_name, collection)
}
