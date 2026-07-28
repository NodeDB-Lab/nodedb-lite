// SPDX-License-Identifier: Apache-2.0

//! Config-path implementation for wired `TextOp` variants.
//!
//! Each function corresponds to one variant routed here from `text_op.rs`.
//! The per-collection binding itself lives in `engine::fts::analyzer`.

use std::sync::Arc;

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::adapter::LitePhysicalFut;

/// Bind a collection's FTS analyzer and/or default fuzzy matching.
///
/// Config-only and non-WAL-durable, the same shape `VectorOp::SetParams` uses.
/// A `None` field means "leave the collection's current setting in place", so
/// both being `None` is a valid no-op that still succeeds.
pub(super) fn text_set_config<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    analyzer_name: Option<String>,
    fuzzy_default: Option<bool>,
) -> Result<LitePhysicalFut<'a>, LiteError>
where
    S: StorageEngine + 'a,
{
    let fts_state = Arc::clone(&engine.fts_state);
    Ok(Box::pin(async move {
        let mut mgr = fts_state
            .manager
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;
        if let Some(name) = analyzer_name.as_deref() {
            mgr.set_collection_analyzer(&collection, name);
        }
        if let Some(fuzzy) = fuzzy_default {
            mgr.set_collection_fuzzy(&collection, fuzzy);
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }))
}
