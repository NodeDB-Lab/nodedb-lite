// SPDX-License-Identifier: Apache-2.0
pub mod indexes;
pub mod reads;
pub mod sets;
pub mod writes;

use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// A collection is "strict" iff the strict engine has a schema for it.
/// Schemaless collections flow through the CRDT engine instead.
pub(crate) fn is_strict<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> bool {
    engine.strict.schema(collection).is_some()
}
