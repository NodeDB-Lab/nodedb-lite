// SPDX-License-Identifier: Apache-2.0
//! Collection info meta-ops: QueryCollectionSize.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// `QueryCollectionSize` — sum the on-disk byte footprint (key + value) of
/// every blob keyed under `{name}*` across all data-bearing namespaces.
///
/// This is exact for the bytes the storage layer hands back, not an estimate;
/// it does not include internal page overhead, but it is deterministic
/// and reflects what would be reclaimed by dropping the collection.
pub async fn handle_query_collection_size<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    _tenant_id: u64,
    name: &str,
) -> Result<QueryResult, LiteError> {
    let mut total_bytes: u64 = 0;
    let prefix = name.as_bytes();
    for &ns in DATA_NAMESPACES {
        let entries = engine.storage.scan_prefix(ns, prefix).await?;
        for (k, v) in entries {
            total_bytes = total_bytes.saturating_add(k.len() as u64);
            total_bytes = total_bytes.saturating_add(v.len() as u64);
        }
    }
    Ok(QueryResult {
        columns: vec!["size_bytes".into()],
        rows: vec![vec![Value::Integer(total_bytes as i64)]],
        rows_affected: 0,
    })
}

/// Namespaces that may carry per-collection data. Keep in sync with
/// `nodedb_types::Namespace` — adding a new data-bearing variant requires
/// adding it here.
const DATA_NAMESPACES: &[Namespace] = &[
    Namespace::Crdt,
    Namespace::LoroState,
    Namespace::Strict,
    Namespace::Columnar,
    Namespace::Kv,
    Namespace::Array,
    Namespace::ArrayOpLog,
    Namespace::ArrayDelta,
    Namespace::Fts,
];
