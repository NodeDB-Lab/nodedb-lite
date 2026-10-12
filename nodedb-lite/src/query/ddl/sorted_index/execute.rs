// SPDX-License-Identifier: Apache-2.0

//! Sorted-index statement execution over the KV sorted-index operations.
//!
//! Result shapes:
//!
//! - `CREATE SORTED INDEX`: `[index, backfilled]`.
//! - `DROP SORTED INDEX`: `[dropped]`.
//! - `RANK`: `[rank]`, NULL for a row the index does not hold.
//! - `TOPK`, `RANGE`: `[rank, key]` rows, the key as text.
//! - `SORTED_COUNT`: `[sorted_count]`.
//!
//! A read naming no sorted index is an undefined object.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::nodedb::collection::ddl::CollectionMeta;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops::sorted::{
    RegisterSortedIndex, f64_to_sort_bytes, kv_drop_sorted_index, kv_register_sorted_index,
    kv_sorted_index_count, kv_sorted_index_range, kv_sorted_index_rank, kv_sorted_index_top_k,
};
use crate::storage::engine::StorageEngine;

use super::statement::{CreateSortedIndex, SortedIndexStatement};

impl<S: StorageEngine> LiteQueryEngine<S> {
    pub(in crate::query) async fn handle_sorted_index(
        &self,
        statement: SortedIndexStatement,
    ) -> Result<QueryResult, LiteError> {
        match statement {
            SortedIndexStatement::Create(create) => self.create_sorted_index(create).await,
            SortedIndexStatement::Drop { name } => kv_drop_sorted_index(self, &name).await,
            SortedIndexStatement::Rank { index, key } => {
                // The rank operation answers NULL for an unknown index. The
                // SQL function refuses it, as it refuses every other read.
                if self.indexes.sorted_named(&index).is_none() {
                    return Err(LiteError::UndefinedObject {
                        object: format!("sorted index '{index}'"),
                    });
                }
                kv_sorted_index_rank(self, &index, &key).await
            }
            SortedIndexStatement::TopK { index, k } => {
                Ok(keys_as_text(kv_sorted_index_top_k(self, &index, k).await?))
            }
            SortedIndexStatement::Range { index, min, max } => {
                let min = min.map(f64_to_sort_bytes);
                let max = max.map(f64_to_sort_bytes);
                let result = kv_sorted_index_range(
                    self,
                    &index,
                    min.as_ref().map(<[u8; 8]>::as_slice),
                    max.as_ref().map(<[u8; 8]>::as_slice),
                )
                .await?;
                Ok(keys_as_text(result))
            }
            SortedIndexStatement::Count { index } => {
                let mut result = kv_sorted_index_count(self, &index).await?;
                result.columns = vec!["sorted_count".into()];
                Ok(result)
            }
        }
    }

    /// Register the index after checking its collection is a KV collection:
    /// the index ranks KV rows, and no other engine stores any.
    async fn create_sorted_index(
        &self,
        create: CreateSortedIndex,
    ) -> Result<QueryResult, LiteError> {
        if !self.is_kv_collection(&create.collection).await? {
            return Err(LiteError::CollectionNotFound {
                engine: "kv",
                collection: create.collection,
            });
        }
        kv_register_sorted_index(
            self,
            RegisterSortedIndex {
                collection: &create.collection,
                index_name: &create.name,
                sort_columns: &create.sort_columns,
                key_column: &create.key_column,
                window_type: create.window.kind,
                window_timestamp_column: &create.window.timestamp_column,
                window_start_ms: create.window.start_ms,
                window_end_ms: create.window.end_ms,
            },
        )
        .await
    }

    /// Whether `name` has persisted collection metadata of type `kv`.
    async fn is_kv_collection(&self, name: &str) -> Result<bool, LiteError> {
        let key = format!("collection:{name}");
        let Some(bytes) = self.storage.get(Namespace::Meta, key.as_bytes()).await? else {
            return Ok(false);
        };
        let meta: CollectionMeta =
            sonic_rs::from_slice(&bytes).map_err(|e| LiteError::Corrupted {
                detail: format!("collection meta of '{name}' does not decode: {e}"),
            })?;
        Ok(meta.collection_type == "kv")
    }
}

/// Render the `key` column of `[rank, key]` rows as text, as Origin does:
/// a key that is not UTF-8 renders with replacement characters.
fn keys_as_text(mut result: QueryResult) -> QueryResult {
    for row in &mut result.rows {
        if let Some(Value::Bytes(key)) = row.get(1) {
            let text = String::from_utf8_lossy(key).into_owned();
            row[1] = Value::String(text);
        }
    }
    result
}
