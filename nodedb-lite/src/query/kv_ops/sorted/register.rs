// SPDX-License-Identifier: Apache-2.0
//! Sorted-index DDL: register and drop.

use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::index::sorted_def::SortedIndexDef;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops::reads::{kv_key, split_kv_key};
use crate::storage::engine::{StorageEngine, WriteOp};

use super::def::{RegisterSortedIndex, entry_for};
use super::keys::pk_entry_key;

/// The entry writes of every row of `def`'s collection, and their count.
async fn backfill<S: StorageEngine>(
    storage: &S,
    def: &SortedIndexDef,
) -> Result<(Vec<WriteOp>, u64), LiteError> {
    let now = crate::runtime::now_millis();
    let mut ops = Vec::new();
    let mut count: u64 = 0;
    for (composite, stored) in storage
        .scan_prefix(Namespace::Kv, &kv_key(&def.collection, b""))
        .await?
    {
        let Some((collection, pk)) = split_kv_key(&composite) else {
            continue;
        };
        if collection != def.collection {
            continue;
        }
        let Some(entry) = entry_for(def, &stored, now)? else {
            continue;
        };
        ops.push(WriteOp::Put {
            ns: Namespace::Meta,
            key: entry.score_key(&def.name, pk),
            value: entry.score_value(),
        });
        ops.push(WriteOp::Put {
            ns: Namespace::Meta,
            key: pk_entry_key(&def.name, pk),
            value: entry.encode(),
        });
        count += 1;
    }
    Ok((ops, count))
}

/// RegisterSortedIndex: rank the rows of a KV collection by a score column.
///
/// The index is built from the rows the collection holds, including the
/// public KV API's buffered writes, and replaces any sorted index of the
/// same name. Returns `[index, backfilled]`: the name and the number of
/// rows ranked.
pub async fn kv_register_sorted_index<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    req: RegisterSortedIndex<'_>,
) -> Result<QueryResult, LiteError> {
    let def = Arc::new(SortedIndexDef::build(&req)?);
    // The flush takes the DDL lock for reading, so it runs before the
    // install takes it for writing.
    engine.kv_local.flush_to(&*engine.storage).await?;
    let storage = &*engine.storage;
    let backfilled = engine
        .indexes
        .install_sorted(storage, Arc::clone(&def), || backfill(storage, &def))
        .await?;
    Ok(QueryResult {
        columns: vec!["index".into(), "backfilled".into()],
        rows: vec![vec![
            Value::String(def.name.clone()),
            Value::Integer(i64::try_from(backfilled).unwrap_or(i64::MAX)),
        ]],
        rows_affected: 0,
        command: None,
    })
}

/// DropSortedIndex: remove a sorted index and every entry it holds.
/// Returns `[dropped]`: the name. An unknown index is `UndefinedObject`.
pub async fn kv_drop_sorted_index<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    index_name: &str,
) -> Result<QueryResult, LiteError> {
    match engine
        .indexes
        .drop_sorted(&*engine.storage, index_name)
        .await?
    {
        Some(def) => Ok(QueryResult {
            columns: vec!["dropped".into()],
            rows: vec![vec![Value::String(def.name.clone())]],
            rows_affected: 0,
            command: None,
        }),
        None => Err(LiteError::UndefinedObject {
            object: format!("sorted index '{index_name}'"),
        }),
    }
}
