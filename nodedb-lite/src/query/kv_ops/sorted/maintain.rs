// SPDX-License-Identifier: Apache-2.0
//! Sorted-index entries a KV write batch implies.
//!
//! Every KV write reaches storage through `kv_ops::indexes::kv_commit`,
//! which adds the ops planned here to the same storage batch as the rows.
//! A row and its sorted entries therefore commit together or not at all.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::index::sorted_def::SortedIndexDef;
use crate::query::kv_ops::reads::split_kv_key;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::def::entry_for;
use super::keys::{PkEntry, pk_entry_key};

/// The entry the row `pk` holds in the index `index_name`, as stored.
async fn stored_entry<S: StorageEngine>(
    storage: &S,
    index_name: &str,
    pk: &[u8],
) -> Result<Option<PkEntry>, LiteError> {
    match storage
        .get(Namespace::Meta, &pk_entry_key(index_name, pk))
        .await?
    {
        None => Ok(None),
        Some(bytes) => PkEntry::decode(&bytes)
            .map(Some)
            .ok_or_else(|| LiteError::Corrupted {
                detail: format!("sorted index '{index_name}': pk entry does not decode"),
            }),
    }
}

/// Append to `out` the writes that move the row `pk` of `index_name` from
/// the entry `old` to the entry `new`.
fn plan_change(
    index_name: &str,
    pk: &[u8],
    old: Option<PkEntry>,
    new: Option<PkEntry>,
    out: &mut Vec<WriteOp>,
) {
    match (old, new) {
        (None, None) => {}
        (Some(old), None) => {
            out.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key: old.score_key(index_name, pk),
            });
            out.push(WriteOp::Delete {
                ns: Namespace::Meta,
                key: pk_entry_key(index_name, pk),
            });
        }
        (old, Some(new)) => {
            if old == Some(new) {
                return;
            }
            if let Some(old) = old
                && !old.same_position(&new)
            {
                out.push(WriteOp::Delete {
                    ns: Namespace::Meta,
                    key: old.score_key(index_name, pk),
                });
            }
            out.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: new.score_key(index_name, pk),
                value: new.score_value(),
            });
            out.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: pk_entry_key(index_name, pk),
                value: new.encode(),
            });
        }
    }
}

/// The sorted-index writes the KV writes `ops` imply, for the indexes in
/// `defs_by_collection`.
///
/// Ops apply in order. A row written twice in one batch plans its second
/// write from the entry its first write left, never from storage.
pub(crate) async fn plan_sorted_ops<S: StorageEngine>(
    storage: &S,
    defs_by_collection: &HashMap<String, Vec<Arc<SortedIndexDef>>>,
    ops: &[WriteOp],
) -> Result<Vec<WriteOp>, LiteError> {
    let mut out = Vec::new();
    if defs_by_collection.is_empty() {
        return Ok(out);
    }
    let now = crate::runtime::now_millis();
    let mut overlay: HashMap<(String, Vec<u8>), Option<PkEntry>> = HashMap::new();
    for op in ops {
        let (key, stored) = match op {
            WriteOp::Put { ns, key, value } if *ns == Namespace::Kv => (key, Some(value)),
            WriteOp::Delete { ns, key } if *ns == Namespace::Kv => (key, None),
            _ => continue,
        };
        let Some((collection, pk)) = split_kv_key(key) else {
            continue;
        };
        let Some(defs) = defs_by_collection.get(collection) else {
            continue;
        };
        for def in defs {
            let slot = (def.name.clone(), pk.to_vec());
            let old = match overlay.get(&slot) {
                Some(entry) => *entry,
                None => stored_entry(storage, &def.name, pk).await?,
            };
            let new = match stored {
                Some(value) => entry_for(def, value, now)?,
                None => None,
            };
            plan_change(&def.name, pk, old, new, &mut out);
            overlay.insert(slot, new);
        }
    }
    Ok(out)
}
