// SPDX-License-Identifier: Apache-2.0

//! EdgePut, EdgePutBatch, EdgeDelete, EdgeDeleteBatch handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_physical::physical_plan::graph::BatchEdge;
use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::history;
use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::runtime::now_millis_i64;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Upsert edge properties into the Namespace::Graph storage table.
///
/// Key layout: `{collection}\x00{src}\x00{label}\x00{dst}`
fn edge_store_key(collection: &str, src: &str, label: &str, dst: &str) -> Vec<u8> {
    let mut k = collection.as_bytes().to_vec();
    k.push(0);
    k.extend_from_slice(src.as_bytes());
    k.push(0);
    k.extend_from_slice(label.as_bytes());
    k.push(0);
    k.extend_from_slice(dst.as_bytes());
    k
}

fn edge_to_value(
    collection: &str,
    src: &str,
    label: &str,
    dst: &str,
    props: &[u8],
) -> Result<Vec<u8>, LiteError> {
    let mut m = HashMap::new();
    m.insert(
        "collection".to_string(),
        Value::String(collection.to_string()),
    );
    m.insert("src".to_string(), Value::String(src.to_string()));
    m.insert("label".to_string(), Value::String(label.to_string()));
    m.insert("dst".to_string(), Value::String(dst.to_string()));
    if !props.is_empty() {
        // Properties are already msgpack bytes from the caller — store raw.
        m.insert("props".to_string(), Value::Bytes(props.to_vec()));
    }
    zerompk::to_msgpack_vec(&Value::Object(m)).map_err(|e| LiteError::Serialization {
        detail: e.to_string(),
    })
}

/// Handle `GraphOp::EdgePut`.
pub async fn edge_put<S: StorageEngine>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    src_id: &str,
    label: &str,
    dst_id: &str,
    properties: &[u8],
) -> Result<QueryResult, LiteError> {
    // Insert into CSR.
    {
        let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map
            .entry(collection.to_string())
            .or_insert_with(CsrIndex::new);
        csr.add_edge(src_id, label, dst_id)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }

    // Persist edge data.
    let key = edge_store_key(collection, src_id, label, dst_id);
    let value = edge_to_value(collection, src_id, label, dst_id, properties)?;
    storage.put(Namespace::Graph, &key, &value).await?;

    // Record bitemporal insert if enabled.
    if history::is_bitemporal(storage.as_ref(), collection)
        .await
        .unwrap_or(false)
    {
        let edge_key = format!("{src_id}->{dst_id}:{label}");
        let props_val = Value::Bytes(properties.to_vec());
        let _ = history::record_edge_insert(
            storage.as_ref(),
            collection,
            &edge_key,
            &props_val,
            now_millis_i64(),
        )
        .await;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: 1,
    })
}

/// Handle `GraphOp::EdgePutBatch`.
pub async fn edge_put_batch<S: StorageEngine>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    edges: &[BatchEdge],
) -> Result<QueryResult, LiteError> {
    if edges.is_empty() {
        return Ok(QueryResult::empty());
    }

    let ts = now_millis_i64();
    let mut write_ops: Vec<WriteOp> = Vec::with_capacity(edges.len());
    let mut bitemporal_edges: Vec<(String, String)> = Vec::new();

    {
        let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        for e in edges {
            let csr = map.entry(e.collection.clone()).or_default();
            csr.add_edge(&e.src_id, &e.label, &e.dst_id)
                .map_err(|g| LiteError::Storage {
                    detail: g.to_string(),
                })?;
        }
    }

    for e in edges {
        let key = edge_store_key(&e.collection, &e.src_id, &e.label, &e.dst_id);
        let value = edge_to_value(&e.collection, &e.src_id, &e.label, &e.dst_id, &[])?;
        write_ops.push(WriteOp::Put {
            ns: Namespace::Graph,
            key,
            value,
        });
        // Collect bitemporal edges.
        if history::is_bitemporal(storage.as_ref(), &e.collection)
            .await
            .unwrap_or(false)
        {
            bitemporal_edges.push((
                e.collection.clone(),
                format!("{}->{}: {}", e.src_id, e.dst_id, e.label),
            ));
        }
    }

    storage.batch_write(&write_ops).await?;

    for (collection, edge_key) in &bitemporal_edges {
        let _ =
            history::record_edge_insert(storage.as_ref(), collection, edge_key, &Value::Null, ts)
                .await;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: edges.len() as u64,
    })
}

/// Handle `GraphOp::EdgeDelete`.
pub async fn edge_delete<S: StorageEngine>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    src_id: &str,
    label: &str,
    dst_id: &str,
) -> Result<QueryResult, LiteError> {
    {
        let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        if let Some(csr) = map.get_mut(collection) {
            csr.remove_edge(src_id, label, dst_id);
        }
    }

    let key = edge_store_key(collection, src_id, label, dst_id);
    storage.delete(Namespace::Graph, &key).await?;

    if history::is_bitemporal(storage.as_ref(), collection)
        .await
        .unwrap_or(false)
    {
        let edge_key = format!("{src_id}->{dst_id}:{label}");
        let _ =
            history::record_edge_delete(storage.as_ref(), collection, &edge_key, now_millis_i64())
                .await;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: 1,
    })
}

/// Handle `GraphOp::EdgeDeleteBatch`.
pub async fn edge_delete_batch<S: StorageEngine>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    edges: &[BatchEdge],
) -> Result<QueryResult, LiteError> {
    if edges.is_empty() {
        return Ok(QueryResult::empty());
    }

    let ts = now_millis_i64();
    let mut write_ops: Vec<WriteOp> = Vec::with_capacity(edges.len());

    {
        let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        for e in edges {
            if let Some(csr) = map.get_mut(&e.collection) {
                csr.remove_edge(&e.src_id, &e.label, &e.dst_id);
            }
            let key = edge_store_key(&e.collection, &e.src_id, &e.label, &e.dst_id);
            write_ops.push(WriteOp::Delete {
                ns: Namespace::Graph,
                key,
            });
        }
    }

    storage.batch_write(&write_ops).await?;

    for e in edges {
        if history::is_bitemporal(storage.as_ref(), &e.collection)
            .await
            .unwrap_or(false)
        {
            let edge_key = format!("{}->{}: {}", e.src_id, e.dst_id, e.label);
            let _ =
                history::record_edge_delete(storage.as_ref(), &e.collection, &edge_key, ts).await;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: edges.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr_map() -> Arc<Mutex<HashMap<String, CsrIndex>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[test]
    fn edge_store_key_layout() {
        let k = edge_store_key("social", "alice", "KNOWS", "bob");
        // Must contain all four components separated by NUL.
        let s = String::from_utf8_lossy(&k);
        assert!(s.contains("social"));
        assert!(s.contains("alice"));
        assert!(s.contains("KNOWS"));
        assert!(s.contains("bob"));
    }

    #[test]
    fn csr_map_insert_and_lookup() {
        let map = make_csr_map();
        let mut locked = map.lock().unwrap();
        let csr = locked.entry("g".to_string()).or_default();
        csr.add_edge("a", "E", "b").unwrap();
        assert!(csr.contains_node("a"));
        assert!(csr.contains_node("b"));
    }
}
