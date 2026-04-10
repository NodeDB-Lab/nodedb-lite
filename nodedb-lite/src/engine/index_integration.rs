//! Index integration for strict and columnar collections.
//!
//! Provides helper functions to maintain secondary indexes (R-tree, HNSW,
//! text/BM25) when rows are inserted or deleted from strict or columnar
//! collections. These indexes enable spatial queries, vector search, and
//! full-text search over typed collections.
//!
//! Uses the existing per-collection index infrastructure in NodeDbLite
//! (HNSW indices, spatial manager, text indices) — the same indexes used
//! by schemaless document collections.
//!
//! Callers: `NodeDbLite` should call `index_row` after `StrictEngine.insert()`
//! or `ColumnarEngine.insert()` to maintain secondary indexes. Call
//! `deindex_row_text` before `delete()` to remove text index entries.

use std::collections::HashMap;
use std::sync::Mutex;

use nodedb_types::columnar::ColumnType;
use nodedb_types::geometry::Geometry;
use nodedb_types::value::Value;
use nodedb_vector::HnswIndex;

use crate::engine::fts::FtsCollectionManager;
use crate::engine::spatial::SpatialIndexManager;
use crate::nodedb::lock_ext::LockExt;

/// Index a row from a strict or columnar collection into secondary indexes.
///
/// Inspects the schema's column types and routes values to the appropriate
/// index: GEOMETRY → R-tree, VECTOR → HNSW, STRING → text/BM25.
///
/// `collection` is the collection name (used as the index key).
/// `row_id` is a string identifier for the row (typically the PK value).
/// `columns` are the column definitions from the schema.
/// `values` are the row's values in schema order.
pub fn index_row(
    collection: &str,
    row_id: &str,
    columns: &[nodedb_types::columnar::ColumnDef],
    values: &[Value],
    hnsw_indices: &Mutex<HashMap<String, HnswIndex>>,
    spatial: &Mutex<SpatialIndexManager>,
    fts: &Mutex<FtsCollectionManager>,
) {
    for (i, col) in columns.iter().enumerate() {
        if i >= values.len() {
            break;
        }
        let val = &values[i];

        match &col.column_type {
            ColumnType::Geometry => {
                index_geometry(collection, &col.name, row_id, val, spatial);
            }
            ColumnType::Vector(dim) => {
                index_vector(collection, &col.name, row_id, val, *dim, hnsw_indices);
            }
            ColumnType::String => {
                index_text(collection, &col.name, row_id, val, fts);
            }
            _ => {} // No secondary index for other types.
        }
    }
}

/// Remove a row's text entries from inverted indexes.
///
/// Only handles String columns (BM25 inverted index). R-tree spatial removal
/// requires the original geometry (uses `SpatialIndexManager.remove_document`
/// directly). HNSW uses soft-delete (tombstone) which doesn't need the original
/// vector — call `HnswIndex.delete(node_id)` directly when the node ID is known.
pub fn deindex_row_text(
    collection: &str,
    row_id: &str,
    columns: &[nodedb_types::columnar::ColumnDef],
    fts: &Mutex<FtsCollectionManager>,
) {
    for col in columns {
        if matches!(col.column_type, ColumnType::String) {
            remove_text(collection, &col.name, row_id, fts);
        }
    }
}

/// Index a geometry value into the spatial R-tree.
fn index_geometry(
    collection: &str,
    field: &str,
    doc_id: &str,
    value: &Value,
    spatial: &Mutex<SpatialIndexManager>,
) {
    let geom = match value {
        Value::Geometry(g) => g.clone(),
        Value::String(s) => {
            // Try parsing as GeoJSON.
            match sonic_rs::from_str::<Geometry>(s) {
                Ok(g) => g,
                Err(_) => return,
            }
        }
        _ => return,
    };

    let mut spatial = spatial.lock_or_recover();
    spatial.index_document(collection, field, doc_id, &geom);
}

/// Index a vector value into the HNSW index.
fn index_vector(
    collection: &str,
    field: &str,
    _doc_id: &str,
    value: &Value,
    dim: u32,
    hnsw_indices: &Mutex<HashMap<String, HnswIndex>>,
) {
    let vector: Vec<f32> = match value {
        Value::Array(arr) => arr
            .iter()
            .take(dim as usize)
            .map(|v| match v {
                Value::Float(f) => *f as f32,
                Value::Integer(n) => *n as f32,
                _ => 0.0,
            })
            .collect(),
        Value::Bytes(b) => {
            // Packed f32 bytes.
            b.chunks_exact(4)
                .take(dim as usize)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect()
        }
        _ => return,
    };

    if vector.len() != dim as usize {
        return;
    }

    let index_key = format!("{collection}:{field}");
    let mut indices = hnsw_indices.lock_or_recover();
    let index = indices
        .entry(index_key)
        .or_insert_with(|| HnswIndex::new(dim as usize, nodedb_types::HnswParams::default()));
    // insert() takes Vec<f32> and returns Result — ignore error for index integration.
    let _ = index.insert(vector);
}

/// Index a string value into the inverted text index (BM25).
fn index_text(
    collection: &str,
    field: &str,
    doc_id: &str,
    value: &Value,
    fts: &Mutex<FtsCollectionManager>,
) {
    let text = match value {
        Value::String(s) => s.as_str(),
        _ => return,
    };
    fts.lock_or_recover()
        .index_field(collection, field, doc_id, text);
}

/// Remove a document from the text index.
fn remove_text(collection: &str, field: &str, doc_id: &str, fts: &Mutex<FtsCollectionManager>) {
    fts.lock_or_recover()
        .remove_field(collection, field, doc_id);
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;

    use super::*;
    use crate::engine::fts::FtsCollectionManager;

    #[test]
    fn index_row_routes_geometry() {
        let columns = vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::nullable("geom", ColumnType::Geometry),
        ];
        let values = vec![
            Value::Integer(1),
            Value::Geometry(Geometry::Point {
                coordinates: [10.0, 20.0],
            }),
        ];

        let hnsw = Mutex::new(HashMap::new());
        let spatial = Mutex::new(SpatialIndexManager::new());
        let text = Mutex::new(FtsCollectionManager::new());

        index_row("test", "1", &columns, &values, &hnsw, &spatial, &text);

        let spatial = spatial.lock().expect("lock");
        assert!(!spatial.is_empty());
    }

    #[test]
    fn index_row_routes_vector() {
        let columns = vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::nullable("emb", ColumnType::Vector(3)),
        ];
        let values = vec![
            Value::Integer(1),
            Value::Array(vec![
                Value::Float(1.0),
                Value::Float(2.0),
                Value::Float(3.0),
            ]),
        ];

        let hnsw = Mutex::new(HashMap::new());
        let spatial = Mutex::new(SpatialIndexManager::new());
        let text = Mutex::new(FtsCollectionManager::new());

        index_row("test", "1", &columns, &values, &hnsw, &spatial, &text);

        let hnsw = hnsw.lock().expect("lock");
        assert!(hnsw.contains_key("test:emb"));
        assert_eq!(hnsw["test:emb"].len(), 1);
    }

    #[test]
    fn index_row_routes_text() {
        let columns = vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::required("body", ColumnType::String),
        ];
        let values = vec![
            Value::Integer(1),
            Value::String("hello world search test".into()),
        ];

        let hnsw = Mutex::new(HashMap::new());
        let spatial = Mutex::new(SpatialIndexManager::new());
        let text = Mutex::new(FtsCollectionManager::new());

        index_row("test", "1", &columns, &values, &hnsw, &spatial, &text);

        let text = text.lock().expect("lock");
        assert!(
            !text.is_empty(),
            "text FTS index should have entries after indexing a String column"
        );
    }

    #[test]
    fn index_row_skips_non_indexable() {
        let columns = vec![
            ColumnDef::required("id", ColumnType::Int64),
            ColumnDef::required("count", ColumnType::Int64),
        ];
        let values = vec![Value::Integer(1), Value::Integer(42)];

        let hnsw = Mutex::new(HashMap::new());
        let spatial = Mutex::new(SpatialIndexManager::new());
        let text = Mutex::new(FtsCollectionManager::new());

        index_row("test", "1", &columns, &values, &hnsw, &spatial, &text);

        // No indexes should be populated for Int64 columns.
        assert!(hnsw.lock().expect("lock").is_empty());
        assert!(spatial.lock().expect("lock").is_empty());
        assert!(
            text.lock().expect("lock").is_empty(),
            "no text index for Int64 columns"
        );
    }
}
