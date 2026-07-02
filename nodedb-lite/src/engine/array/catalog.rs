//! Array catalog persisted in the `Array` namespace.
//!
//! Key layout: `catalog:{name}` → zerompk-encoded `ArrayCatalogEntry`.
//! The full list of array names is stored under `catalog_index` so cold-start
//! restoration can enumerate entries without a namespace scan.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_types::Namespace;
use serde::{Deserialize, Serialize};

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

const CATALOG_PREFIX: &str = "catalog:";
const CATALOG_INDEX_KEY: &[u8] = b"catalog_index";

/// Persisted metadata for one array.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ArrayCatalogEntry {
    /// Array name (human-readable, also used as the storage key).
    pub name: String,
    /// Serialized schema (zerompk of `ArraySchema`).
    pub schema_bytes: Vec<u8>,
    /// FNV-1a hash of the schema used for open-guard mismatch detection.
    pub schema_hash: u64,
    /// Optional bitemporal audit retention in milliseconds.
    pub audit_retain_ms: Option<i64>,
    /// Minimum allowed audit retention floor.
    pub minimum_audit_retain_ms: Option<u64>,
}

impl ArrayCatalogEntry {
    pub fn schema(&self) -> Result<ArraySchema, LiteError> {
        zerompk::from_msgpack(&self.schema_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode ArraySchema: {e}"),
        })
    }
}

/// In-memory + persisted catalog for all arrays.
pub struct ArrayCatalog<S: StorageEngine> {
    storage: Arc<S>,
    /// Cached entries — the source of truth after open().
    entries: HashMap<String, ArrayCatalogEntry>,
}

impl<S: StorageEngine> ArrayCatalog<S> {
    fn catalog_key(name: &str) -> Vec<u8> {
        let mut k = CATALOG_PREFIX.as_bytes().to_vec();
        k.extend_from_slice(name.as_bytes());
        k
    }

    /// Load catalog from storage.
    pub async fn open(storage: Arc<S>) -> Result<Self, LiteError> {
        let mut entries = HashMap::new();

        let index_bytes = storage.get(Namespace::Array, CATALOG_INDEX_KEY).await?;
        let names: Vec<String> = match index_bytes {
            Some(b) => zerompk::from_msgpack(&b).map_err(|e| LiteError::Serialization {
                detail: format!("decode catalog index: {e}"),
            })?,
            None => Vec::new(),
        };

        for name in names {
            let key = Self::catalog_key(&name);
            if let Some(bytes) = storage.get(Namespace::Array, &key).await? {
                let entry: ArrayCatalogEntry =
                    zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode catalog entry '{name}': {e}"),
                    })?;
                entries.insert(name, entry);
            }
        }

        Ok(Self { storage, entries })
    }

    /// Insert a new entry and persist atomically.
    pub async fn insert(&mut self, entry: ArrayCatalogEntry) -> Result<(), LiteError> {
        let name = entry.name.clone();
        let entry_bytes =
            zerompk::to_msgpack_vec(&entry).map_err(|e| LiteError::Serialization {
                detail: format!("encode catalog entry: {e}"),
            })?;

        self.entries.insert(name.clone(), entry);

        let names: Vec<&str> = self.entries.keys().map(|s| s.as_str()).collect();
        let index_bytes =
            zerompk::to_msgpack_vec(&names).map_err(|e| LiteError::Serialization {
                detail: format!("encode catalog index: {e}"),
            })?;

        let key = Self::catalog_key(&name);
        self.storage
            .batch_write(&[
                crate::storage::engine::WriteOp::Put {
                    ns: Namespace::Array,
                    key,
                    value: entry_bytes,
                },
                crate::storage::engine::WriteOp::Put {
                    ns: Namespace::Array,
                    key: CATALOG_INDEX_KEY.to_vec(),
                    value: index_bytes,
                },
            ])
            .await
    }

    /// Remove an entry from the catalog and persist atomically.
    pub async fn remove(&mut self, name: &str) -> Result<(), LiteError> {
        self.entries.remove(name);

        let names: Vec<&str> = self.entries.keys().map(|s| s.as_str()).collect();
        let index_bytes =
            zerompk::to_msgpack_vec(&names).map_err(|e| LiteError::Serialization {
                detail: format!("encode catalog index: {e}"),
            })?;

        let key = Self::catalog_key(name);
        self.storage
            .batch_write(&[
                crate::storage::engine::WriteOp::Delete {
                    ns: Namespace::Array,
                    key,
                },
                crate::storage::engine::WriteOp::Put {
                    ns: Namespace::Array,
                    key: CATALOG_INDEX_KEY.to_vec(),
                    value: index_bytes,
                },
            ])
            .await
    }

    pub fn get(&self, name: &str) -> Option<&ArrayCatalogEntry> {
        self.entries.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }
}

/// Compute a stable FNV-1a hash for an `ArraySchema`.
pub fn hash_schema(schema: &ArraySchema) -> Result<u64, LiteError> {
    let bytes = zerompk::to_msgpack_vec(schema).map_err(|e| LiteError::Serialization {
        detail: format!("hash_schema encode: {e}"),
    })?;
    let mut h: u64 = 14695981039346656037;
    for b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn test_schema() -> ArraySchema {
        ArraySchemaBuilder::new("t")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap()
    }

    fn make_entry(schema: &ArraySchema) -> ArrayCatalogEntry {
        let schema_bytes = zerompk::to_msgpack_vec(schema).unwrap();
        let schema_hash = hash_schema(schema).unwrap();
        ArrayCatalogEntry {
            name: "t".into(),
            schema_bytes,
            schema_hash,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }
    }

    #[tokio::test]
    async fn insert_and_get() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let mut catalog = ArrayCatalog::open(Arc::clone(&storage)).await.unwrap();
        let schema = test_schema();
        let entry = make_entry(&schema);
        catalog.insert(entry).await.unwrap();
        assert!(catalog.get("t").is_some());
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        {
            let mut catalog = ArrayCatalog::open(Arc::clone(&storage)).await.unwrap();
            catalog.insert(make_entry(&test_schema())).await.unwrap();
        }
        let catalog2 = ArrayCatalog::open(Arc::clone(&storage)).await.unwrap();
        assert!(catalog2.get("t").is_some());
    }

    #[tokio::test]
    async fn remove_entry() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let mut catalog = ArrayCatalog::open(Arc::clone(&storage)).await.unwrap();
        catalog.insert(make_entry(&test_schema())).await.unwrap();
        catalog.remove("t").await.unwrap();
        assert!(catalog.get("t").is_none());
    }

    #[test]
    fn hash_schema_stable() {
        let s = test_schema();
        let h1 = hash_schema(&s).unwrap();
        let h2 = hash_schema(&s).unwrap();
        assert_eq!(h1, h2);
    }
}
