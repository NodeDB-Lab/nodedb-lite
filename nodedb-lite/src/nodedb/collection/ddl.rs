//! Collection DDL: create, rop, list collections with metadata.

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// Collection metadata stored in the KV store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMeta {
    pub name: String,
    pub collection_type: String,
    pub created_at_ms: u64,
    pub fields: Vec<(String, String)>,
    /// Optional JSON-serialized engine config (e.g., `KvConfig` for KV collections,
    /// `StrictSchema` for strict collections). Empty for schemaless document collections.
    #[serde(default)]
    pub config_json: Option<String>,
    /// Optional JSON-serialized full `CollectionDescriptor` (from
    /// `nodedb_types::sync::wire::CollectionDescriptor`). Set for collections
    /// materialized from an inbound sync schema announcement so the SQL catalog
    /// can surface the real engine, bitemporal flag, and column schema, and so a
    /// future emit path can reconstruct the descriptor losslessly. `None` for
    /// locally-created collections.
    #[serde(default)]
    pub descriptor_json: Option<String>,
    /// Whether the collection tracks system-time + valid-time versions.
    #[serde(default)]
    pub bitemporal: bool,
    /// Whether the collection was declared with `WITH (crdt=true)`, i.e. its
    /// DML is routed through the CRDT (Loro) path rather than the plain
    /// document path. Announced to Origin in the collection descriptor so the
    /// peer applies the same routing. A declared property, not an inferred one:
    /// Lite's schemaless store happens to be Loro-backed, but that alone does
    /// NOT make a collection CRDT-declared.
    #[serde(default)]
    pub crdt: bool,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Create a collection with optional schema.
    ///
    /// If the collection already exists, returns Ok (idempotent).
    /// Schema is advisory — documents are schemaless by default.
    pub async fn create_collection(
        &self,
        name: &str,
        fields: &[(String, String)],
    ) -> NodeDbResult<()> {
        let meta = CollectionMeta {
            name: name.to_string(),
            collection_type: "document".to_string(),
            created_at_ms: crate::runtime::now_millis(),
            fields: fields.to_vec(),
            config_json: None,
            descriptor_json: None,
            bitemporal: false,
            crdt: false,
        };
        let key = format!("collection:{name}");
        let bytes = sonic_rs::to_vec(&meta).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }

    /// Create a KV collection with typed schema and optional TTL.
    ///
    /// Stores the `KvConfig` as JSON in the collection metadata so that the
    /// KV engine can reconstruct the schema on startup.
    pub async fn create_kv_collection(
        &self,
        name: &str,
        config: &nodedb_types::KvConfig,
    ) -> NodeDbResult<()> {
        let fields: Vec<(String, String)> = config
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.column_type.to_string()))
            .collect();

        let config_json =
            sonic_rs::to_string(config).map_err(|e| NodeDbError::storage(e.to_string()))?;

        let meta = CollectionMeta {
            name: name.to_string(),
            collection_type: "kv".to_string(),
            created_at_ms: crate::runtime::now_millis(),
            fields,
            config_json: Some(config_json),
            descriptor_json: None,
            bitemporal: false,
            crdt: false,
        };
        let key = format!("collection:{name}");
        let bytes = sonic_rs::to_vec(&meta).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }

    /// Synthesize a base document `CollectionMeta` for an implicit CRDT-only
    /// collection — one that exists in CRDT state (created by a document upsert,
    /// a vector insert, or an FTS write, with no explicit `create_collection`)
    /// and therefore has no persisted `collection:` meta. Returns `None` if the
    /// collection is not present in CRDT state (or is an internal `__` name).
    ///
    /// Mirrors the synthetic entry [`list_collections`](Self::list_collections)
    /// already produces for implicit collections, so the outbound sync announce
    /// path can register such collections on Origin with a base document
    /// descriptor before their overlay (vector/FTS) or document data arrives.
    /// Equivalent to the meta an explicit `create_collection` would persist
    /// (`collection_type = "document"`, no `descriptor_json`).
    pub(crate) fn implicit_collection_meta(&self, name: &str) -> Option<CollectionMeta> {
        if name.starts_with("__") {
            return None;
        }
        let crdt = self.crdt.lock_or_recover();
        if !crdt.collection_names().iter().any(|n| n == name) {
            return None;
        }
        Some(CollectionMeta {
            name: name.to_string(),
            collection_type: "document".to_string(),
            created_at_ms: 0,
            fields: Vec::new(),
            config_json: None,
            descriptor_json: None,
            bitemporal: false,
            crdt: false,
        })
    }

    /// Drop a collection — deletes all documents and metadata.
    ///
    /// Uses `clear_collection` for single-batch deletion (one Loro delta
    /// for all document removals). Also removes the text index.
    pub async fn drop_collection(&self, name: &str) -> NodeDbResult<()> {
        // Batch-delete all documents in one delta.
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.clear_collection(name).map_err(NodeDbError::storage)?;
        }

        // Remove text index for this collection.
        {
            let mut fts = self.fts_state.manager.lock_or_recover();
            fts.drop_collection(name);
        }

        // Delete collection metadata from the KV store.
        let key = format!("collection:{name}");
        self.storage
            .delete(nodedb_types::Namespace::Meta, key.as_bytes())
            .await?;
        Ok(())
    }

    /// List all collections.
    pub async fn list_collections(&self) -> NodeDbResult<Vec<CollectionMeta>> {
        let pairs = self
            .storage
            .scan_prefix(nodedb_types::Namespace::Meta, b"collection:")
            .await?;
        let mut result = Vec::new();
        for (_, value) in &pairs {
            if let Ok(meta) = sonic_rs::from_slice::<CollectionMeta>(value) {
                result.push(meta);
            }
        }
        // Also include implicit collections (from CRDT state without explicit DDL).
        let crdt = self.crdt.lock_or_recover();
        let crdt_names = crdt.collection_names();
        let explicit: std::collections::HashSet<String> =
            result.iter().map(|m| m.name.clone()).collect();
        for name in crdt_names {
            if !name.starts_with("__") && !explicit.contains(&name) {
                result.push(CollectionMeta {
                    name,
                    collection_type: "document".to_string(),
                    created_at_ms: 0,
                    fields: Vec::new(),
                    config_json: None,
                    descriptor_json: None,
                    bitemporal: false,
                    crdt: false,
                });
            }
        }
        Ok(result)
    }
}

/// Load all explicitly-persisted collection metadata as a name→meta map.
///
/// Unlike [`NodeDbLite::list_collections`], this does NOT merge implicit CRDT
/// collections — it returns only the metas durably written under the
/// `collection:` prefix (via `create_collection`, `create_kv_collection`, or
/// inbound schema sync). The SQL catalog uses this snapshot to surface the real
/// engine, bitemporal flag, and columns for DDL/synced collections, while
/// implicit CRDT-only collections still fall through to engine-based detection.
/// Free-function form so callers that hold only an `&S` (e.g. the SQL query
/// engine building its catalog) can reuse the same scan-and-decode logic.
pub(crate) async fn load_persisted_collection_metas<S: StorageEngine>(
    storage: &S,
) -> NodeDbResult<std::collections::HashMap<String, CollectionMeta>> {
    let pairs = storage
        .scan_prefix(nodedb_types::Namespace::Meta, b"collection:")
        .await?;
    let mut map = std::collections::HashMap::with_capacity(pairs.len());
    for (_, value) in &pairs {
        if let Ok(meta) = sonic_rs::from_slice::<CollectionMeta>(value) {
            map.insert(meta.name.clone(), meta);
        }
    }
    Ok(map)
}
