//! Inbound collection-schema registration.
//!
//! Materializes a collection locally from a [`CollectionDescriptor`] announced
//! by a sync peer (opcode `0x13`, `SyncMessageType::CollectionSchema`). This is
//! the receive side of collection-schema sync: it create-if-absent registers
//! the collection with the correct engine and persists an authoritative
//! [`CollectionMeta`] so the SQL catalog surfaces the real engine, bitemporal
//! flag, and column schema instead of hardcoded defaults.

use nodedb_types::collection::CollectionType;
use nodedb_types::columnar::{ColumnarProfile, DocumentMode};
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::sync::wire::CollectionDescriptor;

use crate::nodedb::collection::CollectionMeta;
use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Create-if-absent a local collection from an inbound sync descriptor.
    ///
    /// Idempotent: if a collection with this name already has persisted
    /// metadata, this is a no-op (create-only — never clobber existing local
    /// state), mirroring Origin's `PutCollectionIfAbsent`. Otherwise it
    /// performs per-engine materialization and writes one authoritative
    /// [`CollectionMeta`] carrying the collection-type string, field hints,
    /// engine config JSON (for KV), the full serialized descriptor, and the
    /// bitemporal flag.
    pub(crate) async fn register_collection_from_descriptor(
        &self,
        descriptor: &CollectionDescriptor,
    ) -> NodeDbResult<()> {
        let name = descriptor.name.as_str();
        let key = format!("collection:{name}");

        // Idempotent create-only: never clobber existing local metadata.
        if self
            .storage
            .get(nodedb_types::Namespace::Meta, key.as_bytes())
            .await?
            .is_some()
        {
            return Ok(());
        }

        // Per-engine materialization. Exhaustive over `CollectionType` so a new
        // engine variant forces a decision here rather than silently NOP-ing.
        let mut config_json: Option<String> = None;
        match &descriptor.collection_type {
            // Schemaless documents: the CRDT engine creates the collection
            // lazily on first write. Nothing to register engine-side.
            CollectionType::Document(DocumentMode::Schemaless) => {}
            // Strict documents: register the schema with the strict engine so
            // reads/writes and the catalog resolve the real columns. Guard on
            // absence so a partially-materialized prior attempt is tolerated.
            CollectionType::Document(DocumentMode::Strict(schema)) => {
                if self.strict.schema(name).is_none() {
                    self.strict.create_collection(name, schema.clone()).await?;
                }
            }
            // Columnar / timeseries / spatial share one storage core and are
            // created lazily on first insert. Persist meta only.
            CollectionType::Columnar(ColumnarProfile::Plain)
            | CollectionType::Columnar(ColumnarProfile::Timeseries { .. })
            | CollectionType::Columnar(ColumnarProfile::Spatial { .. }) => {}
            // Key-Value: reuse the existing KV create path to register the
            // config, then overwrite the meta below with the authoritative one
            // (descriptor_json + bitemporal). One source of truth wins.
            CollectionType::KeyValue(cfg) => {
                self.create_kv_collection(name, cfg).await?;
                config_json = Some(
                    sonic_rs::to_string(cfg).map_err(|e| NodeDbError::storage(e.to_string()))?,
                );
            }
        }

        let descriptor_json =
            sonic_rs::to_string(descriptor).map_err(|e| NodeDbError::storage(e.to_string()))?;

        let meta = CollectionMeta {
            name: name.to_string(),
            collection_type: descriptor.collection_type.as_str().to_string(),
            created_at_ms: crate::runtime::now_millis(),
            fields: descriptor.fields.clone(),
            config_json,
            descriptor_json: Some(descriptor_json),
            bitemporal: descriptor.bitemporal,
        };
        let bytes = sonic_rs::to_vec(&meta).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::collection::CollectionType;
    use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::id::DatabaseId;
    use nodedb_types::sync::wire::CollectionDescriptor;

    use crate::PagedbStorageMem;
    use crate::nodedb::core::NodeDbLite;

    async fn make_db() -> NodeDbLite<PagedbStorageMem> {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        NodeDbLite::open(storage, 1).await.unwrap()
    }

    fn base_descriptor(name: &str, ct: CollectionType, bitemporal: bool) -> CollectionDescriptor {
        CollectionDescriptor {
            tenant_id: 1,
            database_id: DatabaseId::new(1),
            name: name.into(),
            collection_type: ct,
            bitemporal,
            fields: vec![("email".into(), "TEXT".into())],
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: PartitionStrategy::default(),
            declared_primary_key: None,
            descriptor_version: 1,
        }
    }

    fn strict_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .unwrap()
    }

    fn kv_type() -> CollectionType {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Bytes),
        ])
        .unwrap();
        CollectionType::kv(schema)
    }

    async fn meta_of(
        db: &NodeDbLite<PagedbStorageMem>,
        name: &str,
    ) -> crate::nodedb::collection::CollectionMeta {
        let metas =
            crate::nodedb::collection::ddl::load_persisted_collection_metas(db.storage.as_ref())
                .await
                .unwrap();
        metas.get(name).cloned().expect("meta persisted")
    }

    #[tokio::test]
    async fn apply_strict_persists_real_meta() {
        let db = make_db().await;
        let desc = base_descriptor("s", CollectionType::strict(strict_schema()), true);
        db.register_collection_from_descriptor(&desc).await.unwrap();

        let meta = meta_of(&db, "s").await;
        assert_eq!(meta.collection_type, "document_strict");
        assert!(meta.bitemporal);
        // descriptor_json round-trips back to the same descriptor.
        let dj = meta.descriptor_json.expect("descriptor_json set");
        let back: CollectionDescriptor = sonic_rs::from_str(&dj).unwrap();
        assert_eq!(back, desc);
        // Strict engine now holds the real schema.
        assert!(db.strict.schema("s").is_some());
    }

    #[tokio::test]
    async fn apply_kv_persists_config_and_descriptor() {
        let db = make_db().await;
        let desc = base_descriptor("k", kv_type(), false);
        db.register_collection_from_descriptor(&desc).await.unwrap();

        let meta = meta_of(&db, "k").await;
        assert_eq!(meta.collection_type, "kv");
        assert!(!meta.bitemporal);
        assert!(meta.config_json.is_some());
        assert!(meta.descriptor_json.is_some());
    }

    #[tokio::test]
    async fn apply_schemaless_persists_meta() {
        let db = make_db().await;
        let desc = base_descriptor("d", CollectionType::document(), false);
        db.register_collection_from_descriptor(&desc).await.unwrap();

        let meta = meta_of(&db, "d").await;
        assert_eq!(meta.collection_type, "document_schemaless");
        assert!(!meta.bitemporal);
    }

    #[tokio::test]
    async fn apply_columnar_bitemporal_persists_meta() {
        let db = make_db().await;
        let desc = base_descriptor("c", CollectionType::columnar(), true);
        db.register_collection_from_descriptor(&desc).await.unwrap();

        let meta = meta_of(&db, "c").await;
        assert_eq!(meta.collection_type, "columnar");
        assert!(meta.bitemporal);
    }

    #[tokio::test]
    async fn apply_is_idempotent_and_never_clobbers() {
        let db = make_db().await;
        let desc = base_descriptor("s", CollectionType::strict(strict_schema()), true);
        db.register_collection_from_descriptor(&desc).await.unwrap();

        // Second apply with a DIFFERENT descriptor must be a no-op: the
        // original meta must survive unchanged (create-only semantics).
        let mut desc2 = desc.clone();
        desc2.bitemporal = false;
        desc2.collection_type = CollectionType::document();
        db.register_collection_from_descriptor(&desc2)
            .await
            .unwrap();

        let meta = meta_of(&db, "s").await;
        assert_eq!(meta.collection_type, "document_strict");
        assert!(meta.bitemporal, "original meta must not be clobbered");
    }
}
