//! Shared persistence of a `CollectionMeta` for typed-engine collections
//! (columnar / timeseries / spatial) created locally on Lite.
//!
//! Document and KV creates already persist their own `CollectionMeta`; the
//! columnar-family engines historically stored their schema only in their own
//! in-process engine registry, so `SyncDelegate::get_collection_meta` found
//! nothing and the outbound `CollectionSchema` announce was skipped — a
//! lite-only columnar/timeseries/spatial collection never registered on Origin.
//!
//! This helper writes a `CollectionMeta` carrying a lossless
//! `descriptor_json` (the full `CollectionDescriptor`) so the announce
//! reconstructs the exact engine type verbatim. String-tag synthesis is exact
//! for `columnar` but lossy for `timeseries` (defaults the time key / interval)
//! and `spatial` (defaults the geometry column), so the descriptor is stored
//! rather than relying on the tag.

use nodedb_types::collection::CollectionType;
use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
use nodedb_types::id::DatabaseId;
use nodedb_types::sync::wire::CollectionDescriptor;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Persist a `CollectionMeta` (with a lossless `descriptor_json`) for a
    /// locally-created typed-engine collection so the sync layer can announce
    /// it to Origin.
    ///
    /// `fields` is the `(name, type)` column list surfaced to the SQL catalog;
    /// `collection_type` is the exact engine type carried in the descriptor.
    pub(in crate::query) async fn persist_engine_collection_meta(
        &self,
        name: &str,
        collection_type: CollectionType,
        fields: Vec<(String, String)>,
        bitemporal: bool,
    ) -> Result<(), LiteError> {
        let partition_strategy = PartitionStrategy::default_for_collection_type(&collection_type);

        let descriptor = CollectionDescriptor {
            // Lite is single-tenant (tenant 1) and single-database; the
            // announced descriptor MUST carry the same (tenant, database) the
            // synced data lands under on Origin — data applies under
            // `DatabaseId::DEFAULT`, so any other database would make the
            // registered collection invisible to queries.
            tenant_id: 1,
            database_id: DatabaseId::DEFAULT,
            name: name.to_string(),
            collection_type: collection_type.clone(),
            bitemporal,
            // Columnar-family engine collections are never CRDT-declared;
            // `WITH (crdt=true)` is only accepted on document collections.
            crdt: false,
            fields: fields.clone(),
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy,
            declared_primary_key: None,
            descriptor_version: 1,
        };

        let descriptor_json = sonic_rs::to_string(&descriptor)
            .map_err(|e| LiteError::Query(format!("serialize descriptor: {e}")))?;

        let meta = crate::nodedb::collection::ddl::CollectionMeta {
            name: name.to_string(),
            collection_type: collection_type.to_string(),
            created_at_ms: crate::runtime::now_millis(),
            fields,
            config_json: None,
            descriptor_json: Some(descriptor_json),
            bitemporal,
            crdt: false,
        };

        let key = format!("collection:{name}");
        let bytes =
            sonic_rs::to_vec(&meta).map_err(|e| LiteError::Query(format!("serialize: {e}")))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await
            .map_err(|e| LiteError::Query(format!("storage: {e}")))?;

        Ok(())
    }

    /// Remove a typed-engine collection's persisted `CollectionMeta` on drop,
    /// so a later re-create re-announces cleanly.
    pub(in crate::query) async fn remove_engine_collection_meta(
        &self,
        name: &str,
    ) -> Result<(), LiteError> {
        let key = format!("collection:{name}");
        self.storage
            .delete(nodedb_types::Namespace::Meta, key.as_bytes())
            .await
            .map_err(|e| LiteError::Query(format!("storage: {e}")))?;
        Ok(())
    }
}
