//! Build an outbound [`CollectionDescriptor`] from a locally persisted
//! [`CollectionMeta`], for the `CollectionSchema` (opcode `0x13`) announce
//! emitted before a collection's first CRDT delta in a sync session.
//!
//! Mirrors Origin's announce semantics
//! (`control/server/sync/session_handler/announce.rs`): a lossless
//! `descriptor_json` (set on collections materialized from a prior inbound
//! sync announcement) is preferred verbatim; otherwise the descriptor is
//! synthesized from the locally-known collection type. Strict-document
//! collections without a stored descriptor cannot be losslessly
//! reconstructed from `CollectionMeta`'s string-typed fields (`FromStr` on
//! `CollectionType` returns an empty placeholder schema for
//! `"document_strict"`) and are skipped with a warning.

use nodedb_types::collection::CollectionType;
use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
use nodedb_types::id::DatabaseId;
use nodedb_types::sync::wire::CollectionDescriptor;

use crate::nodedb::collection::CollectionMeta;

/// Build a `CollectionDescriptor` from persisted collection metadata.
///
/// Returns `None` (with a `tracing::warn!`) when the descriptor cannot be
/// reconstructed losslessly — the caller must skip announcing the
/// collection this tick rather than emit an incorrect/empty schema.
pub(crate) fn descriptor_from_meta(meta: &CollectionMeta) -> Option<CollectionDescriptor> {
    if let Some(json) = &meta.descriptor_json {
        match sonic_rs::from_str::<CollectionDescriptor>(json) {
            Ok(descriptor) => return Some(descriptor),
            Err(e) => {
                tracing::warn!(
                    collection = %meta.name,
                    error = %e,
                    "descriptor_json present but failed to deserialize; falling back to synthesis"
                );
            }
        }
    }

    let collection_type = synthesize_collection_type(meta)?;
    let primary = if collection_type.is_kv() {
        PrimaryEngine::KeyValue
    } else {
        PrimaryEngine::Document
    };
    let partition_strategy = PartitionStrategy::default_for_collection_type(&collection_type);

    Some(CollectionDescriptor {
        // Lite is single-tenant (tenant 1, matching the sync session identity)
        // and single-database. The announced descriptor MUST carry the same
        // (tenant, database) the synced data lands under on Origin — deltas
        // apply under `DatabaseId::DEFAULT` (0), so registering the collection
        // under any other database would make it invisible to queries.
        tenant_id: 1,
        database_id: DatabaseId::DEFAULT,
        name: meta.name.clone(),
        collection_type,
        bitemporal: meta.bitemporal,
        fields: meta.fields.clone(),
        primary,
        vector_primary: None,
        partition_strategy,
        declared_primary_key: None,
        descriptor_version: 1,
    })
}

/// Synthesize a `CollectionType` from `meta.collection_type` when no
/// `descriptor_json` is available. Returns `None` (with a warning) for
/// types that cannot be losslessly recovered from the string tag alone.
fn synthesize_collection_type(meta: &CollectionMeta) -> Option<CollectionType> {
    match meta.collection_type.as_str() {
        // Lite's local `create_collection` persists the legacy tag
        // "document" (not the canonical "document_schemaless", which is
        // rejected as a deprecated alias by `CollectionType::from_str`).
        // Schemaless documents carry no field schema, so synthesis is exact.
        "document" | "document_schemaless" => Some(CollectionType::document()),
        "kv" => {
            let Some(config_json) = &meta.config_json else {
                tracing::warn!(
                    collection = %meta.name,
                    "kv collection missing config_json; cannot announce without a schema"
                );
                return None;
            };
            match sonic_rs::from_str::<nodedb_types::KvConfig>(config_json) {
                Ok(cfg) => Some(CollectionType::KeyValue(cfg)),
                Err(e) => {
                    tracing::warn!(
                        collection = %meta.name,
                        error = %e,
                        "kv collection config_json failed to deserialize; cannot announce"
                    );
                    None
                }
            }
        }
        // Columnar-family types carry no column schema in `CollectionType`
        // itself, so the placeholder-free `FromStr` parse is exact.
        "columnar" | "timeseries" | "spatial" => match meta.collection_type.parse() {
            Ok(ct) => Some(ct),
            Err(e) => {
                tracing::warn!(
                    collection = %meta.name,
                    error = %e,
                    "failed to parse columnar-family collection type; cannot announce"
                );
                None
            }
        },
        "document_strict" => {
            tracing::warn!(
                collection = %meta.name,
                "strict collection without descriptor_json cannot be announced \
                 (FromStr yields an empty placeholder schema)"
            );
            None
        }
        other => {
            tracing::warn!(
                collection = %meta.name,
                collection_type = other,
                "unrecognized collection type; cannot announce"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_meta(collection_type: &str) -> CollectionMeta {
        CollectionMeta {
            name: "widgets".to_string(),
            collection_type: collection_type.to_string(),
            created_at_ms: 0,
            fields: Vec::new(),
            config_json: None,
            descriptor_json: None,
            bitemporal: false,
        }
    }

    #[test]
    fn descriptor_json_roundtrips_verbatim() {
        let descriptor = CollectionDescriptor {
            tenant_id: 7,
            database_id: DatabaseId::new(42),
            name: "widgets".into(),
            collection_type: CollectionType::document(),
            bitemporal: true,
            fields: vec![("email".into(), "string".into())],
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: PartitionStrategy::CollectionHomed,
            declared_primary_key: Some("id".into()),
            descriptor_version: 3,
        };
        let mut meta = base_meta("document");
        meta.descriptor_json = Some(sonic_rs::to_string(&descriptor).unwrap());

        let out = descriptor_from_meta(&meta).expect("descriptor should roundtrip");
        assert_eq!(out, descriptor);
    }

    #[test]
    fn document_synthesizes_schemaless() {
        let meta = base_meta("document");
        let out = descriptor_from_meta(&meta).expect("document should synthesize");
        assert!(out.collection_type.is_schemaless());
        assert_eq!(out.primary, PrimaryEngine::Document);
    }

    #[test]
    fn kv_rebuilds_from_config_json() {
        let schema = nodedb_types::columnar::StrictSchema::new(vec![
            nodedb_types::columnar::ColumnDef::required(
                "key",
                nodedb_types::columnar::ColumnType::String,
            )
            .with_primary_key(),
        ])
        .unwrap();
        let config = nodedb_types::KvConfig {
            schema,
            ttl: None,
            capacity_hint: 0,
            inline_threshold: nodedb_types::kv::KV_DEFAULT_INLINE_THRESHOLD,
        };
        let mut meta = base_meta("kv");
        meta.config_json = Some(sonic_rs::to_string(&config).unwrap());

        let out = descriptor_from_meta(&meta).expect("kv should rebuild");
        assert!(out.collection_type.is_kv());
        assert_eq!(out.primary, PrimaryEngine::KeyValue);
    }

    #[test]
    fn kv_without_config_json_is_none() {
        let meta = base_meta("kv");
        assert!(descriptor_from_meta(&meta).is_none());
    }

    #[test]
    fn strict_without_descriptor_json_is_none() {
        let meta = base_meta("document_strict");
        assert!(descriptor_from_meta(&meta).is_none());
    }

    #[test]
    fn unrecognized_type_is_none() {
        let meta = base_meta("bogus");
        assert!(descriptor_from_meta(&meta).is_none());
    }
}
