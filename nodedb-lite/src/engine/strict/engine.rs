//! Strict document engine: Binary Tuple CRUD against the StorageEngine.
//!
//! Each strict collection stores rows as Binary Tuples in the `Strict`
//! namespace, keyed by `{collection}:{pk_bytes}`. The schema is stored
//! in the `Meta` namespace as `strict_schema:{collection}`.
//!
//! Uses the `nodedb-strict` crate for encoding/decoding and Arrow extraction.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_strict::{StrictError, TupleDecoder, TupleEncoder};
use nodedb_types::Namespace;
use nodedb_types::columnar::StrictSchema;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Meta key prefix for strict schemas in the Meta namespace.
pub(super) const META_STRICT_SCHEMA_PREFIX: &str = "strict_schema:";

/// Meta key listing all strict collections.
pub(super) const META_STRICT_COLLECTIONS: &[u8] = b"meta:strict_collections";

/// Manages all strict document collections for a NodeDbLite instance.
///
/// Holds per-collection schemas, encoders, and decoders. Delegates storage
/// to the shared `StorageEngine` under `Namespace::Strict`.
pub struct StrictEngine<S: StorageEngine> {
    pub(super) storage: Arc<S>,
    /// Per-collection schema + encoder + decoder.
    pub(super) collections: HashMap<String, CollectionState>,
}

pub(super) struct CollectionState {
    pub(super) schema: StrictSchema,
    pub(super) encoder: TupleEncoder,
    pub(super) decoder: TupleDecoder,
    /// Column indices of PK columns (for key construction).
    pub(super) pk_indices: Vec<usize>,
    /// Maps schema version → column count at that version.
    /// Used for multi-version reads after ALTER ADD COLUMN.
    pub(super) version_column_counts: HashMap<u16, usize>,
}

impl CollectionState {
    pub(super) fn new(schema: StrictSchema) -> Self {
        let pk_indices: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();
        let encoder = TupleEncoder::new(&schema);
        let decoder = TupleDecoder::new(&schema);
        // Initial version maps to current column count.
        let mut version_column_counts = HashMap::new();
        version_column_counts.insert(schema.version, schema.columns.len());
        Self {
            schema,
            encoder,
            decoder,
            pk_indices,
            version_column_counts,
        }
    }

    /// Build a storage key from PK values: `{collection}:{pk_bytes}`.
    pub(super) fn storage_key(&self, collection: &str, values: &[Value]) -> Vec<u8> {
        let mut key = collection.as_bytes().to_vec();
        key.push(b':');
        for &pk_idx in &self.pk_indices {
            encode_pk_component(&mut key, &values[pk_idx]);
        }
        key
    }

    /// Build a storage key from a single PK value (common case).
    pub(super) fn storage_key_from_pk(&self, collection: &str, pk: &Value) -> Vec<u8> {
        let mut key = collection.as_bytes().to_vec();
        key.push(b':');
        encode_pk_component(&mut key, pk);
        key
    }
}

/// Encode a PK value into sortable bytes for the storage key.
///
/// Uses big-endian for integers (preserves sort order in redb scans),
/// raw UTF-8 for strings, and raw bytes for UUIDs.
fn encode_pk_component(key: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Integer(v) => {
            // XOR with sign bit to make signed integers sort correctly as unsigned bytes.
            let sortable = (*v as u64) ^ (1u64 << 63);
            key.extend_from_slice(&sortable.to_be_bytes());
        }
        Value::String(s) => {
            key.extend_from_slice(s.as_bytes());
        }
        Value::Uuid(s) => {
            // Store UUID as raw UTF-8 bytes for sortable key. UUID strings
            // are already lexicographically sortable in hyphenated form.
            key.extend_from_slice(s.as_bytes());
        }
        Value::Decimal(d) => {
            key.extend_from_slice(&d.serialize());
        }
        _ => {
            // Fallback: use debug representation. Should not happen for valid PK types.
            key.extend_from_slice(format!("{value:?}").as_bytes());
        }
    }
}

/// Convert a `StrictError` to `LiteError`.
pub(super) fn strict_err_to_lite(e: StrictError) -> LiteError {
    LiteError::BadRequest {
        detail: e.to_string(),
    }
}

impl<S: StorageEngine> StrictEngine<S> {
    /// Create a new empty strict engine.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            collections: HashMap::new(),
        }
    }

    /// Restore strict collections from storage on startup.
    pub async fn restore(storage: Arc<S>) -> Result<Self, LiteError> {
        let mut engine = Self::new(Arc::clone(&storage));

        // Load collection list from meta.
        let list_bytes = storage
            .get(Namespace::Meta, META_STRICT_COLLECTIONS)
            .await?;
        let names: Vec<String> = match list_bytes {
            Some(bytes) => zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Storage {
                detail: format!("strict collection list deserialization failed: {e}"),
            })?,
            None => Vec::new(),
        };

        // Load each schema.
        for name in names {
            let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
            if let Some(schema_bytes) = storage.get(Namespace::Meta, meta_key.as_bytes()).await?
                && let Ok(schema) = zerompk::from_msgpack::<StrictSchema>(&schema_bytes)
            {
                engine
                    .collections
                    .insert(name, CollectionState::new(schema));
            }
        }

        Ok(engine)
    }

    // -- Internal helpers --

    pub(super) fn get_state(&self, collection: &str) -> Result<&CollectionState, LiteError> {
        self.collections
            .get(collection)
            .ok_or(LiteError::BadRequest {
                detail: format!("strict collection '{collection}' does not exist"),
            })
    }
}
