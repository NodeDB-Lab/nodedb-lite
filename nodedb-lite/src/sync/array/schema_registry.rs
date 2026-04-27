//! Per-array [`SchemaDoc`] registry with durable cold-start snapshots.
//!
//! Each registered array has one Loro-backed [`SchemaDoc`] that tracks the
//! array's schema and its [`Hlc`] version. Snapshots are persisted under
//! `Namespace::Meta` keys of the form `b"array.schema_doc:{name}"` so the
//! registry can be reconstructed after process restart without a full sync.
//!
//! # Storage format
//!
//! Each key maps to a MessagePack-encoded [`PersistedSchema`] tuple:
//! `(replica_id: u64, schema_hlc_bytes: [u8; 18], loro_snapshot: Vec<u8>)`.
//!
//! # Cold-start scan
//!
//! [`SchemaRegistry::load`] uses `scan_range_sync` starting at the prefix
//! `b"array.schema_doc:"` and stops when the first key that does not share
//! that prefix is encountered.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::schema_crdt::SchemaDoc;
use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;
use crate::sync::array::replica_state::ReplicaState;

/// Prefix used for schema snapshot storage keys.
const SCHEMA_KEY_PREFIX: &str = "array.schema_doc:";

/// Msgpack-serialisable payload persisted per schema.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct PersistedSchema {
    replica_id: u64,
    schema_hlc_bytes: Vec<u8>,
    loro_snapshot: Vec<u8>,
}

/// Storage key for a named array schema snapshot.
fn schema_key(name: &str) -> Vec<u8> {
    format!("{SCHEMA_KEY_PREFIX}{name}").into_bytes()
}

/// Per-array [`SchemaDoc`] registry.
///
/// Thread-safe via an internal [`Mutex`]. Multiple subsystems hold an
/// `Arc<SchemaRegistry<S>>` and call into it independently.
pub struct SchemaRegistry<S: StorageEngineSync> {
    storage: Arc<S>,
    docs: Mutex<HashMap<String, SchemaDoc>>,
    replica: Arc<ReplicaState>,
}

impl<S: StorageEngineSync> SchemaRegistry<S> {
    /// Create an empty registry (no cold-start scan).
    ///
    /// Use [`load`] to reconstruct persisted schemas on startup.
    pub fn new(storage: Arc<S>, replica: Arc<ReplicaState>) -> Self {
        Self {
            storage,
            docs: Mutex::new(HashMap::new()),
            replica,
        }
    }

    /// Load all persisted [`SchemaDoc`] snapshots from storage.
    ///
    /// Scans `Namespace::Meta` starting at `b"array.schema_doc:"` and stops
    /// at the first key that no longer shares that prefix.
    pub fn load(storage: Arc<S>, replica: Arc<ReplicaState>) -> Result<Self, LiteError> {
        let prefix = SCHEMA_KEY_PREFIX.as_bytes();
        let pairs = storage.scan_range_sync(Namespace::Meta, prefix, usize::MAX)?;

        let mut docs = HashMap::new();
        for (key, value) in pairs {
            if !key.starts_with(prefix) {
                break;
            }
            let name_bytes = &key[prefix.len()..];
            let name = std::str::from_utf8(name_bytes).map_err(|e| LiteError::Storage {
                detail: format!("schema_registry: non-UTF8 array name in storage: {e}"),
            })?;

            let persisted: PersistedSchema =
                zerompk::from_msgpack(&value).map_err(|e| LiteError::Storage {
                    detail: format!("schema_registry: decode PersistedSchema for '{name}': {e}"),
                })?;

            let hlc_arr: [u8; 18] =
                persisted
                    .schema_hlc_bytes
                    .try_into()
                    .map_err(|v: Vec<u8>| LiteError::Storage {
                        detail: format!(
                            "schema_registry: schema_hlc_bytes wrong length ({}) for '{name}'",
                            v.len()
                        ),
                    })?;
            let schema_hlc = Hlc::from_bytes(&hlc_arr);

            let mut doc = SchemaDoc::new(replica.replica_id());
            doc.import_snapshot(&persisted.loro_snapshot, schema_hlc, &replica.hlc_gen())
                .map_err(|e| LiteError::Storage {
                    detail: format!("schema_registry: loro import for '{name}': {e}"),
                })?;

            docs.insert(name.to_owned(), doc);
        }

        Ok(Self {
            storage,
            docs: Mutex::new(docs),
            replica,
        })
    }

    /// Register or replace the schema for `name`, minting a new [`Hlc`].
    ///
    /// Persists a Loro snapshot under the schema key so the registry
    /// survives restarts. Returns the freshly minted `schema_hlc`.
    pub fn put_schema(&self, name: &str, schema: &ArraySchema) -> Result<Hlc, LiteError> {
        let mut docs = self.docs.lock().map_err(|_| LiteError::LockPoisoned)?;

        let doc = if let Some(existing) = docs.get_mut(name) {
            existing
                .replace_schema(schema, &self.replica.hlc_gen())
                .map_err(|e| LiteError::Storage {
                    detail: format!("schema_registry put_schema '{name}': {e}"),
                })?;
            existing
        } else {
            let new_doc =
                SchemaDoc::from_schema(self.replica.replica_id(), schema, &self.replica.hlc_gen())
                    .map_err(|e| LiteError::Storage {
                        detail: format!("schema_registry from_schema '{name}': {e}"),
                    })?;
            docs.insert(name.to_owned(), new_doc);
            docs.get_mut(name).expect("just inserted")
        };

        let schema_hlc = doc.schema_hlc();
        let snapshot = doc.export_snapshot().map_err(|e| LiteError::Storage {
            detail: format!("schema_registry export '{name}': {e}"),
        })?;

        self.persist(name, schema_hlc, snapshot)?;
        Ok(schema_hlc)
    }

    /// Return the current `schema_hlc` for `name`, or `None` if unknown.
    pub fn schema_hlc(&self, name: &str) -> Option<Hlc> {
        let docs = self.docs.lock().ok()?;
        docs.get(name).map(|d| d.schema_hlc())
    }

    /// Apply a remote Loro snapshot for `name`.
    ///
    /// Creates the entry if absent. Persists the updated snapshot.
    /// Used by Phase E inbound to ingest schema sync messages.
    pub fn import_snapshot(
        &self,
        name: &str,
        snapshot_bytes: &[u8],
        remote_hlc: Hlc,
    ) -> Result<(), LiteError> {
        let mut docs = self.docs.lock().map_err(|_| LiteError::LockPoisoned)?;

        let doc = docs
            .entry(name.to_owned())
            .or_insert_with(|| SchemaDoc::new(self.replica.replica_id()));

        doc.import_snapshot(snapshot_bytes, remote_hlc, &self.replica.hlc_gen())
            .map_err(|e| LiteError::Storage {
                detail: format!("schema_registry import_snapshot '{name}': {e}"),
            })?;

        let schema_hlc = doc.schema_hlc();
        let snapshot = doc.export_snapshot().map_err(|e| LiteError::Storage {
            detail: format!("schema_registry export after import '{name}': {e}"),
        })?;
        drop(docs);

        self.persist(name, schema_hlc, snapshot)
    }

    /// Return all array names currently registered in this registry.
    pub fn list_arrays(&self) -> Vec<String> {
        self.docs
            .lock()
            .ok()
            .map(|docs| docs.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Export the current Loro snapshot bytes for `name`.
    ///
    /// Returns `None` if `name` is not registered.
    pub fn export_snapshot(&self, name: &str) -> Result<Option<Vec<u8>>, LiteError> {
        let docs = self.docs.lock().map_err(|_| LiteError::LockPoisoned)?;
        if let Some(doc) = docs.get(name) {
            let bytes = doc.export_snapshot().map_err(|e| LiteError::Storage {
                detail: format!("schema_registry export_snapshot '{name}': {e}"),
            })?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    fn persist(
        &self,
        name: &str,
        schema_hlc: Hlc,
        loro_snapshot: Vec<u8>,
    ) -> Result<(), LiteError> {
        let persisted = PersistedSchema {
            replica_id: self.replica.replica_id().as_u64(),
            schema_hlc_bytes: schema_hlc.to_bytes().to_vec(),
            loro_snapshot,
        };
        let bytes = zerompk::to_msgpack_vec(&persisted).map_err(|e| LiteError::Serialization {
            detail: format!("schema_registry persist '{name}': {e}"),
        })?;
        self.storage
            .put_sync(Namespace::Meta, &schema_key(name), &bytes)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use nodedb_array::schema::array_schema::ArraySchema;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::cell_order::{CellOrder, TileOrder};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::sync::replica_id::ReplicaId;
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn simple_schema(name: &str) -> ArraySchema {
        ArraySchema {
            name: name.into(),
            dims: vec![DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            )],
            attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
            tile_extents: vec![10],
            cell_order: CellOrder::RowMajor,
            tile_order: TileOrder::RowMajor,
        }
    }

    fn make_replica(storage: &Arc<RedbStorage>) -> Arc<ReplicaState> {
        Arc::new(ReplicaState::load_or_init(&**storage).unwrap())
    }

    fn make_registry(storage: Arc<RedbStorage>) -> SchemaRegistry<RedbStorage> {
        let replica = make_replica(&storage);
        SchemaRegistry::new(storage, replica)
    }

    #[test]
    fn put_schema_persists() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let reg = make_registry(Arc::clone(&storage));
        let hlc = reg.put_schema("arr", &simple_schema("arr")).unwrap();
        assert!(hlc > Hlc::ZERO);

        // Storage must have the key.
        let raw = storage
            .get_sync(Namespace::Meta, &schema_key("arr"))
            .unwrap();
        assert!(raw.is_some(), "schema must be persisted");
    }

    #[test]
    fn load_restores_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema_reg.redb");

        let schema_hlc;
        {
            let storage = Arc::new(RedbStorage::open(&path).unwrap());
            let replica = make_replica(&storage);
            let reg = SchemaRegistry::new(Arc::clone(&storage), Arc::clone(&replica));
            schema_hlc = reg.put_schema("arr", &simple_schema("arr")).unwrap();
        }

        {
            let storage = Arc::new(RedbStorage::open(&path).unwrap());
            let replica = Arc::new(ReplicaState::load_or_init(&*storage).unwrap());
            let reg = SchemaRegistry::load(Arc::clone(&storage), replica).unwrap();
            let loaded_hlc = reg.schema_hlc("arr").expect("arr must be loaded");
            // After import the HLC is bumped, so it must be >= the stored one.
            assert!(loaded_hlc >= schema_hlc);
        }
    }

    #[test]
    fn import_snapshot_creates_entry() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let replica = make_replica(&storage);

        // Create a schema on replica A.
        let reg_a = SchemaRegistry::new(Arc::clone(&storage), Arc::clone(&replica));
        reg_a.put_schema("x", &simple_schema("x")).unwrap();
        let snapshot = reg_a.export_snapshot("x").unwrap().unwrap();
        let remote_hlc = reg_a.schema_hlc("x").unwrap();

        // Import on registry B.
        let storage_b = Arc::new(RedbStorage::open_in_memory().unwrap());
        let replica_b = make_replica(&storage_b);
        let reg_b = SchemaRegistry::new(storage_b, replica_b);

        assert!(reg_b.schema_hlc("x").is_none(), "not yet present");
        reg_b.import_snapshot("x", &snapshot, remote_hlc).unwrap();
        assert!(reg_b.schema_hlc("x").is_some(), "must exist after import");
    }

    #[test]
    fn import_snapshot_advances_hlc() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let replica = make_replica(&storage);
        let reg = SchemaRegistry::new(Arc::clone(&storage), Arc::clone(&replica));

        // Seed a schema.
        reg.put_schema("y", &simple_schema("y")).unwrap();
        let hlc_before = reg.schema_hlc("y").unwrap();

        // Import a snapshot with a higher HLC.
        let snapshot = reg.export_snapshot("y").unwrap().unwrap();
        let future_hlc = Hlc::new(hlc_before.physical_ms + 50_000, 0, ReplicaId::new(99)).unwrap();
        reg.import_snapshot("y", &snapshot, future_hlc).unwrap();

        let hlc_after = reg.schema_hlc("y").unwrap();
        assert!(hlc_after > hlc_before, "HLC must advance on import");
    }

    #[test]
    fn export_returns_none_for_unknown() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let reg = make_registry(storage);
        let result = reg.export_snapshot("nonexistent").unwrap();
        assert!(result.is_none());
    }
}
