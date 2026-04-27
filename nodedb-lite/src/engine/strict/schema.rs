//! Schema management for strict collections: create, drop, alter, and accessors.
//!
//! All methods take `&self`. Internal state (`StrictEngine::collections`) is
//! protected by an `RwLock`. DDL methods read from storage with the lock
//! dropped, then briefly take the write lock to swap the map entry. The
//! upper layer is expected to serialize concurrent DDL on the same
//! collection.

use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::columnar::StrictSchema;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::engine::{
    CollectionState, META_STRICT_COLLECTIONS, META_STRICT_SCHEMA_PREFIX, StrictEngine,
};

impl<S: StorageEngine> StrictEngine<S> {
    /// Register a new strict collection with the given schema.
    pub async fn create_collection(
        &self,
        name: &str,
        schema: StrictSchema,
    ) -> Result<(), LiteError> {
        // Snapshot existing names + duplicate-check under the read lock.
        let mut names: Vec<String> = {
            let guard = self
                .collections
                .read()
                .map_err(|_| LiteError::LockPoisoned)?;
            if guard.contains_key(name) {
                return Err(LiteError::BadRequest {
                    detail: format!("strict collection '{name}' already exists"),
                });
            }
            guard.keys().cloned().collect()
        };
        names.push(name.to_string());

        // Persist schema + collection list to meta (lock dropped).
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        let schema_bytes =
            zerompk::to_msgpack_vec(&schema).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;
        let names_bytes =
            zerompk::to_msgpack_vec(&names).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        self.storage
            .batch_write(&[
                WriteOp::Put {
                    ns: Namespace::Meta,
                    key: meta_key.into_bytes(),
                    value: schema_bytes,
                },
                WriteOp::Put {
                    ns: Namespace::Meta,
                    key: META_STRICT_COLLECTIONS.to_vec(),
                    value: names_bytes,
                },
            ])
            .await?;

        // Insert into in-memory map.
        let new_state = Arc::new(CollectionState::new(schema));
        let mut guard = self
            .collections
            .write()
            .map_err(|_| LiteError::LockPoisoned)?;
        if guard.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "strict collection '{name}' was created concurrently by another writer"
                ),
            });
        }
        guard.insert(name.to_string(), new_state);
        Ok(())
    }

    /// Drop a strict collection and all its data.
    pub async fn drop_collection(&self, name: &str) -> Result<(), LiteError> {
        // Existence check under read lock.
        {
            let guard = self
                .collections
                .read()
                .map_err(|_| LiteError::LockPoisoned)?;
            if !guard.contains_key(name) {
                return Err(LiteError::BadRequest {
                    detail: format!("strict collection '{name}' does not exist"),
                });
            }
        }

        // Scan and delete all rows (lock dropped).
        let prefix = format!("{name}:");
        let rows = self
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        let mut ops: Vec<WriteOp> = rows
            .iter()
            .map(|(k, _)| WriteOp::Delete {
                ns: Namespace::Strict,
                key: k.clone(),
            })
            .collect();

        // Delete schema meta.
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        ops.push(WriteOp::Delete {
            ns: Namespace::Meta,
            key: meta_key.into_bytes(),
        });

        // Take the write lock briefly to remove and snapshot the new name list.
        let names: Vec<String> = {
            let mut guard = self
                .collections
                .write()
                .map_err(|_| LiteError::LockPoisoned)?;
            guard.remove(name);
            guard.keys().cloned().collect()
        };

        let names_bytes =
            zerompk::to_msgpack_vec(&names).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;
        ops.push(WriteOp::Put {
            ns: Namespace::Meta,
            key: META_STRICT_COLLECTIONS.to_vec(),
            value: names_bytes,
        });

        self.storage.batch_write(&ops).await?;
        Ok(())
    }

    /// Add a column to an existing strict collection.
    ///
    /// Bumps the schema version. Existing tuples in redb are NOT rewritten —
    /// the decoder checks `schema_version` in the tuple header and returns
    /// null/default for columns added after the tuple was written.
    pub async fn alter_add_column(
        &self,
        name: &str,
        column: nodedb_types::columnar::ColumnDef,
    ) -> Result<(), LiteError> {
        // Validate: new column must be nullable or have a default.
        if !column.nullable && column.default.is_none() {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                    column.name
                ),
            });
        }

        // Snapshot the current state under the read lock.
        let old_state = {
            let guard = self
                .collections
                .read()
                .map_err(|_| LiteError::LockPoisoned)?;
            guard.get(name).cloned().ok_or(LiteError::BadRequest {
                detail: format!("strict collection '{name}' does not exist"),
            })?
        };

        // Check for duplicate column name.
        if old_state
            .schema
            .columns
            .iter()
            .any(|c| c.name == column.name)
        {
            return Err(LiteError::BadRequest {
                detail: format!("column '{}' already exists in '{name}'", column.name),
            });
        }

        // Build the new schema.
        let mut new_schema = old_state.schema.clone();
        let old_version = new_schema.version;
        let old_col_count = new_schema.columns.len();
        new_schema.columns.push(column);
        new_schema.version = new_schema.version.saturating_add(1);

        // Build the new CollectionState with version_column_counts carrying
        // history forward.
        let mut new_state = CollectionState::new(new_schema.clone());
        for (v, c) in &old_state.version_column_counts {
            new_state.version_column_counts.insert(*v, *c);
        }
        new_state
            .version_column_counts
            .insert(old_version, old_col_count);
        new_state
            .version_column_counts
            .insert(new_schema.version, new_schema.columns.len());

        // Persist new schema (lock dropped).
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        let schema_bytes =
            zerompk::to_msgpack_vec(&new_schema).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        self.storage
            .put(Namespace::Meta, meta_key.as_bytes(), &schema_bytes)
            .await?;

        // Swap in the new state.
        let mut guard = self
            .collections
            .write()
            .map_err(|_| LiteError::LockPoisoned)?;
        guard.insert(name.to_string(), Arc::new(new_state));

        Ok(())
    }

    /// Get the schema for a collection (returns a clone).
    pub fn schema(&self, name: &str) -> Option<StrictSchema> {
        self.collections
            .read()
            .ok()
            .and_then(|g| g.get(name).map(|s| s.schema.clone()))
    }

    /// List all strict collection names.
    pub fn collection_names(&self) -> Vec<String> {
        self.collections
            .read()
            .map(|g| g.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Rewrite old-version tuples to the current schema version.
    ///
    /// Scans all tuples, finds those with `schema_version < current_version`,
    /// reads them with the old-version decoder, pads with null for new columns,
    /// and re-encodes with the current encoder. This eliminates the per-read
    /// version check overhead.
    ///
    /// Returns the number of tuples rewritten.
    pub async fn compact_tuples(&self, name: &str) -> Result<usize, LiteError> {
        let state = self.get_state(name)?;
        let current_version = state.schema.version;

        if current_version <= 1 {
            return Ok(0); // No schema evolution — nothing to compact.
        }

        let prefix = format!("{name}:");
        let entries = self
            .storage
            .scan_prefix(nodedb_types::Namespace::Strict, prefix.as_bytes())
            .await?;

        let mut rewritten = 0usize;

        for (key, tuple_bytes) in &entries {
            let tuple_version = state
                .decoder
                .schema_version(tuple_bytes)
                .unwrap_or(current_version);

            if tuple_version >= current_version {
                continue;
            }

            let old_col_count = state
                .version_column_counts
                .get(&tuple_version)
                .copied()
                .unwrap_or(state.schema.columns.len());

            let old_schema = nodedb_types::columnar::StrictSchema {
                columns: state.schema.columns[..old_col_count].to_vec(),
                version: tuple_version,
                dropped_columns: Vec::new(),
                bitemporal: state.schema.bitemporal,
            };
            let old_decoder = nodedb_strict::TupleDecoder::new(&old_schema);

            if let Ok(mut values) = old_decoder.extract_all(tuple_bytes) {
                values.resize(state.schema.columns.len(), nodedb_types::value::Value::Null);
                if let Ok(new_tuple) = state.encoder.encode(&values) {
                    self.storage
                        .put(nodedb_types::Namespace::Strict, key, &new_tuple)
                        .await?;
                    rewritten += 1;
                }
            }
        }

        Ok(rewritten)
    }
}
