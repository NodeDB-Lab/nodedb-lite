//! Schema management for strict collections: create, drop, alter, and accessors.

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
        &mut self,
        name: &str,
        schema: StrictSchema,
    ) -> Result<(), LiteError> {
        if self.collections.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("strict collection '{name}' already exists"),
            });
        }

        // Persist schema to meta.
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        let schema_bytes =
            zerompk::to_msgpack_vec(&schema).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        // Update collection list.
        let mut names: Vec<String> = self.collections.keys().cloned().collect();
        names.push(name.to_string());
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

        self.collections
            .insert(name.to_string(), CollectionState::new(schema));
        Ok(())
    }

    /// Drop a strict collection and all its data.
    pub async fn drop_collection(&mut self, name: &str) -> Result<(), LiteError> {
        if !self.collections.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("strict collection '{name}' does not exist"),
            });
        }

        // Scan and delete all rows.
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

        // Update collection list.
        self.collections.remove(name);
        let names: Vec<String> = self.collections.keys().cloned().collect();
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
        &mut self,
        name: &str,
        column: nodedb_types::columnar::ColumnDef,
    ) -> Result<(), LiteError> {
        let state = self
            .collections
            .get_mut(name)
            .ok_or(LiteError::BadRequest {
                detail: format!("strict collection '{name}' does not exist"),
            })?;

        // Validate: new column must be nullable or have a default.
        // Non-nullable columns without a default would break existing tuples.
        if !column.nullable && column.default.is_none() {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                    column.name
                ),
            });
        }

        // Check for duplicate column name.
        if state.schema.columns.iter().any(|c| c.name == column.name) {
            return Err(LiteError::BadRequest {
                detail: format!("column '{}' already exists in '{name}'", column.name),
            });
        }

        // Record old version's column count before bumping.
        let old_version = state.schema.version;
        let old_col_count = state.schema.columns.len();
        state
            .version_column_counts
            .insert(old_version, old_col_count);

        // Append column and bump version.
        state.schema.columns.push(column);
        state.schema.version = state.schema.version.saturating_add(1);
        state
            .version_column_counts
            .insert(state.schema.version, state.schema.columns.len());

        // Rebuild encoder/decoder with new schema.
        state.encoder = nodedb_strict::TupleEncoder::new(&state.schema);
        state.decoder = nodedb_strict::TupleDecoder::new(&state.schema);

        // Persist updated schema.
        let meta_key = format!("{META_STRICT_SCHEMA_PREFIX}{name}");
        let schema_bytes =
            zerompk::to_msgpack_vec(&state.schema).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        self.storage
            .put(Namespace::Meta, meta_key.as_bytes(), &schema_bytes)
            .await?;

        Ok(())
    }

    /// Get the schema for a collection.
    pub fn schema(&self, name: &str) -> Option<&StrictSchema> {
        self.collections.get(name).map(|c| &c.schema)
    }

    /// List all strict collection names.
    pub fn collection_names(&self) -> Vec<&str> {
        self.collections.keys().map(|s| s.as_str()).collect()
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
                continue; // Already current version.
            }

            // Decode with old schema, pad, re-encode with current schema.
            let old_col_count = state
                .version_column_counts
                .get(&tuple_version)
                .copied()
                .unwrap_or(state.schema.columns.len());

            let old_schema = nodedb_types::columnar::StrictSchema {
                columns: state.schema.columns[..old_col_count].to_vec(),
                version: tuple_version,
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
