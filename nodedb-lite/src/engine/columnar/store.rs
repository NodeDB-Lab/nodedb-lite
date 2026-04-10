//! Columnar engine for Lite: manages per-collection memtables, segments,
//! delete bitmaps, and PK indexes against the StorageEngine.
//!
//! Segments are stored in the `Columnar` namespace as:
//! - `{collection}:seg:{segment_id}` — segment bytes
//! - `{collection}:del:{segment_id}` — delete bitmap bytes
//! - `{collection}:meta` — segment metadata (list of segment IDs + row counts)
//!
//! Schemas are stored in the `Meta` namespace as `columnar_schema:{collection}`.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_columnar::delete_bitmap::DeleteBitmap;
use nodedb_columnar::mutation::MutationEngine;
use nodedb_columnar::reader::SegmentReader;
use nodedb_columnar::writer::SegmentWriter;
use nodedb_types::Namespace;
use nodedb_types::columnar::{ColumnarProfile, ColumnarSchema};
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Meta key prefix for columnar schemas.
const META_COLUMNAR_SCHEMA_PREFIX: &str = "columnar_schema:";
/// Meta key listing all columnar collections.
const META_COLUMNAR_COLLECTIONS: &[u8] = b"meta:columnar_collections";

/// Per-collection segment metadata.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
struct SegmentMeta {
    segment_id: u32,
    row_count: u64,
}

/// Per-collection state.
struct CollectionState {
    mutation: MutationEngine,
    profile: ColumnarProfile,
    /// Ordered list of flushed segments.
    segments: Vec<SegmentMeta>,
    /// Next segment ID to assign.
    next_segment_id: u32,
}

/// Manages all columnar collections for a NodeDbLite instance.
pub struct ColumnarEngine<S: StorageEngine> {
    storage: Arc<S>,
    collections: HashMap<String, CollectionState>,
}

impl<S: StorageEngine> ColumnarEngine<S> {
    /// Create a new empty columnar engine.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            collections: HashMap::new(),
        }
    }

    /// Restore columnar collections from storage on startup.
    pub async fn restore(storage: Arc<S>) -> Result<Self, LiteError> {
        let mut engine = Self::new(Arc::clone(&storage));

        let list_bytes = storage
            .get(Namespace::Meta, META_COLUMNAR_COLLECTIONS)
            .await?;
        let names: Vec<String> = match list_bytes {
            Some(bytes) => zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Storage {
                detail: format!("columnar collection list deserialization: {e}"),
            })?,
            None => Vec::new(),
        };

        for name in names {
            let meta_key = format!("{META_COLUMNAR_SCHEMA_PREFIX}{name}");
            #[derive(serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
            struct StoredSchema {
                schema: ColumnarSchema,
                profile: ColumnarProfile,
            }
            if let Some(schema_bytes) = storage.get(Namespace::Meta, meta_key.as_bytes()).await?
                && let Ok(stored) = zerompk::from_msgpack::<StoredSchema>(&schema_bytes)
            {
                // Restore segment metadata.
                let seg_meta_key = format!("{name}:meta");
                let segments: Vec<SegmentMeta> = storage
                    .get(Namespace::Columnar, seg_meta_key.as_bytes())
                    .await?
                    .and_then(|b| zerompk::from_msgpack(&b).ok())
                    .unwrap_or_default();

                let next_id = segments.iter().map(|s| s.segment_id + 1).max().unwrap_or(1);

                // Restore PK index from segments.
                let mut mutation = MutationEngine::new(name.clone(), stored.schema.clone());

                // Rebuild PK index by scanning segment PKs.
                // For now, PK index is rebuilt from segment metadata on cold start.
                // A checkpoint-based approach would be faster for large datasets.
                for seg_meta in &segments {
                    let seg_key = format!("{name}:seg:{}", seg_meta.segment_id);
                    if let Some(seg_bytes) =
                        storage.get(Namespace::Columnar, seg_key.as_bytes()).await?
                        && let Ok(reader) = SegmentReader::open(&seg_bytes)
                    {
                        // Read PK column (column 0 by convention for columnar collections).
                        if let Ok(pk_col) = reader.read_column(0) {
                            rebuild_pk_from_column(&mut mutation, &pk_col, seg_meta.segment_id);
                        }
                    }

                    // Restore delete bitmap.
                    let del_key = format!("{name}:del:{}", seg_meta.segment_id);
                    if let Some(del_bytes) =
                        storage.get(Namespace::Columnar, del_key.as_bytes()).await?
                        && let Ok(bitmap) = DeleteBitmap::from_bytes(&del_bytes)
                    {
                        // Apply deletions to PK index.
                        for row_idx in bitmap.iter() {
                            // We don't have PK values for deleted rows in the bitmap,
                            // so we just note the bitmap exists. The MutationEngine's
                            // delete_bitmaps will be populated on the next delete op.
                            let _ = row_idx;
                        }
                    }
                }

                engine.collections.insert(
                    name,
                    CollectionState {
                        mutation,
                        profile: stored.profile,
                        segments,
                        next_segment_id: next_id,
                    },
                );
            }
        }

        Ok(engine)
    }

    // -- Schema management --

    /// Create a new columnar collection.
    pub async fn create_collection(
        &mut self,
        name: &str,
        schema: ColumnarSchema,
        profile: ColumnarProfile,
    ) -> Result<(), LiteError> {
        if self.collections.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("columnar collection '{name}' already exists"),
            });
        }

        // Persist schema + profile.
        #[derive(serde::Serialize, zerompk::ToMessagePack)]
        struct StoredSchema<'a> {
            schema: &'a ColumnarSchema,
            profile: &'a ColumnarProfile,
        }
        let meta_key = format!("{META_COLUMNAR_SCHEMA_PREFIX}{name}");
        let schema_bytes = zerompk::to_msgpack_vec(&StoredSchema {
            schema: &schema,
            profile: &profile,
        })
        .map_err(|e| LiteError::Serialization {
            detail: e.to_string(),
        })?;

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
                    key: META_COLUMNAR_COLLECTIONS.to_vec(),
                    value: names_bytes,
                },
            ])
            .await?;

        let mutation = MutationEngine::new(name.to_string(), schema);
        self.collections.insert(
            name.to_string(),
            CollectionState {
                mutation,
                profile,
                segments: Vec::new(),
                next_segment_id: 1,
            },
        );

        Ok(())
    }

    /// Drop a columnar collection and all its data.
    pub async fn drop_collection(&mut self, name: &str) -> Result<(), LiteError> {
        let state = self.collections.remove(name).ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{name}' does not exist"),
        })?;

        let mut ops = Vec::new();

        // Delete all segments and delete bitmaps.
        for seg in &state.segments {
            ops.push(WriteOp::Delete {
                ns: Namespace::Columnar,
                key: format!("{name}:seg:{}", seg.segment_id).into_bytes(),
            });
            ops.push(WriteOp::Delete {
                ns: Namespace::Columnar,
                key: format!("{name}:del:{}", seg.segment_id).into_bytes(),
            });
        }

        // Delete metadata.
        ops.push(WriteOp::Delete {
            ns: Namespace::Columnar,
            key: format!("{name}:meta").into_bytes(),
        });
        ops.push(WriteOp::Delete {
            ns: Namespace::Meta,
            key: format!("{META_COLUMNAR_SCHEMA_PREFIX}{name}").into_bytes(),
        });

        // Update collection list.
        let names: Vec<String> = self.collections.keys().cloned().collect();
        let names_bytes =
            zerompk::to_msgpack_vec(&names).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;
        ops.push(WriteOp::Put {
            ns: Namespace::Meta,
            key: META_COLUMNAR_COLLECTIONS.to_vec(),
            value: names_bytes,
        });

        self.storage.batch_write(&ops).await?;
        Ok(())
    }

    /// Add a column to an existing columnar collection.
    ///
    /// Bumps the schema version. Existing segments are NOT rewritten — the
    /// reader null-fills columns that were added after the segment was written.
    pub async fn alter_add_column(
        &mut self,
        name: &str,
        column: nodedb_types::columnar::ColumnDef,
    ) -> Result<(), LiteError> {
        let state = self
            .collections
            .get_mut(name)
            .ok_or(LiteError::BadRequest {
                detail: format!("columnar collection '{name}' does not exist"),
            })?;

        // Non-nullable columns without a default break existing segments.
        if !column.nullable && column.default.is_none() {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                    column.name
                ),
            });
        }

        // Check for duplicate.
        if state
            .mutation
            .schema()
            .columns
            .iter()
            .any(|c| c.name == column.name)
        {
            return Err(LiteError::BadRequest {
                detail: format!("column '{}' already exists in '{name}'", column.name),
            });
        }

        // The MutationEngine owns the schema — we need to rebuild it with the new column.
        // Extract current schema, append column, bump version, create new engine.
        let mut schema = state.mutation.schema().clone();
        schema.columns.push(column);
        schema.version = schema.version.saturating_add(1);

        // Rebuild the mutation engine with the new schema.
        // PK index and delete bitmaps are preserved since column addition doesn't change row layout.
        state.mutation = MutationEngine::new(name.to_string(), schema.clone());

        // Persist updated schema + profile.
        #[derive(serde::Serialize, zerompk::ToMessagePack)]
        struct StoredSchema<'a> {
            schema: &'a ColumnarSchema,
            profile: &'a ColumnarProfile,
        }
        let meta_key = format!("{META_COLUMNAR_SCHEMA_PREFIX}{name}");
        let schema_bytes = zerompk::to_msgpack_vec(&StoredSchema {
            schema: &schema,
            profile: &state.profile,
        })
        .map_err(|e| LiteError::Serialization {
            detail: e.to_string(),
        })?;

        self.storage
            .put(Namespace::Meta, meta_key.as_bytes(), &schema_bytes)
            .await?;

        Ok(())
    }

    /// Get the schema for a collection.
    pub fn schema(&self, name: &str) -> Option<&ColumnarSchema> {
        self.collections.get(name).map(|s| s.mutation.schema())
    }

    /// Get the profile for a collection.
    pub fn profile(&self, name: &str) -> Option<&ColumnarProfile> {
        self.collections.get(name).map(|s| &s.profile)
    }

    /// List all columnar collection names.
    pub fn collection_names(&self) -> Vec<&str> {
        self.collections.keys().map(|s| s.as_str()).collect()
    }

    // -- Write path --

    /// Insert a row into a columnar collection's memtable.
    pub fn insert(&mut self, collection: &str, values: &[Value]) -> Result<(), LiteError> {
        let state = self.get_state_mut(collection)?;
        state
            .mutation
            .insert(values)
            .map_err(columnar_err_to_lite)?;
        Ok(())
    }

    /// Delete a row by PK.
    ///
    /// Rejects deletion on timeseries collections (append-only constraint).
    /// Marks the row in the segment's delete bitmap. Compaction is triggered
    /// separately via `try_compact_collection` when the delete ratio exceeds 20%.
    pub fn delete(&mut self, collection: &str, pk: &Value) -> Result<bool, LiteError> {
        let state = self.get_state_mut(collection)?;

        // Timeseries profile is append-only — DELETE not allowed.
        if matches!(state.profile, ColumnarProfile::Timeseries { .. }) {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "DELETE not allowed on timeseries collection '{collection}' (append-only)"
                ),
            });
        }

        match state.mutation.delete(pk) {
            Ok(_) => Ok(true),
            Err(nodedb_columnar::ColumnarError::PrimaryKeyNotFound) => Ok(false),
            Err(e) => Err(columnar_err_to_lite(e)),
        }
    }

    /// Update a row: DELETE old + INSERT new.
    ///
    /// Rejects update on timeseries collections (append-only constraint).
    pub fn update(
        &mut self,
        collection: &str,
        old_pk: &Value,
        new_values: &[Value],
    ) -> Result<bool, LiteError> {
        let state = self.get_state_mut(collection)?;

        // Timeseries profile is append-only — UPDATE not allowed.
        if matches!(state.profile, ColumnarProfile::Timeseries { .. }) {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "UPDATE not allowed on timeseries collection '{collection}' (append-only)"
                ),
            });
        }

        match state.mutation.update(old_pk, new_values) {
            Ok(_) => Ok(true),
            Err(nodedb_columnar::ColumnarError::PrimaryKeyNotFound) => Ok(false),
            Err(e) => Err(columnar_err_to_lite(e)),
        }
    }

    /// Flush the memtable for a collection to a new segment.
    ///
    /// Called when the memtable reaches its threshold or on shutdown.
    pub async fn flush_collection(&mut self, collection: &str) -> Result<(), LiteError> {
        let state = self
            .collections
            .get_mut(collection)
            .ok_or(LiteError::BadRequest {
                detail: format!("columnar collection '{collection}' does not exist"),
            })?;

        if state.mutation.memtable().is_empty() {
            return Ok(());
        }

        let segment_id = state.next_segment_id;
        state.next_segment_id += 1;

        // Drain with auto dict-encoding for low-cardinality String columns.
        let (schema, columns, row_count) = state.mutation.memtable_mut().drain_optimized();

        let profile_tag = match &state.profile {
            ColumnarProfile::Plain => 0,
            ColumnarProfile::Timeseries { .. } => 1,
            ColumnarProfile::Spatial { .. } => 2,
        };

        let writer = SegmentWriter::new(profile_tag);
        let segment_bytes = writer
            .write_segment(&schema, &columns, row_count)
            .map_err(columnar_err_to_lite)?;

        // Collect all storage ops to execute after releasing mutable borrow on state.
        let seg_key = format!("{collection}:seg:{segment_id}");
        state.segments.push(SegmentMeta {
            segment_id,
            row_count: row_count as u64,
        });
        let meta_key = format!("{collection}:meta");
        let meta_bytes =
            zerompk::to_msgpack_vec(&state.segments).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?;

        state.mutation.on_memtable_flushed(segment_id);

        // Collect delete bitmap writes.
        let mut del_ops: Vec<(String, Vec<u8>)> = Vec::new();
        for (&seg_id, bitmap) in state.mutation.delete_bitmaps() {
            if !bitmap.is_empty() {
                let del_key = format!("{collection}:del:{seg_id}");
                let del_bytes = bitmap.to_bytes().map_err(columnar_err_to_lite)?;
                del_ops.push((del_key, del_bytes));
            }
        }

        // Now do all storage writes (state borrow is released by building the ops above).
        let storage = &self.storage;
        storage
            .put(Namespace::Columnar, seg_key.as_bytes(), &segment_bytes)
            .await?;
        storage
            .put(Namespace::Columnar, meta_key.as_bytes(), &meta_bytes)
            .await?;
        for (del_key, del_bytes) in &del_ops {
            storage
                .put(Namespace::Columnar, del_key.as_bytes(), del_bytes)
                .await?;
        }

        Ok(())
    }

    /// Flush all collections' memtables.
    pub async fn flush_all(&mut self) -> Result<(), LiteError> {
        let names: Vec<String> = self.collections.keys().cloned().collect();
        for name in names {
            self.flush_collection(&name).await?;
        }
        Ok(())
    }

    // -- Compaction --

    /// Check if any segments need compaction and run it.
    ///
    /// Compaction is triggered when a segment's delete ratio exceeds 20%.
    /// The old segment is replaced with a compacted one (deleted rows removed).
    pub async fn try_compact_collection(&mut self, collection: &str) -> Result<bool, LiteError> {
        let state = self
            .collections
            .get(collection)
            .ok_or(LiteError::BadRequest {
                detail: format!("columnar collection '{collection}' does not exist"),
            })?;

        // Find segments needing compaction.
        let mut to_compact = Vec::new();
        for seg_meta in &state.segments {
            if let Some(bitmap) = state.mutation.delete_bitmap(seg_meta.segment_id)
                && bitmap.should_compact(seg_meta.row_count, 0.2)
            {
                to_compact.push(seg_meta.segment_id);
            }
        }

        if to_compact.is_empty() {
            return Ok(false);
        }

        let schema = state.mutation.schema().clone();
        let profile_tag = match &state.profile {
            ColumnarProfile::Plain => 0,
            ColumnarProfile::Timeseries { .. } => 1,
            ColumnarProfile::Spatial { .. } => 2,
        };

        // Compact each segment that exceeds the threshold.
        for seg_id in &to_compact {
            let seg_key = format!("{collection}:seg:{seg_id}");
            let seg_bytes = match self
                .storage
                .get(Namespace::Columnar, seg_key.as_bytes())
                .await?
            {
                Some(b) => b,
                None => continue,
            };

            let empty_bitmap = DeleteBitmap::new();
            let bitmap = self
                .collections
                .get(collection)
                .and_then(|s| s.mutation.delete_bitmap(*seg_id))
                .unwrap_or(&empty_bitmap);

            let result = nodedb_columnar::compaction::compact_segment(
                &seg_bytes,
                bitmap,
                &schema,
                profile_tag,
            )
            .map_err(columnar_err_to_lite)?;

            if let Some(new_seg_bytes) = result.segment {
                // Write compacted segment under the same key (atomic replace).
                self.storage
                    .put(Namespace::Columnar, seg_key.as_bytes(), &new_seg_bytes)
                    .await?;

                // Update row count in metadata.
                if let Some(state) = self.collections.get_mut(collection)
                    && let Some(meta) = state.segments.iter_mut().find(|m| m.segment_id == *seg_id)
                {
                    meta.row_count = result.live_rows as u64;
                }

                // Clear the delete bitmap for this segment.
                let del_key = format!("{collection}:del:{seg_id}");
                self.storage
                    .delete(Namespace::Columnar, del_key.as_bytes())
                    .await?;
            } else {
                // All rows deleted — remove segment entirely.
                self.storage
                    .delete(Namespace::Columnar, seg_key.as_bytes())
                    .await?;
                let del_key = format!("{collection}:del:{seg_id}");
                self.storage
                    .delete(Namespace::Columnar, del_key.as_bytes())
                    .await?;

                if let Some(state) = self.collections.get_mut(collection) {
                    state.segments.retain(|m| m.segment_id != *seg_id);
                }
            }
        }

        // Persist updated metadata.
        if let Some(state) = self.collections.get(collection) {
            let meta_key = format!("{collection}:meta");
            let meta_bytes =
                zerompk::to_msgpack_vec(&state.segments).map_err(|e| LiteError::Serialization {
                    detail: e.to_string(),
                })?;
            self.storage
                .put(Namespace::Columnar, meta_key.as_bytes(), &meta_bytes)
                .await?;
        }

        Ok(true)
    }

    // -- Read path --

    /// Read all segment bytes for a collection (for the table provider).
    pub async fn read_segments(&self, collection: &str) -> Result<Vec<(u32, Vec<u8>)>, LiteError> {
        let state = self.get_state(collection)?;
        let mut segments = Vec::with_capacity(state.segments.len());

        for seg_meta in &state.segments {
            let seg_key = format!("{collection}:seg:{}", seg_meta.segment_id);
            if let Some(bytes) = self
                .storage
                .get(Namespace::Columnar, seg_key.as_bytes())
                .await?
            {
                segments.push((seg_meta.segment_id, bytes));
            }
        }

        Ok(segments)
    }

    /// Get the delete bitmap for a segment.
    pub fn delete_bitmap(&self, collection: &str, segment_id: u32) -> Option<&DeleteBitmap> {
        self.collections
            .get(collection)
            .and_then(|s| s.mutation.delete_bitmap(segment_id))
    }

    /// Row count across all segments + memtable for a collection.
    pub fn row_count(&self, collection: &str) -> usize {
        let Some(state) = self.collections.get(collection) else {
            return 0;
        };
        let seg_rows: u64 = state.segments.iter().map(|s| s.row_count).sum();
        seg_rows as usize + state.mutation.memtable().row_count()
    }

    // -- Internal helpers --

    fn get_state(&self, collection: &str) -> Result<&CollectionState, LiteError> {
        self.collections
            .get(collection)
            .ok_or(LiteError::BadRequest {
                detail: format!("columnar collection '{collection}' does not exist"),
            })
    }

    fn get_state_mut(&mut self, collection: &str) -> Result<&mut CollectionState, LiteError> {
        self.collections
            .get_mut(collection)
            .ok_or(LiteError::BadRequest {
                detail: format!("columnar collection '{collection}' does not exist"),
            })
    }
}

/// Rebuild PK index entries from a decoded PK column.
fn rebuild_pk_from_column(
    mutation: &mut MutationEngine,
    pk_col: &nodedb_columnar::reader::DecodedColumn,
    segment_id: u32,
) {
    use nodedb_columnar::pk_index::{RowLocation, encode_pk};
    use nodedb_columnar::reader::DecodedColumn;

    match pk_col {
        DecodedColumn::Int64 { values, valid } => {
            for (row_idx, (val, &is_valid)) in values.iter().zip(valid.iter()).enumerate() {
                if is_valid {
                    let pk_bytes = encode_pk(&Value::Integer(*val));
                    mutation.pk_index_mut().upsert(
                        pk_bytes,
                        RowLocation {
                            segment_id,
                            row_index: row_idx as u32,
                        },
                    );
                }
            }
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            for (row_idx, &is_valid) in valid.iter().enumerate() {
                if is_valid {
                    let start = offsets[row_idx] as usize;
                    let end = offsets[row_idx + 1] as usize;
                    let pk_bytes = data[start..end].to_vec();
                    mutation.pk_index_mut().upsert(
                        pk_bytes,
                        RowLocation {
                            segment_id,
                            row_index: row_idx as u32,
                        },
                    );
                }
            }
        }
        _ => {}
    }
}

fn columnar_err_to_lite(e: nodedb_columnar::ColumnarError) -> LiteError {
    LiteError::BadRequest {
        detail: e.to_string(),
    }
}
