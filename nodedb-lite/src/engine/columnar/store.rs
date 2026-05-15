//! Columnar engine for Lite: manages per-collection memtables, segments,
//! delete bitmaps, and PK indexes against the StorageEngine.
//!
//! Segments are stored in the `Columnar` namespace as:
//! - `{collection}:seg:{segment_id}` — segment bytes
//! - `{collection}:del:{segment_id}` — delete bitmap bytes
//! - `{collection}:meta` — segment metadata (list of segment IDs + row counts)
//!
//! Schemas are stored in the `Meta` namespace as `columnar_schema:{collection}`.
//!
//! All public methods take `&self`. The collection map lives behind an
//! `RwLock`; each collection's mutable state lives behind an inner
//! `std::sync::Mutex` that is only ever held briefly and never across `.await`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

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

/// Per-collection state. Wrapped in `Mutex` inside `ColumnarEngine`.
struct CollectionState {
    mutation: MutationEngine,
    profile: ColumnarProfile,
    /// Ordered list of flushed segments.
    segments: Vec<SegmentMeta>,
    /// Next segment ID to assign.
    next_segment_id: u32,
}

type CollectionMap = HashMap<String, Arc<Mutex<CollectionState>>>;

/// Manages all columnar collections for a NodeDbLite instance.
pub struct ColumnarEngine<S: StorageEngine> {
    storage: Arc<S>,
    collections: RwLock<CollectionMap>,
}

impl<S: StorageEngine> ColumnarEngine<S> {
    /// Create a new empty columnar engine.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            collections: RwLock::new(HashMap::new()),
        }
    }

    /// Restore columnar collections from storage on startup.
    pub async fn restore(storage: Arc<S>) -> Result<Self, LiteError> {
        let engine = Self::new(Arc::clone(&storage));

        let list_bytes = storage
            .get(Namespace::Meta, META_COLUMNAR_COLLECTIONS)
            .await?;
        let names: Vec<String> = match list_bytes {
            Some(bytes) => zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Storage {
                detail: format!("columnar collection list deserialization: {e}"),
            })?,
            None => Vec::new(),
        };

        let mut loaded: CollectionMap = HashMap::new();
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
                let seg_meta_key = format!("{name}:meta");
                let segments: Vec<SegmentMeta> = storage
                    .get(Namespace::Columnar, seg_meta_key.as_bytes())
                    .await?
                    .and_then(|b| zerompk::from_msgpack(&b).ok())
                    .unwrap_or_default();

                let next_id = segments.iter().map(|s| s.segment_id + 1).max().unwrap_or(1);

                let mut mutation = MutationEngine::new(name.clone(), stored.schema.clone());

                for seg_meta in &segments {
                    let seg_key = format!("{name}:seg:{}", seg_meta.segment_id);
                    if let Some(seg_bytes) =
                        storage.get(Namespace::Columnar, seg_key.as_bytes()).await?
                        && let Ok(reader) = SegmentReader::open(&seg_bytes)
                        && let Ok(pk_col) = reader.read_column(0)
                    {
                        rebuild_pk_from_column(&mut mutation, &pk_col, seg_meta.segment_id);
                    }

                    let del_key = format!("{name}:del:{}", seg_meta.segment_id);
                    if let Some(del_bytes) =
                        storage.get(Namespace::Columnar, del_key.as_bytes()).await?
                        && let Ok(bitmap) = DeleteBitmap::from_bytes(&del_bytes)
                    {
                        for row_idx in bitmap.iter() {
                            let _ = row_idx;
                        }
                    }
                }

                loaded.insert(
                    name,
                    Arc::new(Mutex::new(CollectionState {
                        mutation,
                        profile: stored.profile,
                        segments,
                        next_segment_id: next_id,
                    })),
                );
            }
        }

        *engine
            .collections
            .write()
            .map_err(|_| LiteError::LockPoisoned)? = loaded;

        Ok(engine)
    }

    // -- Internal helpers --

    fn lookup(&self, name: &str) -> Result<Arc<Mutex<CollectionState>>, LiteError> {
        let guard = self
            .collections
            .read()
            .map_err(|_| LiteError::LockPoisoned)?;
        guard.get(name).cloned().ok_or(LiteError::BadRequest {
            detail: format!("columnar collection '{name}' does not exist"),
        })
    }

    fn lock_state<'a>(
        state: &'a Arc<Mutex<CollectionState>>,
    ) -> Result<std::sync::MutexGuard<'a, CollectionState>, LiteError> {
        state.lock().map_err(|_| LiteError::LockPoisoned)
    }

    // -- Schema management --

    /// Create a new columnar collection.
    pub async fn create_collection(
        &self,
        name: &str,
        schema: ColumnarSchema,
        profile: ColumnarProfile,
    ) -> Result<(), LiteError> {
        // Snapshot existing names + dup check under read lock.
        let mut names: Vec<String> = {
            let guard = self
                .collections
                .read()
                .map_err(|_| LiteError::LockPoisoned)?;
            if guard.contains_key(name) {
                return Err(LiteError::BadRequest {
                    detail: format!("columnar collection '{name}' already exists"),
                });
            }
            guard.keys().cloned().collect()
        };
        names.push(name.to_string());

        // Persist schema + collection list (lock dropped).
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
        let state = CollectionState {
            mutation,
            profile,
            segments: Vec::new(),
            next_segment_id: 1,
        };

        let mut guard = self
            .collections
            .write()
            .map_err(|_| LiteError::LockPoisoned)?;
        if guard.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "columnar collection '{name}' was created concurrently by another writer"
                ),
            });
        }
        guard.insert(name.to_string(), Arc::new(Mutex::new(state)));

        Ok(())
    }

    /// Drop a columnar collection and all its data.
    pub async fn drop_collection(&self, name: &str) -> Result<(), LiteError> {
        // Snapshot state and remove from map under write lock.
        let (segments, remaining_names): (Vec<SegmentMeta>, Vec<String>) = {
            let mut guard = self
                .collections
                .write()
                .map_err(|_| LiteError::LockPoisoned)?;
            let state_arc = guard.remove(name).ok_or(LiteError::BadRequest {
                detail: format!("columnar collection '{name}' does not exist"),
            })?;
            let segments = {
                let s = state_arc.lock().map_err(|_| LiteError::LockPoisoned)?;
                s.segments.clone()
            };
            let names: Vec<String> = guard.keys().cloned().collect();
            (segments, names)
        };

        let mut ops = Vec::new();
        for seg in &segments {
            ops.push(WriteOp::Delete {
                ns: Namespace::Columnar,
                key: format!("{name}:seg:{}", seg.segment_id).into_bytes(),
            });
            ops.push(WriteOp::Delete {
                ns: Namespace::Columnar,
                key: format!("{name}:del:{}", seg.segment_id).into_bytes(),
            });
        }
        ops.push(WriteOp::Delete {
            ns: Namespace::Columnar,
            key: format!("{name}:meta").into_bytes(),
        });
        ops.push(WriteOp::Delete {
            ns: Namespace::Meta,
            key: format!("{META_COLUMNAR_SCHEMA_PREFIX}{name}").into_bytes(),
        });

        let names_bytes =
            zerompk::to_msgpack_vec(&remaining_names).map_err(|e| LiteError::Serialization {
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
    pub async fn alter_add_column(
        &self,
        name: &str,
        column: nodedb_types::columnar::ColumnDef,
    ) -> Result<(), LiteError> {
        if !column.nullable && column.default.is_none() {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                    column.name
                ),
            });
        }

        let state_arc = self.lookup(name)?;

        // Snapshot current schema + profile under inner lock.
        let (mut schema, profile) = {
            let s = Self::lock_state(&state_arc)?;
            if s.mutation
                .schema()
                .columns
                .iter()
                .any(|c| c.name == column.name)
            {
                return Err(LiteError::BadRequest {
                    detail: format!("column '{}' already exists in '{name}'", column.name),
                });
            }
            (s.mutation.schema().clone(), s.profile.clone())
        };

        schema.columns.push(column);
        schema.version = schema.version.saturating_add(1);

        // Persist updated schema (lock dropped).
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

        self.storage
            .put(Namespace::Meta, meta_key.as_bytes(), &schema_bytes)
            .await?;

        // Swap in the new MutationEngine.
        let mut s = Self::lock_state(&state_arc)?;
        s.mutation = MutationEngine::new(name.to_string(), schema);

        Ok(())
    }

    /// Get the schema for a collection (returns a clone).
    pub fn schema(&self, name: &str) -> Option<ColumnarSchema> {
        let guard = self.collections.read().ok()?;
        let state_arc = guard.get(name)?;
        let s = state_arc.lock().ok()?;
        Some(s.mutation.schema().clone())
    }

    /// Get the profile for a collection (returns a clone).
    pub fn profile(&self, name: &str) -> Option<ColumnarProfile> {
        let guard = self.collections.read().ok()?;
        let state_arc = guard.get(name)?;
        let s = state_arc.lock().ok()?;
        Some(s.profile.clone())
    }

    /// List all columnar collection names.
    pub fn collection_names(&self) -> Vec<String> {
        self.collections
            .read()
            .map(|g| g.keys().cloned().collect())
            .unwrap_or_default()
    }

    // -- Write path --

    /// Insert a row into a columnar collection's memtable.
    pub fn insert(&self, collection: &str, values: &[Value]) -> Result<(), LiteError> {
        let state_arc = self.lookup(collection)?;
        let mut s = Self::lock_state(&state_arc)?;
        s.mutation.insert(values).map_err(columnar_err_to_lite)?;
        Ok(())
    }

    /// Delete a row by PK.
    pub fn delete(&self, collection: &str, pk: &Value) -> Result<bool, LiteError> {
        let state_arc = self.lookup(collection)?;
        let mut s = Self::lock_state(&state_arc)?;

        if matches!(s.profile, ColumnarProfile::Timeseries { .. }) {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "DELETE not allowed on timeseries collection '{collection}' (append-only)"
                ),
            });
        }

        match s.mutation.delete(pk) {
            Ok(_) => Ok(true),
            Err(nodedb_columnar::ColumnarError::PrimaryKeyNotFound) => Ok(false),
            Err(e) => Err(columnar_err_to_lite(e)),
        }
    }

    /// Update a row: DELETE old + INSERT new.
    pub fn update(
        &self,
        collection: &str,
        old_pk: &Value,
        new_values: &[Value],
    ) -> Result<bool, LiteError> {
        let state_arc = self.lookup(collection)?;
        let mut s = Self::lock_state(&state_arc)?;

        if matches!(s.profile, ColumnarProfile::Timeseries { .. }) {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "UPDATE not allowed on timeseries collection '{collection}' (append-only)"
                ),
            });
        }

        match s.mutation.update(old_pk, new_values) {
            Ok(_) => Ok(true),
            Err(nodedb_columnar::ColumnarError::PrimaryKeyNotFound) => Ok(false),
            Err(e) => Err(columnar_err_to_lite(e)),
        }
    }

    /// Flush the memtable for a collection to a new segment.
    pub async fn flush_collection(&self, collection: &str) -> Result<(), LiteError> {
        let state_arc = self.lookup(collection)?;

        // Drain memtable + collect everything we need under the inner lock.
        struct FlushPayload {
            segment_id: u32,
            seg_key: String,
            segment_bytes: Vec<u8>,
            meta_key: String,
            meta_bytes: Vec<u8>,
            del_ops: Vec<(String, Vec<u8>)>,
        }

        let payload = {
            let mut s = Self::lock_state(&state_arc)?;
            if s.mutation.memtable().is_empty() {
                return Ok(());
            }

            let segment_id = s.next_segment_id;
            s.next_segment_id += 1;

            let (schema, columns, row_count) = s.mutation.memtable_mut().drain_optimized();

            let profile_tag = match &s.profile {
                ColumnarProfile::Plain => 0,
                ColumnarProfile::Timeseries { .. } => 1,
                ColumnarProfile::Spatial { .. } => 2,
            };

            let writer = SegmentWriter::new(profile_tag);
            let segment_bytes = writer
                .write_segment(&schema, &columns, row_count, None)
                .map_err(columnar_err_to_lite)?;

            let seg_key = format!("{collection}:seg:{segment_id}");
            s.segments.push(SegmentMeta {
                segment_id,
                row_count: row_count as u64,
            });
            let meta_key = format!("{collection}:meta");
            let meta_bytes =
                zerompk::to_msgpack_vec(&s.segments).map_err(|e| LiteError::Serialization {
                    detail: e.to_string(),
                })?;

            s.mutation
                .on_memtable_flushed(segment_id as u64)
                .map_err(|e| LiteError::Storage {
                    detail: format!("on_memtable_flushed: {e}"),
                })?;

            let mut del_ops: Vec<(String, Vec<u8>)> = Vec::new();
            for (&seg_id, bitmap) in s.mutation.delete_bitmaps() {
                if !bitmap.is_empty() {
                    let del_key = format!("{collection}:del:{seg_id}");
                    let del_bytes = bitmap.to_bytes().map_err(columnar_err_to_lite)?;
                    del_ops.push((del_key, del_bytes));
                }
            }

            FlushPayload {
                segment_id,
                seg_key,
                segment_bytes,
                meta_key,
                meta_bytes,
                del_ops,
            }
        };

        let _ = payload.segment_id;

        // Storage I/O with lock dropped.
        self.storage
            .put(
                Namespace::Columnar,
                payload.seg_key.as_bytes(),
                &payload.segment_bytes,
            )
            .await?;
        self.storage
            .put(
                Namespace::Columnar,
                payload.meta_key.as_bytes(),
                &payload.meta_bytes,
            )
            .await?;
        for (del_key, del_bytes) in &payload.del_ops {
            self.storage
                .put(Namespace::Columnar, del_key.as_bytes(), del_bytes)
                .await?;
        }

        Ok(())
    }

    /// Flush all collections' memtables.
    pub async fn flush_all(&self) -> Result<(), LiteError> {
        let names: Vec<String> = self
            .collections
            .read()
            .map_err(|_| LiteError::LockPoisoned)?
            .keys()
            .cloned()
            .collect();
        for name in names {
            self.flush_collection(&name).await?;
        }
        Ok(())
    }

    // -- Compaction --

    /// Check if any segments need compaction and run it.
    pub async fn try_compact_collection(&self, collection: &str) -> Result<bool, LiteError> {
        let state_arc = self.lookup(collection)?;

        // Snapshot the data we need, plus capture per-segment delete bitmaps.
        struct Snapshot {
            schema: ColumnarSchema,
            profile_tag: u8,
            to_compact: Vec<u32>,
            bitmaps: HashMap<u32, DeleteBitmap>,
        }

        let snap = {
            let s = Self::lock_state(&state_arc)?;
            let mut to_compact = Vec::new();
            for seg_meta in &s.segments {
                if let Some(bitmap) = s.mutation.delete_bitmap(seg_meta.segment_id as u64)
                    && bitmap.should_compact(seg_meta.row_count, 0.2)
                {
                    to_compact.push(seg_meta.segment_id);
                }
            }
            if to_compact.is_empty() {
                return Ok(false);
            }
            let schema = s.mutation.schema().clone();
            let profile_tag = match &s.profile {
                ColumnarProfile::Plain => 0,
                ColumnarProfile::Timeseries { .. } => 1,
                ColumnarProfile::Spatial { .. } => 2,
            };
            let mut bitmaps = HashMap::new();
            for &seg_id in &to_compact {
                if let Some(b) = s.mutation.delete_bitmap(seg_id as u64) {
                    bitmaps.insert(seg_id, b.clone());
                }
            }
            Snapshot {
                schema,
                profile_tag,
                to_compact,
                bitmaps,
            }
        };

        for seg_id in &snap.to_compact {
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
            let bitmap = snap.bitmaps.get(seg_id).unwrap_or(&empty_bitmap);

            let result = nodedb_columnar::compaction::compact_segment(
                &seg_bytes,
                bitmap,
                &snap.schema,
                snap.profile_tag,
                None,
                None,
            )
            .map_err(columnar_err_to_lite)?;

            if let Some(new_seg_bytes) = result.segment {
                self.storage
                    .put(Namespace::Columnar, seg_key.as_bytes(), &new_seg_bytes)
                    .await?;

                // Update row count under the inner lock (scoped so the guard
                // never crosses the `.delete` await below — clippy is strict).
                {
                    let mut s = Self::lock_state(&state_arc)?;
                    if let Some(meta) = s.segments.iter_mut().find(|m| m.segment_id == *seg_id) {
                        meta.row_count = result.live_rows as u64;
                    }
                }

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

                {
                    let mut s = Self::lock_state(&state_arc)?;
                    s.segments.retain(|m| m.segment_id != *seg_id);
                }
            }
        }

        // Persist updated metadata.
        let meta_bytes = {
            let s = Self::lock_state(&state_arc)?;
            zerompk::to_msgpack_vec(&s.segments).map_err(|e| LiteError::Serialization {
                detail: e.to_string(),
            })?
        };
        let meta_key = format!("{collection}:meta");
        self.storage
            .put(Namespace::Columnar, meta_key.as_bytes(), &meta_bytes)
            .await?;

        Ok(true)
    }

    // -- Read path --

    /// Read all segment bytes for a collection (for the table provider).
    pub async fn read_segments(&self, collection: &str) -> Result<Vec<(u32, Vec<u8>)>, LiteError> {
        let state_arc = self.lookup(collection)?;
        let seg_metas: Vec<SegmentMeta> = {
            let s = Self::lock_state(&state_arc)?;
            s.segments.clone()
        };

        let mut segments = Vec::with_capacity(seg_metas.len());
        for seg_meta in &seg_metas {
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

    /// Get the delete bitmap for a segment (returns a clone).
    pub fn delete_bitmap(&self, collection: &str, segment_id: u32) -> Option<DeleteBitmap> {
        let guard = self.collections.read().ok()?;
        let state_arc = guard.get(collection)?;
        let s = state_arc.lock().ok()?;
        s.mutation.delete_bitmap(segment_id as u64).cloned()
    }

    /// Row count across all segments + memtable for a collection.
    pub fn row_count(&self, collection: &str) -> usize {
        let Ok(guard) = self.collections.read() else {
            return 0;
        };
        let Some(state_arc) = guard.get(collection) else {
            return 0;
        };
        let Ok(s) = state_arc.lock() else {
            return 0;
        };
        let seg_rows: u64 = s.segments.iter().map(|m| m.row_count).sum();
        seg_rows as usize + s.mutation.memtable().row_count()
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
                            segment_id: segment_id as u64,
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
                            segment_id: segment_id as u64,
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
