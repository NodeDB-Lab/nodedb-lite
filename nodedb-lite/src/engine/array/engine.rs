//! `ArrayEngineState` — the embedded ND sparse array engine for NodeDB-Lite.
//!
//! Separates in-memory state from storage access. The state struct is not
//! generic over the storage backend; instead, callers pass `&Arc<S>` to each
//! operation that requires storage I/O. This allows `ArrayEngineState` to be
//! stored in a `tokio::sync::Mutex` inside `NodeDbLite<S: StorageEngine>`
//! without requiring any synchronous storage trait.
//!
//! The read paths (`read_coord`, `slice`, `surrogate_bitmap_scan`) live in
//! `engine::array::read`. Surrogate binding lives in `engine::array::surrogates`.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::coord::value::CoordValue;

use crate::engine::array::catalog::{ArrayCatalog, ArrayCatalogEntry, hash_schema};
use crate::engine::array::manifest::{ArrayManifest, SegmentRef, load_manifest, save_manifest};
use crate::engine::array::memtable::{ArrayMemtable, PutCellArgs, encode_coord};
use crate::engine::array::segments::write_segment;
use crate::engine::array::surrogates::{
    CellBindings, FIRST_SURROGATE, hwm_op, load_hwm, next_after_load,
};
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

const DEFAULT_FLUSH_THRESHOLD: usize = 4096;

/// Per-array runtime state.
pub(crate) struct ArrayState {
    pub schema: ArraySchema,
    pub schema_hash: u64,
    pub manifest: ArrayManifest,
    pub memtable: ArrayMemtable,
    pub audit_retain_ms: Option<i64>,
    /// Coordinate-to-surrogate bindings of this array.
    pub(crate) bindings: CellBindings,
}

/// One cell write passed to `ArrayEngineState::put_cell`.
pub struct CellWrite {
    pub coord: Vec<CoordValue>,
    pub attrs: Vec<nodedb_array::types::cell_value::value::CellValue>,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

/// Storage-agnostic in-memory state for the array engine.
///
/// All operations that touch persistent storage take an explicit `storage`
/// parameter so this struct can be stored in `tokio::sync::Mutex<ArrayEngineState>`
/// inside `NodeDbLite<S: StorageEngine>` without requiring any synchronous storage
/// trait bound.
pub struct ArrayEngineState {
    pub(crate) arrays: HashMap<String, ArrayState>,
    flush_threshold: usize,
    /// Next free surrogate. One counter covers every array, so a surrogate
    /// names one cell across the database.
    next_surrogate: u32,
}

impl Default for ArrayEngineState {
    fn default() -> Self {
        Self {
            arrays: HashMap::new(),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            next_surrogate: FIRST_SURROGATE,
        }
    }
}

impl ArrayEngineState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restore array engine from persistent storage.
    ///
    /// The next free surrogate passes both the persisted high-water mark and
    /// every loaded binding, so a reopen never reuses a surrogate.
    pub async fn open<S: StorageEngine>(storage: &Arc<S>) -> Result<Self, LiteError> {
        let catalog = ArrayCatalog::open(Arc::clone(storage)).await?;
        let mut arrays = HashMap::new();
        let mut max_bound = 0u32;

        for name in catalog.names().map(|s| s.to_owned()).collect::<Vec<_>>() {
            let entry = catalog
                .get(&name)
                .ok_or_else(|| LiteError::Storage {
                    detail: format!("array catalog entry '{name}' missing after iteration"),
                })?
                .clone();
            let schema = entry.schema()?;
            let manifest = load_manifest(storage, &name).await?;
            let bindings = CellBindings::load(storage, &name).await?;
            max_bound = max_bound.max(bindings.max_bound());
            arrays.insert(
                name,
                ArrayState {
                    schema,
                    schema_hash: entry.schema_hash,
                    manifest,
                    memtable: ArrayMemtable::new(),
                    audit_retain_ms: entry.audit_retain_ms,
                    bindings,
                },
            );
        }

        let next_surrogate = next_after_load(load_hwm(storage).await?, max_bound)?;
        Ok(Self {
            arrays,
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            next_surrogate,
        })
    }

    /// Create a new array. Returns `Err` if an array named `name` already exists.
    /// `audit_retain_ms` / `minimum_audit_retain_ms` come from
    /// `CREATE ARRAY ... WITH (...)`. They are stored rather than defaulted:
    /// the retention merge in `ops::compact` and the inline purge in
    /// `retention` both read them, so an array created with a retention policy
    /// that was dropped on the way in would keep every tile version while
    /// reporting the policy as set.
    pub async fn create_array<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        schema: ArraySchema,
        audit_retain_ms: Option<i64>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<(), LiteError> {
        if self.arrays.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("array '{name}' already exists"),
            });
        }
        // NUL separates the array name from the coordinate in binding keys.
        if name.contains('\0') {
            return Err(LiteError::BadRequest {
                detail: format!("array name {name:?} contains NUL: use a name without NUL"),
            });
        }
        let schema_bytes =
            zerompk::to_msgpack_vec(&schema).map_err(|e| LiteError::Serialization {
                detail: format!("encode schema: {e}"),
            })?;
        let schema_hash = hash_schema(&schema)?;
        let entry = ArrayCatalogEntry {
            name: name.to_owned(),
            schema_bytes,
            schema_hash,
            audit_retain_ms,
            minimum_audit_retain_ms,
        };
        let mut catalog = ArrayCatalog::open(Arc::clone(storage)).await?;
        catalog.insert(entry).await?;
        self.arrays.insert(
            name.to_owned(),
            ArrayState {
                schema,
                schema_hash,
                manifest: ArrayManifest::new(),
                memtable: ArrayMemtable::new(),
                audit_retain_ms,
                bindings: CellBindings::default(),
            },
        );
        Ok(())
    }

    /// Delete an array and all its data.
    pub async fn delete_array<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .remove(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        crate::engine::array::manifest::drop_manifest(
            storage,
            name,
            &state.manifest,
            state.bindings.delete_ops(name),
        )
        .await?;
        let mut catalog = ArrayCatalog::open(Arc::clone(storage)).await?;
        catalog.remove(name).await?;
        Ok(())
    }

    /// Write a cell into the array.
    ///
    /// Every put path reaches this function, so it is the one place that binds
    /// a surrogate. The first write of a coordinate takes the next free
    /// surrogate. Later writes, and writes after a delete or erase, reuse it.
    pub async fn put_cell<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        write: CellWrite,
    ) -> Result<(), LiteError> {
        let CellWrite {
            coord,
            attrs,
            system_from_ms,
            valid_from_ms,
            valid_until_ms,
        } = write;
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;

        let key = encode_coord(&coord)?;
        let fresh = state.bindings.get(&key).is_none();
        let surrogate = state
            .bindings
            .bind(name, key.clone(), &mut self.next_surrogate)?;

        let put = state.memtable.put_cell(
            &state.schema,
            PutCellArgs {
                coord,
                attrs,
                surrogate,
                system_from_ms,
                valid_from_ms,
                valid_until_ms,
            },
        );
        if let Err(e) = put {
            if fresh {
                state
                    .bindings
                    .rollback_new(&key, surrogate, &mut self.next_surrogate);
            }
            return Err(e);
        }

        if state.memtable.stats().cell_count >= self.flush_threshold {
            let name_owned = name.to_owned();
            self.flush_memtable(storage, &name_owned).await?;
        }

        Ok(())
    }

    /// Soft-delete a cell by appending a tombstone version.
    pub fn delete_cell(
        &mut self,
        name: &str,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let schema = state.schema.clone();
        state
            .memtable
            .delete_cell(&schema, coord, system_from_ms)
            .map(|_| ())
    }

    /// GDPR erase a cell — appends the `0xFE` erasure sentinel and immediately
    /// flushes so the sentinel is durably stored on disk.
    pub async fn gdpr_erase_cell<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let schema = state.schema.clone();
        state.memtable.erase_cell(&schema, coord, system_from_ms)?;
        self.flush_memtable(storage, name).await
    }

    /// Flush pending memtable data to a persistent segment.
    pub async fn flush<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
    ) -> Result<(), LiteError> {
        self.flush_memtable(storage, name).await
    }

    async fn flush_memtable<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;

        if state.memtable.is_empty() {
            return Ok(());
        }

        let schema = state.schema.clone();
        let schema_hash = state.schema_hash;
        let tiles = state.memtable.drain_and_materialise(&schema)?;

        if tiles.is_empty() {
            return Ok(());
        }

        let refs: Vec<_> = tiles.iter().map(|(id, tile)| (*id, tile)).collect();
        let new_id = state.manifest.next_id;
        state.manifest.next_id += 1;
        let bytes = write_segment(storage, name, new_id, schema_hash, &refs).await?;
        state.manifest.segments.push(SegmentRef {
            id: new_id,
            byte_len: bytes.len() as u64,
        });

        // The memtable has no WAL. New bindings and the high-water mark land
        // in one batch after the segment and before the manifest. A crash
        // before this batch loses the cells and their bindings together. A
        // crash between this batch and the manifest leaves bindings with no
        // cell. Those bindings stay valid.
        let mut binding_ops = state.bindings.dirty_ops(name);
        binding_ops.push(hwm_op(self.next_surrogate));
        storage.batch_write(&binding_ops).await?;
        state.bindings.mark_clean();

        save_manifest(storage, name, &state.manifest).await?;

        // Retention compaction (only if configured).
        if let Some(retain_ms) = state.audit_retain_ms {
            let now_ms = crate::runtime::now_millis_i64();
            crate::engine::array::retention::run_retention(
                storage,
                name,
                &mut state.manifest,
                &schema,
                schema_hash,
                retain_ms,
                now_ms,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::OPEN_UPPER;
    use std::sync::Arc;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("test")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap()
    }

    async fn storage() -> Arc<PagedbStorageMem> {
        Arc::new(PagedbStorageMem::open_in_memory().await.unwrap())
    }

    /// A retention policy set at CREATE must reach the array, and survive a
    /// reopen. The purge in `retention` and the merge in `ops::compact` both
    /// read it, so an array that silently defaulted to `None` would keep every
    /// tile version while reporting the policy as set.
    #[tokio::test]
    async fn create_array_keeps_its_retention_policy() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "retained", schema(), Some(86_400_000), Some(3_600_000))
            .await
            .unwrap();

        assert_eq!(
            engine
                .arrays
                .get("retained")
                .and_then(|a| a.audit_retain_ms),
            Some(86_400_000),
            "the retention policy must reach the live array state"
        );

        let reopened = ArrayEngineState::open(&s).await.unwrap();
        assert_eq!(
            reopened
                .arrays
                .get("retained")
                .and_then(|a| a.audit_retain_ms),
            Some(86_400_000),
            "the retention policy must survive a reopen — it is persisted in \
             the catalog entry, not just held in memory"
        );
    }

    #[tokio::test]
    async fn create_and_put_and_read() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "a", schema(), None, None)
            .await
            .unwrap();
        engine
            .put_cell(
                &s,
                "a",
                CellWrite {
                    coord: vec![CoordValue::Int64(1)],
                    attrs: vec![CellValue::Int64(42)],
                    system_from_ms: 100,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .await
            .unwrap();
        engine.flush(&s, "a").await.unwrap();
        let result = engine
            .read_coord(&s, "a", &[CoordValue::Int64(1)], 200)
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().attrs[0], CellValue::Int64(42));
    }

    #[tokio::test]
    async fn delete_returns_none() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "b", schema(), None, None)
            .await
            .unwrap();
        engine
            .put_cell(
                &s,
                "b",
                CellWrite {
                    coord: vec![CoordValue::Int64(2)],
                    attrs: vec![CellValue::Int64(7)],
                    system_from_ms: 10,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .await
            .unwrap();
        engine
            .delete_cell("b", vec![CoordValue::Int64(2)], 20)
            .unwrap();
        engine.flush(&s, "b").await.unwrap();
        let result = engine
            .read_coord(&s, "b", &[CoordValue::Int64(2)], 100)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn open_restores_catalog() {
        let s = storage().await;
        {
            let mut engine = ArrayEngineState::open(&s).await.unwrap();
            engine
                .create_array(&s, "persist", schema(), None, None)
                .await
                .unwrap();
        }
        let engine2 = ArrayEngineState::open(&s).await.unwrap();
        assert!(engine2.arrays.contains_key("persist"));
    }

    #[tokio::test]
    async fn bitemporal_as_of() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "bt", schema(), None, None)
            .await
            .unwrap();
        engine
            .put_cell(
                &s,
                "bt",
                CellWrite {
                    coord: vec![CoordValue::Int64(0)],
                    attrs: vec![CellValue::Int64(10)],
                    system_from_ms: 100,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .await
            .unwrap();
        engine
            .put_cell(
                &s,
                "bt",
                CellWrite {
                    coord: vec![CoordValue::Int64(0)],
                    attrs: vec![CellValue::Int64(20)],
                    system_from_ms: 200,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .await
            .unwrap();
        engine.flush(&s, "bt").await.unwrap();
        let r = engine
            .read_coord(&s, "bt", &[CoordValue::Int64(0)], 150)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.attrs[0], CellValue::Int64(10));
        let r2 = engine
            .read_coord(&s, "bt", &[CoordValue::Int64(0)], 300)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2.attrs[0], CellValue::Int64(20));
    }

    async fn put_x(
        engine: &mut ArrayEngineState,
        s: &Arc<PagedbStorageMem>,
        name: &str,
        x: i64,
        sys: i64,
    ) {
        engine
            .put_cell(
                s,
                name,
                CellWrite {
                    coord: vec![CoordValue::Int64(x)],
                    attrs: vec![CellValue::Int64(x * 10)],
                    system_from_ms: sys,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .await
            .unwrap();
    }

    async fn surrogate_of(
        engine: &ArrayEngineState,
        s: &Arc<PagedbStorageMem>,
        name: &str,
        x: i64,
    ) -> nodedb_types::Surrogate {
        engine
            .read_coord(s, name, &[CoordValue::Int64(x)], i64::MAX - 1)
            .await
            .unwrap()
            .expect("live cell")
            .surrogate
    }

    async fn binding_rows(s: &Arc<PagedbStorageMem>, name: &str) -> usize {
        let mut prefix = b"surrogate\0".to_vec();
        prefix.extend_from_slice(name.as_bytes());
        prefix.push(0);
        s.scan_prefix(nodedb_types::Namespace::Array, &prefix)
            .await
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn overwrite_keeps_the_surrogate() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "ow", schema(), None, None)
            .await
            .unwrap();
        put_x(&mut engine, &s, "ow", 1, 100).await;
        let first = surrogate_of(&engine, &s, "ow", 1).await;
        put_x(&mut engine, &s, "ow", 1, 200).await;
        engine.flush(&s, "ow").await.unwrap();
        let older = engine
            .read_coord(&s, "ow", &[CoordValue::Int64(1)], 150)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first, nodedb_types::Surrogate::ZERO);
        assert_eq!(older.surrogate, first);
        assert_eq!(surrogate_of(&engine, &s, "ow", 1).await, first);
    }

    #[tokio::test]
    async fn distinct_coordinates_get_distinct_nonzero_surrogates() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "d", schema(), None, None)
            .await
            .unwrap();
        put_x(&mut engine, &s, "d", 1, 100).await;
        put_x(&mut engine, &s, "d", 2, 100).await;
        let a = surrogate_of(&engine, &s, "d", 1).await;
        let b = surrogate_of(&engine, &s, "d", 2).await;
        assert_ne!(a, nodedb_types::Surrogate::ZERO);
        assert_ne!(b, nodedb_types::Surrogate::ZERO);
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn reopen_after_flush_never_reuses_a_surrogate() {
        let s = storage().await;
        let (s1, s2) = {
            let mut engine = ArrayEngineState::open(&s).await.unwrap();
            engine
                .create_array(&s, "r", schema(), None, None)
                .await
                .unwrap();
            put_x(&mut engine, &s, "r", 1, 100).await;
            put_x(&mut engine, &s, "r", 2, 100).await;
            engine.flush(&s, "r").await.unwrap();
            (
                surrogate_of(&engine, &s, "r", 1).await,
                surrogate_of(&engine, &s, "r", 2).await,
            )
        };

        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        put_x(&mut engine, &s, "r", 3, 200).await;
        put_x(&mut engine, &s, "r", 1, 200).await;
        engine.flush(&s, "r").await.unwrap();
        let s3 = surrogate_of(&engine, &s, "r", 3).await;
        assert_ne!(s3, s1);
        assert_ne!(s3, s2);
        assert_ne!(s3, nodedb_types::Surrogate::ZERO);
        assert_eq!(surrogate_of(&engine, &s, "r", 1).await, s1);
        assert_eq!(surrogate_of(&engine, &s, "r", 2).await, s2);
    }

    #[tokio::test]
    async fn put_after_delete_reuses_the_surrogate() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "pd", schema(), None, None)
            .await
            .unwrap();
        put_x(&mut engine, &s, "pd", 4, 100).await;
        let before = surrogate_of(&engine, &s, "pd", 4).await;
        engine
            .delete_cell("pd", vec![CoordValue::Int64(4)], 200)
            .unwrap();
        engine.flush(&s, "pd").await.unwrap();
        put_x(&mut engine, &s, "pd", 4, 300).await;
        assert_eq!(surrogate_of(&engine, &s, "pd", 4).await, before);
    }

    #[tokio::test]
    async fn delete_array_removes_binding_keys() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "gone", schema(), None, None)
            .await
            .unwrap();
        put_x(&mut engine, &s, "gone", 1, 100).await;
        put_x(&mut engine, &s, "gone", 2, 100).await;
        engine.flush(&s, "gone").await.unwrap();
        assert_eq!(binding_rows(&s, "gone").await, 2);
        engine.delete_array(&s, "gone").await.unwrap();
        assert_eq!(binding_rows(&s, "gone").await, 0);
    }

    #[tokio::test]
    async fn surrogate_bitmap_scan_returns_the_bound_ids() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "bm", schema(), None, None)
            .await
            .unwrap();
        put_x(&mut engine, &s, "bm", 1, 100).await;
        put_x(&mut engine, &s, "bm", 5, 100).await;
        engine.flush(&s, "bm").await.unwrap();
        put_x(&mut engine, &s, "bm", 9, 200).await;
        let mut expected = roaring::RoaringBitmap::new();
        for x in [1, 5, 9] {
            expected.insert(surrogate_of(&engine, &s, "bm", x).await.as_u32());
        }
        let got = engine
            .surrogate_bitmap_scan(&s, "bm", vec![None], i64::MAX - 1)
            .await
            .unwrap();
        assert_eq!(got, expected);
        assert_eq!(got.len(), 3);
        assert!(!got.contains(0));
    }

    #[tokio::test]
    async fn bindings_of_a_exclude_array_a_colon_b() {
        let s = storage().await;
        let (in_a, in_ab) = {
            let mut engine = ArrayEngineState::open(&s).await.unwrap();
            engine
                .create_array(&s, "a", schema(), None, None)
                .await
                .unwrap();
            engine
                .create_array(&s, "a:b", schema(), None, None)
                .await
                .unwrap();
            put_x(&mut engine, &s, "a", 1, 100).await;
            put_x(&mut engine, &s, "a:b", 1, 100).await;
            put_x(&mut engine, &s, "a:b", 2, 100).await;
            engine.flush(&s, "a").await.unwrap();
            engine.flush(&s, "a:b").await.unwrap();
            (
                surrogate_of(&engine, &s, "a", 1).await,
                surrogate_of(&engine, &s, "a:b", 1).await,
            )
        };
        assert_ne!(in_a, in_ab, "one counter covers every array");
        assert_eq!(binding_rows(&s, "a").await, 1);
        assert_eq!(binding_rows(&s, "a:b").await, 2);

        let engine = ArrayEngineState::open(&s).await.unwrap();
        assert_eq!(surrogate_of(&engine, &s, "a", 1).await, in_a);
        assert_eq!(surrogate_of(&engine, &s, "a:b", 1).await, in_ab);
    }

    #[tokio::test]
    async fn create_array_refuses_a_name_with_nul() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        assert!(
            engine
                .create_array(&s, "a\0b", schema(), None, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn failed_put_binds_no_surrogate() {
        let s = storage().await;
        let mut engine = ArrayEngineState::open(&s).await.unwrap();
        engine
            .create_array(&s, "f", schema(), None, None)
            .await
            .unwrap();
        // x = -1 lies below the domain low bound of 0.
        let err = engine
            .put_cell(
                &s,
                "f",
                CellWrite {
                    coord: vec![CoordValue::Int64(-1)],
                    attrs: vec![CellValue::Int64(1)],
                    system_from_ms: 100,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .await;
        assert!(err.is_err());
        put_x(&mut engine, &s, "f", 1, 100).await;
        assert_eq!(
            surrogate_of(&engine, &s, "f", 1).await,
            nodedb_types::Surrogate::new(1)
        );
    }
}
