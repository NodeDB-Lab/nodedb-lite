//! Per-array segment manifest persisted in the `Array` namespace.
//!
//! Key layout: `manifest:{name}` → zerompk-encoded `ArrayManifest`.
//! Each segment reference carries a numeric ID used to form the segment
//! storage key (`segment:{name}:{id}`).

use std::sync::Arc;

use nodedb_types::Namespace;
use serde::{Deserialize, Serialize};

use crate::error::LiteError;
use crate::storage::engine::{StorageEngineSync, WriteOp};

const MANIFEST_PREFIX: &str = "manifest:";

/// Reference to one flushed segment.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SegmentRef {
    /// Monotonically increasing segment ID within this array.
    pub id: u64,
    /// Byte length of the segment payload stored in redb.
    pub byte_len: u64,
}

/// Full manifest for one array.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ArrayManifest {
    pub segments: Vec<SegmentRef>,
    /// Next segment ID to assign.
    pub next_id: u64,
}

impl ArrayManifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next segment ID and push a new `SegmentRef`.
    pub fn push_segment(&mut self, byte_len: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.segments.push(SegmentRef { id, byte_len });
        id
    }

    /// Remove segments whose IDs appear in `drop_ids` (retention compaction).
    pub fn drop_segments(&mut self, drop_ids: &[u64]) {
        let set: std::collections::HashSet<u64> = drop_ids.iter().copied().collect();
        self.segments.retain(|s| !set.contains(&s.id));
    }
}

fn manifest_key(name: &str) -> Vec<u8> {
    let mut k = MANIFEST_PREFIX.as_bytes().to_vec();
    k.extend_from_slice(name.as_bytes());
    k
}

pub fn segment_key(name: &str, id: u64) -> Vec<u8> {
    format!("segment:{name}:{id}").into_bytes()
}

/// Persist the manifest for `name` to storage.
pub fn save_manifest<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    manifest: &ArrayManifest,
) -> Result<(), LiteError> {
    let bytes = zerompk::to_msgpack_vec(manifest).map_err(|e| LiteError::Serialization {
        detail: format!("encode ArrayManifest: {e}"),
    })?;
    storage.put_sync(Namespace::Array, &manifest_key(name), &bytes)
}

/// Load the manifest for `name` from storage. Returns an empty manifest
/// when the key is absent (first open of a freshly created array).
pub fn load_manifest<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
) -> Result<ArrayManifest, LiteError> {
    match storage.get_sync(Namespace::Array, &manifest_key(name))? {
        Some(bytes) => zerompk::from_msgpack(&bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode ArrayManifest: {e}"),
        }),
        None => Ok(ArrayManifest::new()),
    }
}

/// Remove the manifest and all segment blobs for `name` from storage.
/// Used by `delete_array`.
pub fn drop_manifest<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    manifest: &ArrayManifest,
) -> Result<(), LiteError> {
    let mut ops: Vec<WriteOp> = manifest
        .segments
        .iter()
        .map(|s| WriteOp::Delete {
            ns: Namespace::Array,
            key: segment_key(name, s.id),
        })
        .collect();
    ops.push(WriteOp::Delete {
        ns: Namespace::Array,
        key: manifest_key(name),
    });
    storage.batch_write_sync(&ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use std::sync::Arc;

    #[test]
    fn manifest_push_and_persist() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let mut m = ArrayManifest::new();
        m.push_segment(128);
        m.push_segment(256);
        save_manifest(&storage, "a", &m).unwrap();

        let m2 = load_manifest(&storage, "a").unwrap();
        assert_eq!(m2.segments.len(), 2);
        assert_eq!(m2.segments[0].id, 0);
        assert_eq!(m2.segments[1].id, 1);
        assert_eq!(m2.next_id, 2);
    }

    #[test]
    fn load_missing_returns_empty() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let m = load_manifest(&storage, "no_such").unwrap();
        assert!(m.segments.is_empty());
        assert_eq!(m.next_id, 0);
    }

    #[test]
    fn drop_removes_manifest_and_segments() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let mut m = ArrayManifest::new();
        let id = m.push_segment(64);
        let seg_bytes = b"fake_segment_bytes";
        storage
            .put_sync(Namespace::Array, &segment_key("b", id), seg_bytes)
            .unwrap();
        save_manifest(&storage, "b", &m).unwrap();

        drop_manifest(&storage, "b", &m).unwrap();

        assert!(load_manifest(&storage, "b").unwrap().segments.is_empty());
        assert!(
            storage
                .get_sync(Namespace::Array, &segment_key("b", id))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn drop_segments_filter() {
        let mut m = ArrayManifest::new();
        m.push_segment(10);
        m.push_segment(20);
        m.push_segment(30);
        m.drop_segments(&[0, 2]);
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].id, 1);
    }
}
