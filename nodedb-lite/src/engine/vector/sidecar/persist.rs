// SPDX-License-Identifier: Apache-2.0

//! Sidecar persistence: store/restore codec sidecars from redb.
//!
//! Persisting on every insert is avoided (training-free codecs are fast, but
//! storage writes add per-insert latency).  Instead, callers persist on
//! `delete` — where a missing entry after restart would require full retraining
//! rather than an incremental rebuild — and rely on lazy `ensure_sidecar`
//! rebuild for crashes between inserts.

use nodedb_types::Namespace;

use crate::engine::vector::VectorState;
use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

pub(super) fn sidecar_storage_key(index_key: &str) -> String {
    format!("sidecar:{index_key}")
}

/// Persist the current in-memory sidecar for `index_key` to redb.
///
/// Best-effort: when the sidecar is absent the call is a no-op.  Serialization
/// or storage failures are logged as warnings — the in-memory sidecar remains
/// the source of truth and the next restart will trigger an `ensure_sidecar`
/// rebuild.
pub(crate) async fn persist_sidecar<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
) -> Result<(), LiteError> {
    let bytes = {
        let sidecars = vector_state.codec_sidecars.lock_or_recover();
        match sidecars.get(index_key) {
            None => return Ok(()),
            Some(sidecar) => match sidecar.to_bytes() {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        index_key,
                        error = %e,
                        "sidecar serialize failed; skipping persist (will rebuild on restart)"
                    );
                    return Ok(());
                }
            },
        }
    };

    let key = sidecar_storage_key(index_key);
    if let Err(e) = vector_state
        .storage
        .put(Namespace::Vector, key.as_bytes(), &bytes)
        .await
    {
        tracing::warn!(
            index_key,
            error = %e,
            "sidecar storage write failed; in-memory sidecar remains valid"
        );
    }
    Ok(())
}

/// Try to restore a persisted sidecar for `index_key` from storage.
///
/// Returns:
/// - `Ok(true)` — sidecar was already in memory (no I/O) or was successfully
///   restored from persisted bytes.
/// - `Ok(false)` — no persisted bytes exist; caller should fall through to
///   `ensure_sidecar` for training.
/// - `Err(LiteError::Storage)` — bytes were found but failed to deserialize
///   (bad magic, unknown version, corrupt payload). Caller should attempt
///   retraining via `ensure_sidecar`.
pub(crate) async fn try_restore_sidecar<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
) -> Result<bool, LiteError> {
    // Fast path: already loaded into memory.
    {
        let sidecars = vector_state.codec_sidecars.lock_or_recover();
        if sidecars.contains_key(index_key) {
            return Ok(true);
        }
    }

    let key = sidecar_storage_key(index_key);
    let bytes = vector_state
        .storage
        .get(Namespace::Vector, key.as_bytes())
        .await?;

    let bytes = match bytes {
        None => return Ok(false),
        Some(b) => b,
    };

    let sidecar = nodedb_vector::rerank::CodecSidecar::from_bytes(&bytes).map_err(|e| {
        LiteError::Storage {
            detail: format!("sidecar restore for '{index_key}': {e}"),
        }
    })?;

    vector_state
        .codec_sidecars
        .lock_or_recover()
        .insert(index_key.to_string(), sidecar);

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;
    use nodedb_types::Namespace;
    use nodedb_vector::HnswIndex;
    use nodedb_vector::rerank::CodecName;

    use crate::engine::vector::sidecar::install::install_sidecar_for_index;
    use crate::engine::vector::state::VectorState;
    use crate::error::LiteError;
    use crate::storage::engine::{KvPair, StorageEngine, WriteOp};

    /// A `StorageEngine` backed by an in-memory `HashMap` that actually stores
    /// and retrieves bytes. Used for persist/restore tests.
    struct RealMemStore {
        data: StdMutex<StdHashMap<Vec<u8>, Vec<u8>>>,
    }

    impl RealMemStore {
        fn new() -> Self {
            Self {
                data: StdMutex::new(StdHashMap::new()),
            }
        }

        fn write_raw(&self, key: &[u8], value: Vec<u8>) {
            self.data.lock().unwrap().insert(key.to_vec(), value);
        }
    }

    #[async_trait]
    impl StorageEngine for RealMemStore {
        async fn get(&self, _ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
            Ok(self.data.lock().unwrap().get(key).cloned())
        }

        async fn put(&self, _ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, _ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }

        async fn scan_prefix(
            &self,
            _ns: Namespace,
            _prefix: &[u8],
        ) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }

        async fn batch_write(&self, _ops: &[WriteOp]) -> Result<(), LiteError> {
            Ok(())
        }

        async fn count(&self, _ns: Namespace) -> Result<u64, LiteError> {
            Ok(0)
        }
    }

    fn make_state() -> VectorState<RealMemStore> {
        VectorState::new(Arc::new(RealMemStore::new()), 50)
    }

    fn populate_index(state: &VectorState<RealMemStore>, index_key: &str, dim: usize, n: usize) {
        let mut indices = state.hnsw_indices.lock_or_recover();
        let index = indices
            .entry(index_key.to_string())
            .or_insert_with(|| HnswIndex::new(dim, Default::default()));
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32 * 0.01).collect();
            index.insert(v).expect("insert ok");
        }
    }

    #[tokio::test]
    async fn persist_then_restore_sq8() {
        let state = make_state();
        let key = "col_persist_sq8";
        const DIM: usize = 16;
        const N: usize = 3;

        populate_index(&state, key, DIM, N);
        install_sidecar_for_index(&state, key, CodecName::Sq8).expect("install ok");

        assert_eq!(
            state
                .codec_sidecars
                .lock_or_recover()
                .get(key)
                .unwrap()
                .len(),
            N
        );

        persist_sidecar(&state, key).await.expect("persist ok");

        state.codec_sidecars.lock_or_recover().remove(key);
        assert!(
            !state.codec_sidecars.lock_or_recover().contains_key(key),
            "sidecar must be gone before restore"
        );

        let restored = try_restore_sidecar(&state, key).await.expect("restore ok");
        assert!(restored, "try_restore_sidecar must return Ok(true)");

        let sidecars = state.codec_sidecars.lock_or_recover();
        let sidecar = sidecars.get(key).expect("sidecar present after restore");
        assert_eq!(
            sidecar.len(),
            N,
            "restored sidecar must hold all {N} encoded entries"
        );
        assert_eq!(sidecar.codec_name(), CodecName::Sq8);
    }

    #[tokio::test]
    async fn try_restore_returns_false_when_no_persisted_bytes() {
        let state = make_state();
        let result = try_restore_sidecar(&state, "nonexistent_col")
            .await
            .expect("should not error when no bytes stored");
        assert!(!result, "expected Ok(false) when nothing is persisted");
    }

    #[tokio::test]
    async fn try_restore_returns_err_on_corrupt_bytes() {
        let state = make_state();
        let key = "col_corrupt";
        let storage_key = sidecar_storage_key(key);
        state
            .storage
            .write_raw(storage_key.as_bytes(), b"GARBAGE_NOT_A_SIDECAR".to_vec());

        let result = try_restore_sidecar(&state, key).await;
        assert!(
            result.is_err(),
            "corrupt bytes must return Err, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn persist_no_sidecar_is_noop() {
        let state = make_state();
        let result = persist_sidecar(&state, "no_sidecar_key").await;
        assert!(result.is_ok(), "persist with no sidecar must return Ok(())");
    }
}
