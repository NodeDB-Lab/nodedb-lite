// SPDX-License-Identifier: Apache-2.0
//! WalAppend handler for Lite.
//!
//! On Origin, WalAppend is the Raft-durable commit path that assigns a
//! cluster-wide LSN. On Lite the same semantics — durability + monotonic
//! ordering — are satisfied by redb's transactional commit combined with an
//! atomic LSN counter persisted in the Meta namespace.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, StorageEngineSync, WriteOp};

/// Key under which the next available WAL LSN is stored in Namespace::Meta.
const WAL_LSN_KEY: &[u8] = b"__lite_wal_lsn__";

/// In-process monotonic LSN counter, mirroring the persisted value.
///
/// Loaded from storage on first use; subsequent increments are atomic so that
/// concurrent callers (if any) never produce duplicate LSNs.
static WAL_LSN: AtomicU64 = AtomicU64::new(0);
static WAL_LSN_LOADED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Load the WAL LSN from storage if not yet initialised, then return the
/// next available LSN (incrementing both the in-process counter and the
/// persisted value).
fn next_lsn<S: StorageEngine + StorageEngineSync>(storage: &Arc<S>) -> Result<u64, LiteError> {
    // Lazy-load from persistent storage on first call.
    if !WAL_LSN_LOADED.load(Ordering::Acquire) {
        let persisted = storage
            .get_sync(Namespace::Meta, WAL_LSN_KEY)?
            .map(|b| {
                let arr: [u8; 8] = b.try_into().unwrap_or([0u8; 8]);
                u64::from_le_bytes(arr)
            })
            .unwrap_or(0);
        WAL_LSN.store(persisted, Ordering::Release);
        WAL_LSN_LOADED.store(true, Ordering::Release);
    }

    let lsn = WAL_LSN.fetch_add(1, Ordering::AcqRel);
    // Persist the updated counter so it survives restarts.
    storage.put_sync(Namespace::Meta, WAL_LSN_KEY, &(lsn + 1).to_le_bytes())?;
    Ok(lsn)
}

/// Handle a `MetaOp::WalAppend`.
///
/// Writes `payload` to Namespace::Meta under a key derived from the assigned
/// LSN, then commits via the `StorageEngineSync::put_sync` path (which is
/// backed by a redb write transaction and is O_DIRECT durable). Returns the
/// assigned LSN as `Value::Integer`.
pub async fn handle_wal_append<S: StorageEngine + StorageEngineSync>(
    storage: &Arc<S>,
    payload: &[u8],
) -> Result<QueryResult, LiteError> {
    let lsn = next_lsn(storage)?;
    // Store the payload keyed by LSN so a crash-recover scan can reconstruct
    // ordering. Key: b"wal:" + lsn as 8 LE bytes.
    let mut key = Vec::with_capacity(12);
    key.extend_from_slice(b"wal:");
    key.extend_from_slice(&lsn.to_le_bytes());
    storage
        .batch_write(&[WriteOp::Put {
            ns: Namespace::Meta,
            key,
            value: payload.to_vec(),
        }])
        .await?;
    Ok(QueryResult {
        columns: vec!["lsn".into()],
        rows: vec![vec![Value::Integer(lsn as i64)]],
        rows_affected: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn wal_append_assigns_monotonic_lsn() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());

        // Reset static state between test runs in the same process.
        WAL_LSN_LOADED.store(false, Ordering::SeqCst);
        WAL_LSN.store(0, Ordering::SeqCst);

        let r1 = handle_wal_append(&storage, b"op1").await.unwrap();
        let r2 = handle_wal_append(&storage, b"op2").await.unwrap();

        let lsn1 = match &r1.rows[0][0] {
            Value::Integer(n) => *n,
            _ => panic!("expected integer lsn"),
        };
        let lsn2 = match &r2.rows[0][0] {
            Value::Integer(n) => *n,
            _ => panic!("expected integer lsn"),
        };
        assert!(lsn2 > lsn1, "LSNs must be strictly increasing");
        assert_eq!(lsn1 + 1, lsn2);
    }
}
