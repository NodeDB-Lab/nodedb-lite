// SPDX-License-Identifier: Apache-2.0
//! Tenant lifecycle operations for Lite: snapshot, restore, and purge.
//!
//! # Tenancy model on Lite
//!
//! Lite is currently single-tenant by default (tenant_id = 0). Multi-tenancy
//! is supported by prefixing every KV key with `t/<tenant_id>/` for tenants
//! other than 0. Keys without the `t/` prefix are treated as belonging to
//! tenant 0 — existing data continues to work without migration.
//!
//! For tenant 0, `collect_tenant_keys` matches BOTH the legacy un-prefixed keys
//! AND any explicit `t/0/`-prefixed keys written by newer code. Keys starting
//! with `t/<non-zero>/` are excluded, so tenant 0 can never capture
//! another tenant's data.
//!
//! # Snapshot wire format
//!
//! `CreateTenantSnapshot` serialises all entries for the requested tenant as a
//! MessagePack blob: `Vec<(u8, Vec<u8>, Vec<u8>)>` where each tuple is
//! `(namespace_byte, user_key_bytes, value_bytes)`.  User-key bytes are stored
//! **without** any tenant prefix so restoring into a different tenant_id is
//! safe: `RestoreTenantSnapshot` applies the target tenant's prefix.
//!
//! The blob is returned as a single-row `QueryResult` with column `"snapshot"`.

use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{KvPair, StorageEngine, WriteOp};

/// One entry: `(namespace_byte, user_key_without_tenant_prefix, value)`.
type SnapshotEntry = (u8, Vec<u8>, Vec<u8>);

/// `(full_storage_key, user_key_without_prefix, namespace, value)` tuple
/// returned by `collect_tenant_keys`.
type TenantKeyTuple = (Vec<u8>, Vec<u8>, Namespace, Vec<u8>);

/// Tenant key prefix: `t/<tenant_id>/`.
fn tenant_prefix(tenant_id: u64) -> Vec<u8> {
    format!("t/{tenant_id}/").into_bytes()
}

/// All `Namespace` variants in declaration order.
const ALL_NAMESPACES: &[Namespace] = &[
    Namespace::Meta,
    Namespace::Vector,
    Namespace::Graph,
    Namespace::Crdt,
    Namespace::LoroState,
    Namespace::Spatial,
    Namespace::Strict,
    Namespace::Columnar,
    Namespace::Kv,
    Namespace::Array,
    Namespace::ArrayOpLog,
    Namespace::ArrayDelta,
    Namespace::Fts,
];

/// Scan every namespace and return all `(ns_byte, full_storage_key, value)`
/// tuples that belong to `tenant_id`, plus the user-key (without prefix).
///
/// Returns `(full_storage_key, user_key, ns, value)` tuples.
async fn collect_tenant_keys<S: StorageEngine>(
    storage: &S,
    tenant_id: u64,
) -> Result<Vec<TenantKeyTuple>, LiteError> {
    let explicit_prefix = tenant_prefix(tenant_id);
    let mut result: Vec<TenantKeyTuple> = Vec::new();

    for &ns in ALL_NAMESPACES {
        let pairs: Vec<KvPair> = storage.scan_prefix(ns, b"").await?;
        for (storage_key, value) in pairs {
            if storage_key.starts_with(&explicit_prefix) {
                let user_key = storage_key[explicit_prefix.len()..].to_vec();
                result.push((storage_key, user_key, ns, value));
            } else if tenant_id == 0 && !storage_key.starts_with(b"t/") {
                // Legacy un-prefixed key — belongs to the default tenant (0).
                // Keys starting with `t/` belong to explicitly-namespaced tenants.
                let user_key = storage_key.clone();
                result.push((storage_key, user_key, ns, value));
            }
        }
    }

    Ok(result)
}

/// Handle `MetaOp::CreateTenantSnapshot { tenant_id }`.
///
/// Serialises all storage entries for `tenant_id` as a MessagePack blob and
/// returns it in a single-row `QueryResult` with column `"snapshot"`.
pub async fn handle_create_tenant_snapshot<S: StorageEngine>(
    storage: &S,
    tenant_id: u64,
) -> Result<QueryResult, LiteError> {
    let raw = collect_tenant_keys(storage, tenant_id).await?;

    let entries: Vec<SnapshotEntry> = raw
        .into_iter()
        .map(|(_full_key, user_key, ns, value)| (ns as u8, user_key, value))
        .collect();

    let blob = zerompk::to_msgpack_vec(&entries).map_err(|e| LiteError::Storage {
        detail: format!("tenant snapshot serialise failed: {e}"),
    })?;

    Ok(QueryResult {
        columns: vec!["snapshot".into()],
        rows: vec![vec![Value::Bytes(blob)]],
        rows_affected: entries.len() as u64,
    })
}

/// Handle `MetaOp::RestoreTenantSnapshot { tenant_id, snapshot }`.
///
/// Parses the snapshot blob and writes every entry under `tenant_id`'s prefix
/// in a single atomic batch.  If the snapshot originated from a different
/// tenant, keys are re-prefixed transparently.
///
/// For tenant 0, the explicit `t/0/` prefix is used so restored keys are
/// distinguishable from legacy un-prefixed data.
pub async fn handle_restore_tenant_snapshot<S: StorageEngine>(
    storage: &S,
    tenant_id: u64,
    snapshot: &[u8],
) -> Result<QueryResult, LiteError> {
    let entries: Vec<SnapshotEntry> =
        zerompk::from_msgpack(snapshot).map_err(|e| LiteError::Storage {
            detail: format!("tenant snapshot parse failed: {e}"),
        })?;

    let prefix = tenant_prefix(tenant_id);
    let mut ops: Vec<WriteOp> = Vec::with_capacity(entries.len());

    for (ns_byte, user_key, value) in &entries {
        let ns = Namespace::from_u8(*ns_byte).ok_or_else(|| LiteError::Storage {
            detail: format!("snapshot contains unknown namespace byte {ns_byte}"),
        })?;
        let mut full_key = prefix.clone();
        full_key.extend_from_slice(user_key);
        ops.push(WriteOp::Put {
            ns,
            key: full_key,
            value: value.clone(),
        });
    }

    let written = ops.len();
    storage.batch_write(&ops).await?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: written as u64,
    })
}

/// Handle `MetaOp::PurgeTenant { tenant_id }`.
///
/// Deletes every storage entry that belongs to `tenant_id` in a single atomic
/// batch.  Idempotent: re-running after a crash is safe.
pub async fn handle_purge_tenant<S: StorageEngine>(
    storage: &S,
    tenant_id: u64,
) -> Result<QueryResult, LiteError> {
    let raw = collect_tenant_keys(storage, tenant_id).await?;

    let ops: Vec<WriteOp> = raw
        .into_iter()
        .map(|(full_key, _user_key, ns, _value)| WriteOp::Delete { ns, key: full_key })
        .collect();

    let deleted = ops.len() as u64;
    storage.batch_write(&ops).await?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: deleted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    async fn make_storage() -> PagedbStorageMem {
        PagedbStorageMem::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn snapshot_roundtrip_tenant_zero_legacy_keys() {
        let s = make_storage().await;
        // Write legacy un-prefixed keys (tenant 0 legacy, no `t/` prefix).
        s.put(Namespace::Kv, b"doc:1", b"value1").await.unwrap();
        s.put(Namespace::Kv, b"doc:2", b"value2").await.unwrap();
        s.put(Namespace::Meta, b"schema:v1", b"meta").await.unwrap();

        let result = handle_create_tenant_snapshot(&s, 0).await.unwrap();
        assert_eq!(result.columns, vec!["snapshot"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows_affected, 3);

        let blob = match &result.rows[0][0] {
            Value::Bytes(b) => b.clone(),
            other => panic!("expected Bytes, got {other:?}"),
        };

        // Clear storage and restore into tenant 0.
        s.delete(Namespace::Kv, b"doc:1").await.unwrap();
        s.delete(Namespace::Kv, b"doc:2").await.unwrap();
        s.delete(Namespace::Meta, b"schema:v1").await.unwrap();

        let restore = handle_restore_tenant_snapshot(&s, 0, &blob).await.unwrap();
        assert_eq!(restore.rows_affected, 3);

        // Restored under t/0/ prefix.
        let val = s.get(Namespace::Kv, b"t/0/doc:1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"value1".as_slice()));
        let val2 = s.get(Namespace::Kv, b"t/0/doc:2").await.unwrap();
        assert_eq!(val2.as_deref(), Some(b"value2".as_slice()));
    }

    #[tokio::test]
    async fn snapshot_roundtrip_non_zero_tenant() {
        let s = make_storage().await;
        // Write an explicit tenant 42 key.
        s.put(Namespace::Strict, b"t/42/row:1", b"strict_data")
            .await
            .unwrap();

        let result = handle_create_tenant_snapshot(&s, 42).await.unwrap();
        assert_eq!(result.rows_affected, 1);
        let blob = match &result.rows[0][0] {
            Value::Bytes(b) => b.clone(),
            other => panic!("expected Bytes, got {other:?}"),
        };

        // Restore into tenant 99.
        let restore = handle_restore_tenant_snapshot(&s, 99, &blob).await.unwrap();
        assert_eq!(restore.rows_affected, 1);

        let val = s.get(Namespace::Strict, b"t/99/row:1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"strict_data".as_slice()));
    }

    #[tokio::test]
    async fn purge_tenant_removes_only_target_tenant() {
        let s = make_storage().await;
        // Tenant 0 legacy keys.
        s.put(Namespace::Kv, b"doc:1", b"v1").await.unwrap();
        s.put(Namespace::Kv, b"doc:2", b"v2").await.unwrap();
        // Tenant 1 key — must NOT be removed.
        s.put(Namespace::Kv, b"t/1/doc:3", b"v3").await.unwrap();

        let result = handle_purge_tenant(&s, 0).await.unwrap();
        assert_eq!(result.rows_affected, 2);

        assert!(s.get(Namespace::Kv, b"doc:1").await.unwrap().is_none());
        assert!(s.get(Namespace::Kv, b"doc:2").await.unwrap().is_none());
        // Tenant 1 key unaffected.
        assert!(s.get(Namespace::Kv, b"t/1/doc:3").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn purge_tenant_is_idempotent() {
        let s = make_storage().await;
        s.put(Namespace::Kv, b"x", b"y").await.unwrap();
        handle_purge_tenant(&s, 0).await.unwrap();

        let second = handle_purge_tenant(&s, 0).await.unwrap();
        assert_eq!(second.rows_affected, 0);
    }

    #[tokio::test]
    async fn empty_tenant_snapshot_roundtrip() {
        let s = make_storage().await;
        let result = handle_create_tenant_snapshot(&s, 7).await.unwrap();
        assert_eq!(result.rows_affected, 0);
        assert_eq!(result.rows.len(), 1);

        let blob = match &result.rows[0][0] {
            Value::Bytes(b) => b.clone(),
            other => panic!("expected Bytes, got {other:?}"),
        };

        let entries: Vec<SnapshotEntry> = zerompk::from_msgpack(&blob).unwrap();
        assert!(entries.is_empty());

        let restore = handle_restore_tenant_snapshot(&s, 7, &blob).await.unwrap();
        assert_eq!(restore.rows_affected, 0);
    }

    #[tokio::test]
    async fn tenant_zero_does_not_capture_other_tenants() {
        let s = make_storage().await;
        // Tenant 0 legacy key.
        s.put(Namespace::Kv, b"my_key", b"val0").await.unwrap();
        // Tenant 5 explicit key.
        s.put(Namespace::Kv, b"t/5/key", b"val5").await.unwrap();

        let result = handle_create_tenant_snapshot(&s, 0).await.unwrap();
        // Only tenant 0's key should be captured.
        assert_eq!(result.rows_affected, 1);
    }
}
