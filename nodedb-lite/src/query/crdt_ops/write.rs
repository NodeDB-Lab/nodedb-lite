// SPDX-License-Identifier: Apache-2.0
//! CRDT write, policy-set, and delta-apply handlers.

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Apply a remote CRDT delta from another peer.
///
/// Imports the raw Loro delta bytes, then acknowledges the mutation on
/// success or rejects it on import failure.
pub async fn handle_apply<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    delta: &[u8],
    mutation_id: u64,
) -> Result<QueryResult, LiteError> {
    let result = {
        let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        crdt.import_remote(delta)
    };

    match result {
        Ok(()) => {
            let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
            crdt.acknowledge(mutation_id);
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 1,
            })
        }
        Err(import_err) => {
            let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
            crdt.reject_delta(mutation_id);
            Err(import_err)
        }
    }
}

/// Import a per-collection Loro snapshot (durable RESTORE re-issue path).
///
/// A snapshot is just a Loro-encoded update set scoped to one collection's
/// container; `CrdtState::import` (aka `import_remote`) is a monotonic,
/// idempotent, commutative merge that resolves the target container by name
/// from the encoded bytes themselves (see `CrdtState`'s container-naming
/// doc comment), so re-using the same import path used for remote deltas is
/// correct here too — no per-collection document split is needed on Lite.
pub async fn handle_import_snapshot<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    bytes: &[u8],
) -> Result<QueryResult, LiteError> {
    let crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.import_remote(bytes)?;
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}

/// Set the conflict resolution policy for a CRDT collection.
pub async fn handle_set_policy<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    policy_json: &str,
) -> Result<QueryResult, LiteError> {
    let policy: nodedb_crdt::CollectionPolicy =
        sonic_rs::from_str(policy_json).map_err(|e| LiteError::BadRequest {
            detail: format!("invalid CollectionPolicy JSON: {e}"),
        })?;

    let mut crdt = engine.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
    crdt.set_policy(collection, policy);

    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 1,
    })
}
