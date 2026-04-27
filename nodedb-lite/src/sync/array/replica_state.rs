//! Persistent replica identity and HLC generator for the array CRDT sync subsystem.
//!
//! [`ReplicaState`] is loaded once at [`NodeDbLite::open`] and shared across
//! the sync subsystem via [`Arc`]. It combines the stable [`ReplicaId`]
//! (persisted under `Namespace::Meta` key `b"array.replica_id"`) with a
//! monotonic [`HlcGenerator`] seeded from that identity.

use std::sync::Arc;

use nodedb_array::sync::hlc::{Hlc, HlcGenerator};
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;

/// Storage key for the persistent replica id.
const REPLICA_ID_KEY: &[u8] = b"array.replica_id";

/// Persistent replica identity + monotonic HLC generator.
///
/// Constructed once at database open. Multiple subsystems share an
/// `Arc<ReplicaState>` so they all mint HLCs from the same generator.
pub struct ReplicaState {
    replica_id: ReplicaId,
    hlc_gen: Arc<HlcGenerator>,
}

impl ReplicaState {
    /// Load the [`ReplicaId`] from `Namespace::Meta` key `b"array.replica_id"`.
    ///
    /// If no stored id is found, generates a fresh UUID-v7-derived [`ReplicaId`]
    /// and persists it before returning. On subsequent opens the same id is
    /// returned.
    pub fn load_or_init<S: StorageEngineSync>(storage: &S) -> Result<Self, LiteError> {
        let existing = storage.get_sync(Namespace::Meta, REPLICA_ID_KEY)?;

        let replica_id = if let Some(bytes) = existing {
            if bytes.len() != 8 {
                return Err(LiteError::Storage {
                    detail: format!("array.replica_id: expected 8 bytes, got {}", bytes.len()),
                });
            }
            let arr: [u8; 8] = bytes.try_into().map_err(|_| LiteError::Storage {
                detail: "array.replica_id: byte conversion failed".into(),
            })?;
            ReplicaId::new(u64::from_be_bytes(arr))
        } else {
            let id = ReplicaId::generate();
            storage.put_sync(Namespace::Meta, REPLICA_ID_KEY, &id.as_u64().to_be_bytes())?;
            id
        };

        let hlc_gen = Arc::new(HlcGenerator::new(replica_id));
        Ok(Self {
            replica_id,
            hlc_gen,
        })
    }

    /// The stable 64-bit replica identity.
    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    /// The shared [`HlcGenerator`] for this replica.
    pub fn hlc_gen(&self) -> Arc<HlcGenerator> {
        Arc::clone(&self.hlc_gen)
    }

    /// Generate the next monotonic [`Hlc`].
    ///
    /// Delegates to the internal [`HlcGenerator`]. Maps [`ArrayError`] into
    /// [`LiteError::Storage`].
    pub fn next_hlc(&self) -> Result<Hlc, LiteError> {
        self.hlc_gen.next().map_err(|e| LiteError::Storage {
            detail: format!("array sync HLC: {e}"),
        })
    }

    /// Observe a remote [`Hlc`] and advance the local generator past it.
    ///
    /// Called when receiving any op or ack from a remote replica so that
    /// subsequent locally minted HLCs are causally greater.
    pub fn observe(&self, remote: Hlc) -> Result<(), LiteError> {
        self.hlc_gen
            .observe(remote)
            .map_err(|e| LiteError::Storage {
                detail: format!("array sync HLC observe: {e}"),
            })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;

    fn open_storage() -> Arc<RedbStorage> {
        Arc::new(RedbStorage::open_in_memory().unwrap())
    }

    #[test]
    fn load_or_init_generates_new_on_empty() {
        let storage = open_storage();
        let state = ReplicaState::load_or_init(&*storage).unwrap();
        // ReplicaId must be non-zero (UUID-v7 low 64 bits is never 0 in practice).
        // More importantly, it must be persisted.
        let bytes = storage
            .get_sync(Namespace::Meta, REPLICA_ID_KEY)
            .unwrap()
            .expect("replica_id must be persisted");
        assert_eq!(bytes.len(), 8);
        let stored = u64::from_be_bytes(bytes.try_into().unwrap());
        assert_eq!(stored, state.replica_id().as_u64());
    }

    #[test]
    fn load_or_init_returns_same_on_reload() {
        let storage = open_storage();
        let id1 = ReplicaState::load_or_init(&*storage).unwrap().replica_id();
        let id2 = ReplicaState::load_or_init(&*storage).unwrap().replica_id();
        assert_eq!(id1, id2, "reload must return the same replica_id");
    }

    #[test]
    fn next_hlc_monotonic() {
        let storage = open_storage();
        let state = ReplicaState::load_or_init(&*storage).unwrap();
        let mut prev = state.next_hlc().unwrap();
        for _ in 0..99 {
            let curr = state.next_hlc().unwrap();
            assert!(
                curr > prev,
                "HLC must be strictly increasing: {curr:?} <= {prev:?}"
            );
            prev = curr;
        }
    }

    #[test]
    fn observe_advances_clock() {
        let storage = open_storage();
        let state = ReplicaState::load_or_init(&*storage).unwrap();

        let baseline = state.next_hlc().unwrap();
        // Synthesise a remote HLC far in the future.
        let future_replica = ReplicaId::new(0xdead);
        let future_hlc = Hlc::new(baseline.physical_ms + 100_000, 0, future_replica).unwrap();

        state.observe(future_hlc).unwrap();

        let next = state.next_hlc().unwrap();
        assert!(
            next > future_hlc,
            "local HLC after observe must exceed observed remote: {next:?} <= {future_hlc:?}"
        );
    }
}
