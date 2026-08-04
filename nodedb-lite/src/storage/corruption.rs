// SPDX-License-Identifier: Apache-2.0

//! What to do when opening a store that cannot be read.
//!
//! An embedded database is frequently the only copy of its data. Whether a
//! damaged store can be thrown away and rebuilt depends on something only the
//! embedder knows — whether an Origin exists to re-sync from, whether the data
//! is reproducible, whether starting empty is worse than not starting. So the
//! library reports the fault and stops; discarding is a decision the caller
//! makes, in advance, by name.
//!
//! Note that "does this store sync?" is not something the library can infer at
//! open time. `LiteConfig::sync_enabled` governs whether KV writes flow through
//! Loro, not whether an Origin has ever been reachable, and `start_sync` is
//! called after open in any case. Treating either as consent to discard would
//! reintroduce exactly the silent decision this type exists to prevent.

use serde::{Deserialize, Serialize};

/// How [`open`](crate::storage::pagedb_storage::PagedbStorage) reacts to a
/// corruption-class fault in the store it was asked to open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorruptionPolicy {
    /// Report the corruption and leave every byte where it is. The default.
    ///
    /// Open returns a corruption-class error the caller can match on
    /// (`ErrorDetails::SegmentCorrupted`). Nothing is renamed, deleted, or
    /// created, so the store is in the same state after the failed open as
    /// before it — available for `pagedb-fsck`, for a byte-for-byte backup, or
    /// for a later open under a different policy.
    #[default]
    FailClosed,

    /// Rename the damaged store aside and continue against a fresh, empty one.
    ///
    /// The old store is moved to `{path}.corrupt.{unix_secs}` and never
    /// deleted, but a caller opting into this is choosing to start empty: the
    /// database that comes back has none of the previous data in it, and any
    /// writes it accepts diverge from the copy set aside. Only meaningful when
    /// the caller has another source of truth to refill from.
    DiscardStoreAndRecreate,
}

impl CorruptionPolicy {
    /// Whether this policy permits destroying or replacing stored bytes.
    pub(crate) fn may_discard(self) -> bool {
        matches!(self, CorruptionPolicy::DiscardStoreAndRecreate)
    }
}
