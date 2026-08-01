// SPDX-License-Identifier: BUSL-1.1

//! What each flush writes for the CRDT layer: an incremental update in the
//! common case, a full snapshot only when the updates have earned one.
//!
//! A Loro snapshot export costs O(document). Writing one per flush therefore
//! costs the size of the whole collection every `auto_flush_ms`, whatever the
//! write rate was — the term that produced both unbounded file growth and a
//! flush that held the reader-visible lock for all wall time. An update export
//! costs O(new operations), so the same tick under the same load writes what
//! actually changed.
//!
//! The base snapshot is still rewritten periodically. Restore replays every
//! delta written since the base, so letting deltas accumulate without bound
//! moves the cost from flush to open. Rewriting once the deltas reach a
//! fraction of the base keeps that replay bounded by roughly the same
//! fraction, and amortises the O(document) export over the writes that made it
//! necessary.

use super::types::{CrdtEngine, DELTA_CHECKPOINT_MIN_BYTES, DELTA_CHECKPOINT_RATIO};
use crate::error::LiteError;

/// One CRDT write a flush must perform, with the frontier it covers.
pub struct CrdtWrite {
    /// Collection this write belongs to.
    pub collection: String,
    /// Whether this is a fresh base snapshot or an update on top of one.
    pub kind: CrdtWriteKind,
    /// Payload, unwrapped — the caller adds whatever framing storage needs.
    pub bytes: Vec<u8>,
    /// Frontier the payload was exported at. Report it back through
    /// [`CrdtEngine::mark_persisted`] once the write has committed.
    pub version: loro::VersionVector,
}

/// Which of the two shapes a [`CrdtWrite`] carries.
#[derive(Clone, Copy, Debug)]
pub enum CrdtWriteKind {
    /// A full snapshot that supersedes the base and the `superseded_deltas`
    /// deltas written on top of it. Those must be deleted in the same batch,
    /// or a later restore replays updates the new base already contains.
    Checkpoint { superseded_deltas: u64 },
    /// An update from the previously persisted frontier, stored under `seq`.
    Delta { seq: u64 },
}

/// A committed [`CrdtWrite`], reported back so the engine can advance its
/// bookkeeping. Carries the payload's length rather than the payload.
pub struct CrdtPersisted {
    pub collection: String,
    pub kind: CrdtWriteKind,
    pub bytes: usize,
    pub version: loro::VersionVector,
}

impl CrdtWrite {
    /// Describe this write without holding on to its payload.
    pub fn persisted(&self) -> CrdtPersisted {
        CrdtPersisted {
            collection: self.collection.clone(),
            kind: self.kind,
            bytes: self.bytes.len(),
            version: self.version.clone(),
        }
    }
}

impl CrdtEngine {
    /// Decide what every collection needs written and export it.
    ///
    /// A collection whose oplog frontier has not moved since its last
    /// persisted write is absent from the result entirely, so an idle store
    /// exports nothing. A collection that has moved yields either an update
    /// since that frontier, or — when it has no base yet, or its accumulated
    /// deltas have reached [`DELTA_CHECKPOINT_RATIO`] of the base — a fresh
    /// full snapshot.
    ///
    /// Pass the committed writes back through [`Self::mark_persisted`]; until
    /// then the engine still considers them outstanding, so a failed batch is
    /// retried rather than silently skipped.
    pub fn plan_persistence(&self) -> Result<Vec<CrdtWrite>, LiteError> {
        let mut out = Vec::new();
        for (collection, state) in &self.states {
            let version = state.oplog_version_vector();
            let persisted = self.flushed_versions.get(collection);
            if persisted == Some(&version) {
                continue;
            }

            let (kind, bytes) = match persisted {
                Some(from) if !self.checkpoint_is_due(collection) => {
                    let bytes =
                        state
                            .export_updates_since(from)
                            .map_err(|e| LiteError::Storage {
                                detail: format!("delta export for '{collection}' failed: {e}"),
                            })?;
                    let seq = self.next_delta_seq.get(collection).copied().unwrap_or(0);
                    (CrdtWriteKind::Delta { seq }, bytes)
                }
                _ => {
                    let superseded_deltas =
                        self.next_delta_seq.get(collection).copied().unwrap_or(0);
                    let bytes = self.export_one(collection, state)?;
                    (CrdtWriteKind::Checkpoint { superseded_deltas }, bytes)
                }
            };

            out.push(CrdtWrite {
                collection: collection.clone(),
                kind,
                bytes,
                version,
            });
        }
        Ok(out)
    }

    /// Advance the bookkeeping for writes that are now durable.
    ///
    /// Call only after the batch has committed — see [`Self::plan_persistence`].
    pub fn mark_persisted(&mut self, persisted: impl IntoIterator<Item = CrdtPersisted>) {
        for entry in persisted {
            match entry.kind {
                CrdtWriteKind::Checkpoint { .. } => {
                    self.checkpoint_bytes
                        .insert(entry.collection.clone(), entry.bytes);
                    self.delta_bytes.insert(entry.collection.clone(), 0);
                    self.next_delta_seq.insert(entry.collection.clone(), 0);
                }
                CrdtWriteKind::Delta { seq } => {
                    *self
                        .delta_bytes
                        .entry(entry.collection.clone())
                        .or_insert(0) += entry.bytes;
                    self.next_delta_seq
                        .insert(entry.collection.clone(), seq + 1);
                }
            }
            self.flushed_versions
                .insert(entry.collection, entry.version);
        }
    }

    /// Seed the bookkeeping from what restore found on disk, so the first
    /// flush after an open does not rewrite a base that is already current.
    pub fn adopt_persisted_state(
        &mut self,
        collection: &str,
        version: loro::VersionVector,
        checkpoint_bytes: usize,
        delta_bytes: usize,
        next_delta_seq: u64,
    ) {
        self.flushed_versions
            .insert(collection.to_string(), version);
        self.checkpoint_bytes
            .insert(collection.to_string(), checkpoint_bytes);
        self.delta_bytes.insert(collection.to_string(), delta_bytes);
        self.next_delta_seq
            .insert(collection.to_string(), next_delta_seq);
    }

    /// Whether this collection's accumulated deltas have grown enough to be
    /// worth folding back into the base.
    ///
    /// The floor matters as much as the ratio: a fraction of a small document
    /// is a few hundred bytes, which would put us back to a full rewrite per
    /// flush for exactly the collections where the delta path is cheapest.
    fn checkpoint_is_due(&self, collection: &str) -> bool {
        let Some(&base) = self.checkpoint_bytes.get(collection) else {
            return true;
        };
        let accumulated = self.delta_bytes.get(collection).copied().unwrap_or(0);
        accumulated >= (base / DELTA_CHECKPOINT_RATIO).max(DELTA_CHECKPOINT_MIN_BYTES)
    }
}
