// SPDX-License-Identifier: Apache-2.0

//! Lite identity and CRDT state restore.

use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::CrdtEngine;
use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::{
    META_CRDT_DELTAS, META_CSR_LEGACY, META_LAST_FLUSHED_MID, NodeDbLite,
};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Restore Lite identity (lite_id + epoch) and CRDT state.
    ///
    /// Loads or creates the Lite identity, restores per-collection CRDT
    /// snapshots, backfills the registered-collection set and `LatestVersion`
    /// index from persisted bitemporal flags, restores pending deltas, checks
    /// partial-flush safety, and deletes the legacy single-CSR checkpoint if
    /// present.
    pub(in crate::nodedb::core::open) async fn restore_identity_and_crdt(
        storage: &Arc<S>,
    ) -> NodeDbResult<(CrdtEngine, crate::identity::LiteIdentity)> {
        // ── Load or create Lite identity (lite_id + epoch + peer id) ──
        //
        // This must happen before any outbound sync so the handshake carries a
        // non-empty lite_id and epoch ≥ 1, enabling Origin's idempotent-producer
        // gate. The epoch is incremented on every open, so a new process
        // incarnation fences out writes from the previous one. The peer id
        // comes from the same record, which is what binds the identity every
        // local operation is authored under to the store holding them.
        let lite_identity = crate::identity::LiteIdentity::load_or_create(&**storage)
            .await
            .map_err(|e| {
                // Preserve corruption typing so a corrupt identity read is
                // routed to the post-open recovery driver rather than
                // crash-looping as a generic storage error.
                let detail = format!("lite identity load failed: {e}");
                if crate::error::is_corruption(&e) {
                    NodeDbError::segment_corrupted(detail)
                } else {
                    NodeDbError::storage(detail)
                }
            })?;

        // ── Restore CRDT state, one Loro document per collection ──
        // Snapshots are stored under `loro_snapshot:<collection>`, so the whole
        // set is recovered with a single prefix scan. A collection whose
        // snapshot fails its CRC32C check is dropped individually — the other
        // collections stay intact instead of the whole engine resetting.
        let mut crdt = CrdtEngine::new(lite_identity.peer_id)
            .map_err(|e| NodeDbError::storage(format!("CRDT init failed: {e}")))?;
        let snapshot_entries = storage
            .scan_prefix(Namespace::LoroState, CrdtEngine::snapshot_key_prefix())
            .await?;
        let mut base_bytes: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (key, envelope) in &snapshot_entries {
            let Some(collection) = CrdtEngine::collection_from_snapshot_key(key) else {
                tracing::error!(
                    "CRDT snapshot key is not a valid `loro_snapshot:<collection>` entry — \
                     skipping; its collection cannot be determined without guessing."
                );
                continue;
            };
            match crate::storage::checksum::unwrap(envelope) {
                Some(snapshot) => {
                    base_bytes.insert(collection.to_string(), snapshot.len());
                    crdt.import_snapshot(collection, &snapshot).map_err(|e| {
                        NodeDbError::storage(format!("CRDT restore of '{collection}' failed: {e}"))
                    })?;
                }
                None => {
                    tracing::error!(
                        collection = %collection,
                        "CRDT snapshot CRC32C mismatch — discarding corrupted snapshot for this \
                         collection. A full re-sync from Origin is needed for it."
                    );
                    // Delete the corrupted snapshot so we don't re-read it.
                    let _ = storage.delete(Namespace::LoroState, key).await;
                }
            }
        }

        // ── Replay the incremental updates written on top of each snapshot ──
        // Flush writes a full snapshot only periodically; between checkpoints it
        // appends `loro_delta:<collection>:<seq>` entries. A prefix scan returns
        // them in key order, which the zero-padded sequence makes replay order.
        // Skipping this loop would silently roll a collection back to its last
        // checkpoint, so a corrupt entry is an error rather than a warning.
        let mut delta_bytes: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut next_delta_seq: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let delta_entries = storage
            .scan_prefix(Namespace::LoroState, CrdtEngine::state_delta_key_prefix())
            .await?;
        for (key, envelope) in &delta_entries {
            let Some((collection, seq)) = CrdtEngine::state_delta_from_key(key) else {
                tracing::error!(
                    "CRDT update key is not a valid `loro_delta:<collection>:<seq>` entry — \
                     skipping; its collection cannot be determined without guessing."
                );
                continue;
            };
            let Some(update) = crate::storage::checksum::unwrap(envelope) else {
                return Err(NodeDbError::segment_corrupted(format!(
                    "CRDT update '{collection}' #{seq} failed its CRC32C check; the writes it \
                     carries are not in the snapshot behind it, so opening without it would \
                     silently roll the collection back"
                )));
            };
            delta_bytes
                .entry(collection.to_string())
                .and_modify(|n| *n += update.len())
                .or_insert(update.len());
            next_delta_seq.insert(collection.to_string(), seq + 1);
            crdt.import_snapshot(collection, &update).map_err(|e| {
                NodeDbError::storage(format!(
                    "CRDT update replay for '{collection}' #{seq} failed: {e}"
                ))
            })?;
        }

        // Seed the checkpoint accounting from what was on disk, so the first
        // flush after open does not rewrite a base that is already current.
        for (collection, base) in &base_bytes {
            let Some(version) = crdt.state(collection).map(|s| s.oplog_version_vector()) else {
                continue;
            };
            crdt.adopt_persisted_state(
                collection,
                version,
                *base,
                delta_bytes.get(collection).copied().unwrap_or(0),
                next_delta_seq.get(collection).copied().unwrap_or(0),
            );
        }

        // Rebuild the CRDT's registered-collection set from persisted bitemporal
        // flags so that SELECT queries on bitemporal collections work immediately
        // after open, even for collections with no inserted documents yet.
        // Also backfill the LatestVersion index for collections written before
        // the index was introduced — safe on fresh DBs and idempotent otherwise.
        const BITEMPORAL_PREFIX: &[u8] = b"document_bitemporal:";
        let bitemporal_entries = storage
            .scan_prefix(Namespace::Meta, BITEMPORAL_PREFIX)
            .await
            .unwrap_or_default();
        for (key, value) in &bitemporal_entries {
            // Only process collections where the flag byte is 0x01 (enabled).
            if value.first().copied() != Some(1) {
                continue;
            }
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(name) = key_str.strip_prefix("document_bitemporal:")
            {
                crdt.register_collection(name);

                if let Err(e) = crate::engine::document::history::ops::backfill_latest_version(
                    storage.as_ref(),
                    name,
                )
                .await
                {
                    tracing::warn!(
                        collection = name,
                        error = %e,
                        "LatestVersion backfill failed — bitemporal reads will \
                         fall back to prefix scan for this collection"
                    );
                }
            }
        }

        // Restore pending deltas — prefer incremental entries over legacy bulk blob.
        let incremental_entries = storage.scan_prefix(Namespace::Crdt, b"delta:").await?;

        if !incremental_entries.is_empty() {
            // Use incremental entries (append-only format).
            crdt.restore_pending_deltas_incremental(&incremental_entries);
        } else if let Some(delta_bytes) = storage.get(Namespace::Crdt, META_CRDT_DELTAS).await? {
            // Fall back to legacy bulk blob.
            crdt.restore_pending_deltas(&delta_bytes);
        }

        // Partial flush safety: check if the last-flushed mutation_id matches.
        if crdt.pending_count() > 0
            && let Some(last_flushed_bytes) =
                storage.get(Namespace::Meta, META_LAST_FLUSHED_MID).await?
            && last_flushed_bytes.len() == 8
        {
            let last_flushed = u64::from_le_bytes(last_flushed_bytes.try_into().unwrap_or([0; 8]));
            let max_pending = crdt
                .pending_deltas()
                .iter()
                .map(|d| d.mutation_id)
                .max()
                .unwrap_or(0);

            if max_pending > 0 && last_flushed > 0 && max_pending != last_flushed {
                tracing::warn!(
                    last_flushed,
                    max_pending,
                    "partial flush detected — pending deltas may be inconsistent. \
                     Clearing pending queue; CRDT state is authoritative."
                );
                crdt.clear_pending_deltas();
            }
        }

        // ── Delete legacy single-CSR checkpoint if present ──
        if storage
            .get(Namespace::Graph, META_CSR_LEGACY)
            .await?
            .is_some()
        {
            let _ = storage.delete(Namespace::Graph, META_CSR_LEGACY).await;
        }

        Ok((crdt, lite_identity))
    }
}
