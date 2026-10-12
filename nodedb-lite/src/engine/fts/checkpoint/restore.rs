// SPDX-License-Identifier: Apache-2.0

//! Restore a persisted FTS checkpoint on cold open.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_fts::backend::FtsBackend;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::{FtsIndex, IndexScope};
use nodedb_mem::MemoryGovernor;
use nodedb_types::Namespace;
use nodedb_types::Surrogate;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::format::{
    COLLECTIONS_KEY, DB, FtsSurrogateState, LAYOUT_KEY, LAYOUT_PER_FIELD, META_SUBKEYS,
    SURROGATES_KEY, SerPosting, TID, doclens_key, meta_key, postings_prefix, scoped_term_prefix,
    ser_to_compact,
};
use crate::engine::fts::manager::resident_index;
use crate::storage::engine::StorageEngine;

/// FTS state read back from a checkpoint.
#[derive(Default)]
pub(crate) struct RestoredFts {
    pub indices: HashMap<String, FtsIndex<MemoryBackend>>,
    pub id_to_surrogate: HashMap<String, u32>,
    pub surrogate_to_id: HashMap<u32, String>,
    pub next_surrogate: u32,
    /// `true` when the checkpoint was written with per-field indexes for
    /// schemaless documents. `false` for an older checkpoint, whose
    /// schemaless documents need re-indexing to gain them.
    pub per_field_layout: bool,
}

fn decode_err(what: &str, e: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::serialization("msgpack", format!("fts checkpoint {what}: {e}"))
}

fn backend_err(index_key: &str, e: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::storage(format!("fts checkpoint restore of '{index_key}': {e}"))
}

/// Load the postings of `index_key` into `idx`: from its pagedb segment when
/// one exists, else from the legacy per-term KV entries.
///
/// Stored terms carry the [`scoped_term_prefix`] of `index_key`. The
/// memtable holds bare terms, so the prefix is stripped on insert.
async fn restore_postings<S: StorageEngine>(
    storage: &S,
    index_key: &str,
    idx: &FtsIndex<MemoryBackend>,
) -> NodeDbResult<()> {
    let scope = IndexScope::document(index_key);
    let term_scope = scoped_term_prefix(index_key);

    #[cfg(not(target_arch = "wasm32"))]
    if let Some(seg_ext) = storage.as_fts_segment_ext()
        && let Some(blob) = seg_ext
            .open_fts_segment(index_key)
            .await
            .map_err(|e| backend_err(index_key, e))?
    {
        let entries = zerompk::from_msgpack::<super::format::PostingsBlob>(&blob)
            .map_err(|e| decode_err(&format!("segment of '{index_key}'"), e))?;
        for (scoped_term, postings) in entries {
            // A segment belongs to one index, so a term without the prefix
            // is already bare.
            let term = scoped_term
                .strip_prefix(term_scope.as_str())
                .unwrap_or(&scoped_term);
            for sp in postings {
                idx.memtable()
                    .insert(DB, TID, scope, term, ser_to_compact(sp));
            }
        }
        return Ok(());
    }

    // A posting key holds a term scoped to this index. Keys of an index
    // whose name extends this one (`"{key}:mt…"`) share the key prefix but
    // not the term prefix, and are skipped.
    let prefix = postings_prefix(index_key);
    for (raw_key, value) in storage
        .scan_prefix(Namespace::Fts, prefix.as_bytes())
        .await?
    {
        let scoped_term = std::str::from_utf8(&raw_key)
            .ok()
            .and_then(|k| k.strip_prefix(&prefix))
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                decode_err(
                    &format!("posting key of '{index_key}'"),
                    "not a UTF-8 term key",
                )
            })?;
        let Some(term) = scoped_term.strip_prefix(term_scope.as_str()) else {
            continue;
        };
        let postings = zerompk::from_msgpack::<Vec<SerPosting>>(&value)
            .map_err(|e| decode_err(&format!("postings of '{scoped_term}'"), e))?;
        for sp in postings {
            idx.memtable()
                .insert(DB, TID, scope, term, ser_to_compact(sp));
        }
    }
    Ok(())
}

/// Restore FTS state from storage on cold open.
///
/// `governor` is bound into every restored [`FtsIndex`] for memory accounting.
///
/// Returns an empty state if no checkpoint is found. Fails when any part of
/// the checkpoint cannot be read or decoded: a partial restore would serve
/// an index silently missing documents.
pub(crate) async fn restore_fts<S>(
    storage: &S,
    governor: Arc<MemoryGovernor>,
) -> NodeDbResult<RestoredFts>
where
    S: StorageEngine,
{
    // ── Read collection list ──────────────────────────────────────────────────
    let Some(keys_bytes) = storage.get(Namespace::Fts, COLLECTIONS_KEY).await? else {
        return Ok(RestoredFts::default());
    };
    let mut index_keys = zerompk::from_msgpack::<Vec<String>>(&keys_bytes)
        .map_err(|e| decode_err("index key list", e))?;

    let per_field_layout = storage
        .get(Namespace::Fts, LAYOUT_KEY)
        .await?
        .is_some_and(|v| v.first().copied() == Some(LAYOUT_PER_FIELD));
    if !per_field_layout {
        // An older layout kept the whole-document index under
        // `"{collection}:_doc"`. The re-index that follows an outdated
        // restore rebuilds it under the current key, so the old one is not
        // loaded: it would pose as a field named `_doc`.
        index_keys.retain(|k| !k.ends_with(":_doc"));
    }
    if index_keys.is_empty() {
        return Ok(RestoredFts::default());
    }

    // ── Read surrogate maps ───────────────────────────────────────────────────
    let surrogate_bytes = storage
        .get(Namespace::Fts, SURROGATES_KEY)
        .await?
        .ok_or_else(|| decode_err("surrogate maps", "missing"))?;
    let state = zerompk::from_msgpack::<FtsSurrogateState>(&surrogate_bytes)
        .map_err(|e| decode_err("surrogate maps", e))?;
    let mut id_to_surrogate: HashMap<String, u32> =
        HashMap::with_capacity(state.id_to_surrogate.len());
    let mut surrogate_to_id: HashMap<u32, String> =
        HashMap::with_capacity(state.id_to_surrogate.len());
    for (id, s) in state.id_to_surrogate {
        surrogate_to_id.insert(s, id.clone());
        id_to_surrogate.insert(id, s);
    }

    // ── Restore per-index data ────────────────────────────────────────────────
    let mut indices: HashMap<String, FtsIndex<MemoryBackend>> =
        HashMap::with_capacity(index_keys.len());

    for index_key in &index_keys {
        let scope = IndexScope::document(index_key);
        let idx = resident_index(Arc::clone(&governor));
        restore_postings(storage, index_key, &idx).await?;

        // ── Doc lengths (always on B+ tree) ──────────────────────────────────
        if let Some(data) = storage
            .get(Namespace::Fts, doclens_key(index_key).as_bytes())
            .await?
        {
            let pairs = zerompk::from_msgpack::<Vec<(u32, u32)>>(&data)
                .map_err(|e| decode_err(&format!("doc lengths of '{index_key}'"), e))?;
            for (s, len) in pairs {
                idx.backend()
                    .write_doc_length(DB, TID, scope, Surrogate(s), len)
                    .map_err(|e| backend_err(index_key, e))?;
                idx.backend()
                    .increment_stats(DB, TID, scope, len)
                    .map_err(|e| backend_err(index_key, e))?;
            }
        }

        // ── Meta blobs (always on B+ tree) ────────────────────────────────────
        for &subkey in META_SUBKEYS {
            if let Some(data) = storage
                .get(Namespace::Fts, meta_key(index_key, subkey).as_bytes())
                .await?
            {
                idx.backend()
                    .write_meta(DB, TID, scope, subkey, &data)
                    .map_err(|e| backend_err(index_key, e))?;
            }
        }

        indices.insert(index_key.clone(), idx);
    }

    tracing::debug!(
        index_count = indices.len(),
        surrogate_count = id_to_surrogate.len(),
        per_field_layout,
        "fts checkpoint restored"
    );

    Ok(RestoredFts {
        indices,
        id_to_surrogate,
        surrogate_to_id,
        next_surrogate: state.next_surrogate,
        per_field_layout,
    })
}
