// SPDX-License-Identifier: Apache-2.0

//! Serialize [`FtsCollectionManager`](crate::engine::fts::FtsCollectionManager)
//! state into write ops, without I/O.

use std::collections::HashMap;

use nodedb_fts::backend::FtsBackend;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::{FtsIndex, IndexScope};
use nodedb_types::Namespace;
use nodedb_types::Surrogate;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::format::{
    COLLECTIONS_KEY, DB, FtsSurrogateState, LAYOUT_KEY, LAYOUT_PER_FIELD, META_SUBKEYS,
    PostingsBlob, SURROGATES_KEY, TID, compact_to_ser, doclens_key, meta_key, scoped_term_prefix,
};
use crate::storage::engine::WriteOp;

fn put(key: Vec<u8>, value: Vec<u8>) -> WriteOp {
    WriteOp::Put {
        ns: Namespace::Fts,
        key,
        value,
    }
}

fn to_msgpack<T: zerompk::ToMessagePack>(value: &T) -> NodeDbResult<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| NodeDbError::serialization("msgpack", e))
}

/// Serialize all term postings of `idx` into one msgpack blob. Each stored
/// term carries the [`scoped_term_prefix`] of `index_key`.
///
/// Returns `None` if the memtable has no terms (empty index — nothing to write).
fn serialize_postings_blob(
    index_key: &str,
    idx: &FtsIndex<MemoryBackend>,
) -> NodeDbResult<Option<Vec<u8>>> {
    let mt = idx.memtable();
    let scope = IndexScope::document(index_key);
    let prefix = scoped_term_prefix(index_key);
    let mut entries: PostingsBlob = Vec::new();

    for term in mt.terms(DB, TID, scope) {
        let postings = mt.get_postings(DB, TID, scope, &term);
        if postings.is_empty() {
            continue;
        }
        entries.push((
            format!("{prefix}{term}"),
            postings.iter().map(compact_to_ser).collect(),
        ));
    }

    if entries.is_empty() {
        return Ok(None);
    }
    to_msgpack(&entries).map(Some)
}

/// Collect KV `WriteOp`s for doc-lengths and meta blobs (always on B+ tree).
fn metadata_ops_for_index(
    index_key: &str,
    idx: &FtsIndex<MemoryBackend>,
    ops: &mut Vec<WriteOp>,
) -> NodeDbResult<()> {
    let mt = idx.memtable();
    let scope = IndexScope::document(index_key);

    // ── Doc lengths (per-doc lengths needed by BM25 scoring) ─────────────────
    let mut surrogates: Vec<u32> = mt
        .terms(DB, TID, scope)
        .iter()
        .flat_map(|t| {
            mt.get_postings(DB, TID, scope, t)
                .into_iter()
                .map(|p| p.doc_id.0)
        })
        .collect();
    surrogates.sort_unstable();
    surrogates.dedup();

    let mut doclens: Vec<(u32, u32)> = Vec::with_capacity(surrogates.len());
    for &s in &surrogates {
        if let Some(len) = idx
            .backend()
            .read_doc_length(DB, TID, scope, Surrogate(s))
            .map_err(|e| NodeDbError::storage(format!("fts doc_len: {e}")))?
        {
            doclens.push((s, len));
        }
    }
    if !doclens.is_empty() {
        ops.push(put(
            doclens_key(index_key).into_bytes(),
            to_msgpack(&doclens)?,
        ));
    }

    // ── Meta blobs (fieldnorms, analyzer, language) ───────────────────────────
    for &subkey in META_SUBKEYS {
        if let Some(data) = idx
            .backend()
            .read_meta(DB, TID, scope, subkey)
            .map_err(|e| NodeDbError::storage(format!("fts meta read: {e}")))?
        {
            ops.push(put(meta_key(index_key, subkey).into_bytes(), data));
        }
    }

    Ok(())
}

/// `(index_key, blob)` posting segments of one checkpoint.
type SegmentWrites = Vec<(String, Vec<u8>)>;

/// Serialize FTS state into write ops (no I/O, safe to call while holding a
/// mutex guard). Returns `(kv_ops, segment_writes)`. The caller writes
/// `segment_writes` through `FtsSegmentExt::write_fts_segment` when the
/// backend has one.
pub(crate) fn serialize_fts(
    indices: &HashMap<String, FtsIndex<MemoryBackend>>,
    id_to_surrogate: &HashMap<String, u32>,
    next_surrogate: u32,
) -> NodeDbResult<(Vec<WriteOp>, SegmentWrites)> {
    let mut ops: Vec<WriteOp> = Vec::new();
    let mut segment_writes: SegmentWrites = Vec::new();

    // ── Collection list, layout version, surrogate maps ──────────────────────
    let index_keys: Vec<String> = indices.keys().cloned().collect();
    ops.push(put(COLLECTIONS_KEY.to_vec(), to_msgpack(&index_keys)?));
    ops.push(put(LAYOUT_KEY.to_vec(), vec![LAYOUT_PER_FIELD]));
    let surrogate_state = FtsSurrogateState {
        id_to_surrogate: id_to_surrogate
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
        next_surrogate,
    };
    ops.push(put(SURROGATES_KEY.to_vec(), to_msgpack(&surrogate_state)?));

    // ── Per-index data ────────────────────────────────────────────────────────
    for (key, idx) in indices {
        // Always collect metadata ops (doc-lengths, meta blobs) onto B+ tree.
        metadata_ops_for_index(key, idx, &mut ops)?;

        // Collect posting data: dispatched to pagedb segments or unpacked
        // into per-term KV entries at write time. Indices with no terms
        // produce no segment write.
        if let Some(blob) = serialize_postings_blob(key, idx)? {
            segment_writes.push((key.clone(), blob));
        }
    }

    Ok((ops, segment_writes))
}
