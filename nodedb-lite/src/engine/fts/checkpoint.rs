//! Checkpoint serialization and restoration for [`FtsCollectionManager`].
//!
//! Persists the full in-memory FTS state so that a cold open can load the
//! index without re-tokenizing source documents.
//!
//! ## Storage layout
//!
//! ### B+ tree (`Namespace::Fts`) — always used
//!
//! | Key                             | Value                                      |
//! |---------------------------------|--------------------------------------------|
//! | `fts:_collections`              | MessagePack `Vec<String>` — index key list |
//! | `fts:_surrogates`               | MessagePack `FtsSurrogateState`            |
//! | `fts:{index_key}:doclens`       | MessagePack `Vec<(u32,u32)>` — surrogate/len |
//! | `fts:{index_key}:meta:{subkey}` | raw bytes (fieldnorms/analyzer/language)   |
//!
//! ### pagedb segments — used when `as_fts_segment_ext()` returns `Some`
//!
//! | Segment name          | Value                                            |
//! |-----------------------|--------------------------------------------------|
//! | `fts/seg/{index_key}` | MessagePack `Vec<(String, Vec<SerPosting>)>`     |
//!
//! When pagedb segments are unavailable (WASM / legacy backends), posting data
//! falls back to the legacy KV path:
//!
//! | Key                               | Value                              |
//! |-----------------------------------|------------------------------------|
//! | `fts:{index_key}:mt:{scoped_term}`| MessagePack `Vec<SerPosting>`      |
//! | `fts:{index_key}:mtstat`          | MessagePack `(u32, u64)` (unused)  |
//!
//! ## Rationale: memtable vs segment storage
//!
//! `nodedb-fts` on Lite uses `MemoryBackend` exclusively.  All postings live in
//! a `Memtable`; the backend's LSM segment layer is unused.  The pagedb segment
//! path bundles all per-term posting entries for one index key into a single
//! segment blob, reducing B+ tree pressure from O(vocab_size) entries to O(1)
//! per index key.

use std::collections::HashMap;

use nodedb_fts::FtsIndex;
use nodedb_fts::backend::FtsBackend;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::block::CompactPosting;
use nodedb_types::Namespace;
use nodedb_types::Surrogate;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use serde::{Deserialize, Serialize};

use crate::storage::engine::{StorageEngine, WriteOp};

/// Surrogate maps persisted alongside posting data.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) struct FtsSurrogateState {
    /// `doc_id` string → dense u32 surrogate.
    pub id_to_surrogate: Vec<(String, u32)>,
    /// Next surrogate to assign.
    pub next_surrogate: u32,
}

/// Known meta subkeys written by `nodedb-fts`.
const META_SUBKEYS: &[&str] = &["fieldnorms", "analyzer", "language"];

/// A single memtable posting entry serialized as a flat tuple.
///
/// Matches `CompactPosting` fields: `(doc_id, term_freq, fieldnorm, positions)`.
type SerPosting = (u32, u32, u8, Vec<u32>);

fn compact_to_ser(p: &CompactPosting) -> SerPosting {
    (p.doc_id.0, p.term_freq, p.fieldnorm, p.positions.clone())
}

fn ser_to_compact(s: SerPosting) -> CompactPosting {
    CompactPosting {
        doc_id: Surrogate(s.0),
        term_freq: s.1,
        fieldnorm: s.2,
        positions: s.3,
    }
}

/// Serialize all term postings for `index_key` into a single msgpack blob.
///
/// Returns `None` if the memtable has no terms (empty index — nothing to write).
fn serialize_postings_blob(
    _index_key: &str,
    idx: &FtsIndex<MemoryBackend>,
) -> NodeDbResult<Option<Vec<u8>>> {
    let mt = idx.memtable();
    let mut entries: Vec<(String, Vec<SerPosting>)> = Vec::new();

    for scoped_term in mt.terms() {
        let postings = mt.get_postings(&scoped_term);
        if postings.is_empty() {
            continue;
        }
        let ser: Vec<SerPosting> = postings.iter().map(compact_to_ser).collect();
        entries.push((scoped_term, ser));
    }

    if entries.is_empty() {
        return Ok(None);
    }

    let bytes =
        zerompk::to_msgpack_vec(&entries).map_err(|e| NodeDbError::serialization("msgpack", e))?;
    Ok(Some(bytes))
}

/// Collect KV `WriteOp`s for doc-lengths and meta blobs (always on B+ tree).
fn metadata_ops_for_index(
    index_key: &str,
    idx: &FtsIndex<MemoryBackend>,
    ops: &mut Vec<WriteOp>,
) -> NodeDbResult<()> {
    const TID: u64 = 0;
    let mt = idx.memtable();

    // ── Doc lengths (per-doc lengths needed by BM25 scoring) ─────────────────
    let mut surrogates: Vec<u32> = mt
        .terms()
        .iter()
        .flat_map(|t| mt.get_postings(t).into_iter().map(|p| p.doc_id.0))
        .collect();
    surrogates.sort_unstable();
    surrogates.dedup();

    let mut doclens: Vec<(u32, u32)> = Vec::with_capacity(surrogates.len());
    for &s in &surrogates {
        if let Some(len) = idx
            .backend()
            .read_doc_length(TID, index_key, Surrogate(s))
            .map_err(|e| NodeDbError::storage(format!("fts doc_len: {e}")))?
        {
            doclens.push((s, len));
        }
    }
    if !doclens.is_empty() {
        let doclens_key = format!("fts:{index_key}:doclens");
        let bytes = zerompk::to_msgpack_vec(&doclens)
            .map_err(|e| NodeDbError::serialization("msgpack", e))?;
        ops.push(WriteOp::Put {
            ns: Namespace::Fts,
            key: doclens_key.into_bytes(),
            value: bytes,
        });
    }

    // ── Meta blobs (fieldnorms, analyzer, language) ───────────────────────────
    for &subkey in META_SUBKEYS {
        if let Some(data) = idx
            .backend()
            .read_meta(TID, index_key, subkey)
            .map_err(|e| NodeDbError::storage(format!("fts meta read: {e}")))?
        {
            let meta_key = format!("fts:{index_key}:meta:{subkey}");
            ops.push(WriteOp::Put {
                ns: Namespace::Fts,
                key: meta_key.into_bytes(),
                value: data,
            });
        }
    }

    Ok(())
}

/// Serialize FTS state into write ops (no I/O, safe to call while holding a
/// mutex guard).  Returns `(kv_ops, segment_writes)` where `segment_writes`
/// is a list of `(index_key, blob)` tuples that should be written via
/// `FtsSegmentExt::write_fts_segment` if available.
#[allow(clippy::type_complexity)]
pub(crate) fn serialize_fts(
    indices: &HashMap<String, FtsIndex<MemoryBackend>>,
    id_to_surrogate: &HashMap<String, u32>,
    next_surrogate: u32,
) -> NodeDbResult<(Vec<WriteOp>, Vec<(String, Vec<u8>)>)> {
    let mut ops: Vec<WriteOp> = Vec::new();
    let mut segment_writes: Vec<(String, Vec<u8>)> = Vec::new();

    // ── Collection list ───────────────────────────────────────────────────────
    let index_keys: Vec<String> = indices.keys().cloned().collect();
    let keys_bytes = zerompk::to_msgpack_vec(&index_keys)
        .map_err(|e| NodeDbError::serialization("msgpack", e))?;
    ops.push(WriteOp::Put {
        ns: Namespace::Fts,
        key: b"fts:_collections".to_vec(),
        value: keys_bytes,
    });

    // ── Surrogate maps ────────────────────────────────────────────────────────
    let surrogate_state = FtsSurrogateState {
        id_to_surrogate: id_to_surrogate
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
        next_surrogate,
    };
    let surrogate_bytes = zerompk::to_msgpack_vec(&surrogate_state)
        .map_err(|e| NodeDbError::serialization("msgpack", e))?;
    ops.push(WriteOp::Put {
        ns: Namespace::Fts,
        key: b"fts:_surrogates".to_vec(),
        value: surrogate_bytes,
    });

    // ── Per-index data ────────────────────────────────────────────────────────
    for (key, idx) in indices {
        // Always collect metadata ops (doc-lengths, meta blobs) onto B+ tree.
        metadata_ops_for_index(key, idx, &mut ops)?;

        // Collect posting data: will be dispatched to pagedb segments or
        // unpacked into per-term KV entries at write time.  Indices with no
        // terms produce no segment_write entry; that's fine.
        if let Some(blob) = serialize_postings_blob(key, idx)? {
            segment_writes.push((key.clone(), blob));
        }
    }

    Ok((ops, segment_writes))
}

/// Write pre-serialized FTS state to storage.
///
/// `ops` contains B+ tree writes (collections, surrogates, doclens, meta).
/// `segment_writes` contains `(index_key, posting_blob)` pairs that are
/// written via `FtsSegmentExt` when available, or unpacked into per-term KV
/// entries on the fallback path.
///
/// Callers serialize inside the FTS mutex (sync, no I/O) and call this
/// function after releasing the lock to perform async I/O.
pub(crate) async fn write_serialized_fts<S>(
    storage: &S,
    mut ops: Vec<WriteOp>,
    segment_writes: Vec<(String, Vec<u8>)>,
) -> NodeDbResult<()>
where
    S: StorageEngine,
{
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(seg_ext) = storage.as_fts_segment_ext() {
        // pagedb path: write posting blobs as encrypted segments, then flush
        // the B+ tree batch (collections, surrogates, doclens, meta).
        for (index_key, blob) in &segment_writes {
            seg_ext
                .write_fts_segment(index_key, blob)
                .await
                .map_err(|e| {
                    NodeDbError::storage(format!("fts segment write '{index_key}': {e}"))
                })?;
        }
        storage
            .batch_write(&ops)
            .await
            .map_err(|e| NodeDbError::storage(format!("fts checkpoint batch_write: {e}")))?;
        return Ok(());
    }

    // KV fallback path (WASM / legacy backends / test doubles): unpack the posting
    // blobs back into per-term KV entries.
    for (index_key, blob) in &segment_writes {
        if let Ok(entries) = zerompk::from_msgpack::<Vec<(String, Vec<SerPosting>)>>(blob) {
            for (scoped_term, postings) in entries {
                let bytes = zerompk::to_msgpack_vec(&postings)
                    .map_err(|e| NodeDbError::serialization("msgpack", e))?;
                let mt_key = format!("fts:{index_key}:mt:{scoped_term}");
                ops.push(WriteOp::Put {
                    ns: Namespace::Fts,
                    key: mt_key.into_bytes(),
                    value: bytes,
                });
            }
        }
    }

    storage
        .batch_write(&ops)
        .await
        .map_err(|e| NodeDbError::storage(format!("fts checkpoint batch_write: {e}")))?;
    Ok(())
}

/// Restore FTS state from storage on cold open.
///
/// Returns `(indices, id_to_surrogate, surrogate_to_id, next_surrogate)`.
/// Returns an empty state if no checkpoint is found.
pub(crate) async fn restore_fts<S>(
    storage: &S,
) -> NodeDbResult<(
    HashMap<String, FtsIndex<MemoryBackend>>,
    HashMap<String, u32>,
    HashMap<u32, String>,
    u32,
)>
where
    S: StorageEngine,
{
    const TID: u64 = 0;

    // ── Read collection list ──────────────────────────────────────────────────
    let Some(keys_bytes) = storage.get(Namespace::Fts, b"fts:_collections").await? else {
        return Ok((HashMap::new(), HashMap::new(), HashMap::new(), 0));
    };
    let Ok(index_keys) = zerompk::from_msgpack::<Vec<String>>(&keys_bytes) else {
        tracing::warn!("fts checkpoint: failed to decode collection list — starting fresh");
        return Ok((HashMap::new(), HashMap::new(), HashMap::new(), 0));
    };

    if index_keys.is_empty() {
        return Ok((HashMap::new(), HashMap::new(), HashMap::new(), 0));
    }

    // ── Read surrogate maps ───────────────────────────────────────────────────
    let surrogate_bytes = storage
        .get(Namespace::Fts, b"fts:_surrogates")
        .await?
        .unwrap_or_default();
    let (id_to_surrogate, surrogate_to_id, next_surrogate) =
        if let Ok(state) = zerompk::from_msgpack::<FtsSurrogateState>(&surrogate_bytes) {
            let mut i2s: HashMap<String, u32> = HashMap::with_capacity(state.id_to_surrogate.len());
            let mut s2i: HashMap<u32, String> = HashMap::with_capacity(state.id_to_surrogate.len());
            for (id, s) in state.id_to_surrogate {
                s2i.insert(s, id.clone());
                i2s.insert(id, s);
            }
            (i2s, s2i, state.next_surrogate)
        } else {
            tracing::warn!("fts checkpoint: failed to decode surrogate maps — starting fresh");
            return Ok((HashMap::new(), HashMap::new(), HashMap::new(), 0));
        };

    // ── Restore per-index data ────────────────────────────────────────────────
    let mut indices: HashMap<String, FtsIndex<MemoryBackend>> =
        HashMap::with_capacity(index_keys.len());

    for index_key in &index_keys {
        let backend = MemoryBackend::new();
        let idx = FtsIndex::new(backend);

        // ── Posting data: try pagedb segment path first, fall back to KV ─────
        let mut restored_from_segment = false;

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(seg_ext) = storage.as_fts_segment_ext() {
            match seg_ext.open_fts_segment(index_key).await {
                Ok(Some(blob)) => {
                    if let Ok(entries) =
                        zerompk::from_msgpack::<Vec<(String, Vec<SerPosting>)>>(&blob)
                    {
                        for (scoped_term, postings) in entries {
                            for sp in postings {
                                idx.memtable().insert(&scoped_term, ser_to_compact(sp));
                            }
                        }
                        restored_from_segment = true;
                    } else {
                        tracing::warn!(
                            index_key,
                            "fts segment blob corrupt — falling back to KV postings"
                        );
                    }
                }
                Ok(None) => {
                    // No segment yet (first open after migration, or empty index).
                    // Will fall through to KV scan below.
                }
                Err(e) => {
                    tracing::warn!(
                        index_key,
                        error = %e,
                        "fts segment open failed — falling back to KV postings"
                    );
                }
            }
        }

        // KV fallback: legacy per-term posting entries.
        if !restored_from_segment {
            let mt_prefix = format!("fts:{index_key}:mt:").into_bytes();
            let mt_entries = storage.scan_prefix(Namespace::Fts, &mt_prefix).await?;
            let mt_prefix_str = format!("fts:{index_key}:mt:");
            for (raw_key, value) in &mt_entries {
                let key_str = String::from_utf8_lossy(raw_key);
                let scoped_term = key_str
                    .strip_prefix(&mt_prefix_str)
                    .unwrap_or("")
                    .to_string();
                if scoped_term.is_empty() {
                    continue;
                }
                if let Ok(ser) = zerompk::from_msgpack::<Vec<SerPosting>>(value) {
                    for sp in ser {
                        idx.memtable().insert(&scoped_term, ser_to_compact(sp));
                    }
                }
            }
        }

        // ── Doc lengths (always on B+ tree) ──────────────────────────────────
        let doclens_key = format!("fts:{index_key}:doclens");
        if let Some(data) = storage.get(Namespace::Fts, doclens_key.as_bytes()).await?
            && let Ok(pairs) = zerompk::from_msgpack::<Vec<(u32, u32)>>(&data)
        {
            for (s, len) in pairs {
                let _ = idx
                    .backend()
                    .write_doc_length(TID, index_key, Surrogate(s), len);
                let _ = idx.backend().increment_stats(TID, index_key, len);
            }
        }

        // ── Meta blobs (always on B+ tree) ────────────────────────────────────
        for &subkey in META_SUBKEYS {
            let meta_key = format!("fts:{index_key}:meta:{subkey}");
            if let Some(data) = storage.get(Namespace::Fts, meta_key.as_bytes()).await? {
                let _ = idx.backend().write_meta(TID, index_key, subkey, &data);
            }
        }

        indices.insert(index_key.clone(), idx);
    }

    tracing::debug!(
        index_count = indices.len(),
        surrogate_count = id_to_surrogate.len(),
        "fts checkpoint restored"
    );

    Ok((indices, id_to_surrogate, surrogate_to_id, next_surrogate))
}
