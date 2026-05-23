//! Checkpoint serialization and restoration for [`FtsCollectionManager`].
//!
//! Persists the full in-memory FTS state to `Namespace::Fts` so that a cold
//! open can load the index without re-tokenizing source documents.
//!
//! ## Key layout under `Namespace::Fts`
//!
//! | Key                                     | Value                                         |
//! |-----------------------------------------|-----------------------------------------------|
//! | `fts:_collections`                      | MessagePack `Vec<String>` — index key list    |
//! | `fts:_surrogates`                       | MessagePack `FtsSurrogateState`               |
//! | `fts:{index_key}:mt:{scoped_term}`      | MessagePack `Vec<(u32,u32,u8,Vec<u32>)>` — memtable postings |
//! | `fts:{index_key}:mtstat`                | MessagePack `(u32, u64)` — memtable stats     |
//! | `fts:{index_key}:doclens`               | MessagePack `Vec<(u32, u32)>` — surrogate/len |
//! | `fts:{index_key}:meta:{subkey}`         | raw bytes (fieldnorms/analyzer/language blobs)|
//!
//! ## Rationale: memtable vs segment storage
//!
//! `nodedb-fts` accumulates posting data in an in-memory `Memtable` until the
//! spill threshold (32M entries by default) is reached.  For small-to-medium
//! corpora on Lite, all postings stay in the memtable — the backend's segment
//! storage is effectively empty.  Serializing the segment storage alone would
//! produce empty checkpoints.
//!
//! We therefore serialize the `Memtable` directly via the `FtsIndex::memtable()`
//! accessor.  Memtable terms are stored with a `"{tid}:{collection}:"` scope
//! prefix (e.g., `"0:articles:_doc:rustsearch"`); we persist the full scoped
//! key and replay it identically on restore so that `insert(scoped_term, ...)`
//! recreates the correct in-memory structure.

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

/// Collect all `WriteOp`s needed to persist a single FTS index.
fn ops_for_index(
    index_key: &str,
    idx: &FtsIndex<MemoryBackend>,
    ops: &mut Vec<WriteOp>,
) -> NodeDbResult<()> {
    const TID: u64 = 0;

    let mt = idx.memtable();

    // ── Memtable postings ─────────────────────────────────────────────────────
    // `mt.terms()` returns the fully scoped keys "tid:collection:term".
    // We persist the full scoped key so restore can call `mt.insert(scoped, p)`
    // identically.
    for scoped_term in mt.terms() {
        let postings = mt.get_postings(&scoped_term);
        if postings.is_empty() {
            continue;
        }
        let ser: Vec<SerPosting> = postings.iter().map(compact_to_ser).collect();
        let bytes =
            zerompk::to_msgpack_vec(&ser).map_err(|e| NodeDbError::serialization("msgpack", e))?;
        // Use URL-safe base64 of the scoped_term bytes to avoid key collisions
        // with colons in the collection name.
        let mt_key = format!("fts:{index_key}:mt:{scoped_term}");
        ops.push(WriteOp::Put {
            ns: Namespace::Fts,
            key: mt_key.into_bytes(),
            value: bytes,
        });
    }

    // ── Doc lengths (backend — written per-doc by index_document) ─────────────
    // Collect all surrogates seen in memtable postings.
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

/// Flush the full FTS state to storage.
/// Serialize FTS state into write ops synchronously (no I/O, safe to call
/// while holding a mutex guard).
pub(crate) fn serialize_fts(
    indices: &HashMap<String, FtsIndex<MemoryBackend>>,
    id_to_surrogate: &HashMap<String, u32>,
    next_surrogate: u32,
) -> NodeDbResult<Vec<WriteOp>> {
    let mut ops: Vec<WriteOp> = Vec::new();

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
        ops_for_index(key, idx, &mut ops)?;
    }

    Ok(ops)
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

        // ── Memtable postings ─────────────────────────────────────────────────
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

        // Memtable stats (doc_count, tok_sum) are NOT used for BM25 scoring;
        // `index_stats` reads from backend `collection_stats` which is restored
        // via `increment_stats` below.  We do not restore memtable stats.

        // ── Doc lengths (backend) ─────────────────────────────────────────────
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

        // ── Meta blobs ────────────────────────────────────────────────────────
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
