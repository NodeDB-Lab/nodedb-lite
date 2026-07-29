// SPDX-License-Identifier: Apache-2.0

//! Durable per-document vector storage — the source of truth for vectors.
//!
//! # Why this exists
//!
//! A vector used to live in exactly one place: the in-memory HNSW index, which
//! reached disk only when `flush` wrote the `vec/hnsw/<collection>` segment.
//! That gave vectors a weaker durability guarantee than the documents they
//! belong to — a document is durable the moment its write is acknowledged
//! (versioned put), while its vector survived only if a later flush happened to
//! run. An unclean exit therefore lost every vector written since the last
//! flush, silently, because the write had already reported success.
//!
//! It also made the segment the *only* copy, so a segment that could not be
//! reopened left exactly two options, both wrong: keep a checkpoint whose node
//! vectors are empty placeholders (the first distance computation panics with
//! `dist_to_node: byte-length mismatch`), or drop the index and lose every
//! vector permanently. There was no third option because nothing else held the
//! data — in particular the CRDT holds only `embedding_dim`, never the floats,
//! so the "rebuild from CRDT" the restore path spoke of could never have worked.
//!
//! # The contract
//!
//! Every vector is written here in the same operation that makes its document
//! durable. [`crate::engine::vector::pagedb_backing`] segments become a
//! *derived* index: an accelerator that can always be rebuilt from these rows
//! ([`load_collection`]), never the master copy. That makes a corrupt or
//! unreadable segment a rebuild, not a data-loss event.
//!
//! # Layout
//!
//! `Namespace::Vector`, key `v:<collection>:<doc_id>`, value = little-endian
//! `f32` values with no header. The dimension is implied by the byte length,
//! which is why [`decode`] rejects a length that is not a multiple of 4. The
//! `v:` prefix is disjoint from the other keys in this namespace (`hnsw:<name>`
//! checkpoints and `hnsw_id_map`), so a prefix scan returns vectors only.

use std::collections::HashMap;

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

/// Bytes per stored element.
const F32_BYTES: usize = 4;

/// Key prefix for per-document vectors, disjoint from `hnsw:*`.
const VEC_PREFIX: &str = "v:";

/// Key-space prefix for one collection's vectors.
pub(crate) fn collection_prefix(collection: &str) -> Vec<u8> {
    format!("{VEC_PREFIX}{collection}:").into_bytes()
}

/// Durable key for one document's vector.
pub(crate) fn key(collection: &str, doc_id: &str) -> Vec<u8> {
    format!("{VEC_PREFIX}{collection}:{doc_id}").into_bytes()
}

/// Encode `vector` as little-endian `f32` bytes.
pub(crate) fn encode(vector: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vector.len() * F32_BYTES);
    for v in vector {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Decode little-endian `f32` bytes back into a vector.
///
/// Returns `None` when `bytes` is not a whole number of `f32`s — a truncated or
/// foreign row is skipped rather than reinterpreted, since a mis-sized vector
/// would panic the distance kernels it is fed to.
pub(crate) fn decode(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.is_empty() || bytes.len() % F32_BYTES != 0 {
        return None;
    }
    Some(
        bytes
            .chunks_exact(F32_BYTES)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    )
}

/// The write that makes `vector` durable for `doc_id`.
///
/// Returned as a [`WriteOp`] so callers can place it in the SAME batch as the
/// document itself — the whole point is that the two become durable together.
pub(crate) fn put_op(collection: &str, doc_id: &str, vector: &[f32]) -> WriteOp {
    WriteOp::Put {
        ns: Namespace::Vector,
        key: key(collection, doc_id),
        value: encode(vector),
    }
}

/// Remove a document's durable vector.
pub(crate) async fn remove<S: StorageEngine>(
    storage: &S,
    collection: &str,
    doc_id: &str,
) -> Result<(), LiteError> {
    storage.delete(Namespace::Vector, &key(collection, doc_id)).await
}

/// Every collection that has at least one durable vector.
///
/// Discovery must not depend on `META_HNSW_COLLECTIONS`: that list is written
/// by `flush`, so a database that has taken writes but never flushed has no
/// list at all — and those are exactly the vectors most at risk. Deriving the
/// collection set from the durable rows means a never-flushed database still
/// rebuilds its indexes on open.
pub(crate) async fn list_collections<S: StorageEngine>(
    storage: &S,
) -> Result<Vec<String>, LiteError> {
    let rows = storage
        .scan_prefix(Namespace::Vector, VEC_PREFIX.as_bytes())
        .await?;
    let mut names: Vec<String> = Vec::new();
    for (row_key, _) in rows {
        // `v:<collection>:<doc_id>` — the collection is up to the FIRST ':'
        // after the prefix; document ids may themselves contain ':'.
        let Some(rest) = row_key.get(VEC_PREFIX.len()..) else {
            continue;
        };
        let Some(sep) = rest.iter().position(|&b| b == b':') else {
            continue;
        };
        let Ok(name) = std::str::from_utf8(&rest[..sep]) else {
            continue;
        };
        if !names.iter().any(|n| n == name) {
            names.push(name.to_owned());
        }
    }
    Ok(names)
}

/// Load every durable vector for `collection`, as `(doc_id, vector)`.
///
/// This is what makes the HNSW segment rebuildable. Rows that fail to decode
/// are skipped with a warning rather than aborting the load: one unreadable row
/// must not cost the whole index, and skipping is safe because the index is
/// derived — the row stays on disk for a later repair.
pub(crate) async fn load_collection<S: StorageEngine>(
    storage: &S,
    collection: &str,
) -> Result<Vec<(String, Vec<f32>)>, LiteError> {
    let prefix = collection_prefix(collection);
    let rows = storage.scan_prefix(Namespace::Vector, &prefix).await?;

    let mut out = Vec::with_capacity(rows.len());
    for (row_key, value) in rows {
        let Some(doc_id) = row_key.get(prefix.len()..).and_then(|b| std::str::from_utf8(b).ok())
        else {
            tracing::warn!(
                collection,
                "durable vector row has a non-UTF-8 key; skipping"
            );
            continue;
        };
        let Some(vector) = decode(&value) else {
            tracing::warn!(
                collection,
                doc_id,
                bytes = value.len(),
                "durable vector row is not a whole number of f32s; skipping"
            );
            continue;
        };
        out.push((doc_id.to_owned(), vector));
    }
    Ok(out)
}

/// Rebuild `collection`'s HNSW from its durable vectors.
///
/// The single implementation shared by every recovery path — open-time restore
/// and lazy-load both land here, so they cannot drift into different recovery
/// behaviour. Returns the index plus the `"<collection>:<internal_id>" ->
/// (doc_id, internal_id)` entries for it; internal ids are reassigned from
/// zero, so those entries REPLACE any persisted map for this collection.
///
/// `template` carries the `(dim, params)` of the index being replaced so a
/// rebuild cannot silently change how distances are computed. It is taken BY
/// VALUE rather than as an `&HnswIndex` because the index holds a `RefCell`
/// arena — borrowing it across this `await` would make every calling future
/// non-`Send`. When it is `None` (nothing to replace) the params default
/// exactly as `ensure_hnsw` would set them on a first insert. Returns `None`
/// when the collection has no durable vectors.
pub(crate) async fn rebuild_index<S: StorageEngine>(
    storage: &S,
    collection: &str,
    template: Option<(usize, crate::engine::vector::HnswParams)>,
) -> Result<Option<(crate::engine::vector::HnswIndex, HashMap<String, (String, u32)>)>, LiteError> {
    use crate::engine::vector::{HnswIndex, HnswParams};

    let rows = load_collection(storage, collection).await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let (dim, params) = match template {
        Some(t) => t,
        None => (rows[0].1.len(), HnswParams::default()),
    };
    let mut index = HnswIndex::new(dim, params);
    let mut id_map = HashMap::new();

    for (doc_id, vector) in rows {
        if vector.len() != dim {
            tracing::warn!(
                collection,
                doc_id,
                expected = dim,
                found = vector.len(),
                "durable vector has the wrong dimension for its collection; skipping"
            );
            continue;
        }
        let internal_id = index.len() as u32;
        if let Err(e) = index.insert(vector) {
            tracing::warn!(collection, doc_id, error = %e, "durable vector insert failed; skipping");
            continue;
        }
        id_map.insert(format!("{collection}:{internal_id}"), (doc_id, internal_id));
    }

    Ok(Some((index, id_map)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_vector() {
        let v = vec![1.0_f32, -2.5, 0.0, 3.25];
        assert_eq!(decode(&encode(&v)).expect("decodes"), v);
    }

    /// A truncated row must be REJECTED, not reinterpreted. A vector whose
    /// length is not a whole number of f32s would reach the distance kernels
    /// with the wrong byte length and panic there instead.
    #[test]
    fn rejects_a_truncated_row() {
        let mut bytes = encode(&[1.0_f32, 2.0]);
        bytes.pop();
        assert!(decode(&bytes).is_none());
        assert!(decode(&[]).is_none());
    }

    /// The per-document prefix must not collide with the other keys living in
    /// `Namespace::Vector`, or a rebuild scan would pick up checkpoints.
    #[test]
    fn key_prefix_is_disjoint_from_checkpoint_keys() {
        let k = key("entries", "abc");
        assert!(k.starts_with(&collection_prefix("entries")));
        assert!(!k.starts_with(b"hnsw:"));
        assert_ne!(k.as_slice(), b"hnsw_id_map");
    }

    /// One collection's scan prefix must not match another's.
    #[test]
    fn collection_prefixes_do_not_alias() {
        let entries = collection_prefix("entries");
        assert!(!key("entries_archive", "x").starts_with(&entries));
    }
}
