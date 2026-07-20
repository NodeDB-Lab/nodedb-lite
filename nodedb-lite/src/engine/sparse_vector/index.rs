// SPDX-License-Identifier: Apache-2.0

//! In-memory inverted index for sparse vectors (learned sparse retrieval).
//!
//! Maps each dimension to a posting list of `(internal_doc_id, weight)` pairs.
//! Scoring accumulates the dot product over **only the query's non-zero
//! dimensions**, so cost is proportional to the query's nnz times the average
//! posting-list length — never to the number of indexed documents.
//!
//! Maintained synchronously on document write / delete and checkpoint-
//! serializable so a reopen never needs a rebuild.

use std::cmp::Ordering;
use std::collections::HashMap;

use nodedb_types::SparseVector;

/// A scored hit from the sparse inverted index.
#[derive(Debug, Clone, PartialEq)]
pub struct SparseHit {
    /// The original string document ID.
    pub doc_id: String,
    /// Dot product of the query vector with this document's sparse vector.
    pub score: f32,
}

/// Inverted index over sparse vectors for a single `(collection, field)` pair.
///
/// Documents are addressed externally by their string ID and internally by a
/// dense `u32` so posting lists stay compact.
pub struct SparseInvertedIndex {
    /// Dimension → posting list of `(internal_id, weight)`.
    postings: HashMap<u32, Vec<(u32, f32)>>,
    /// Internal doc ID → the document's own sorted `(dimension, weight)` entries.
    ///
    /// Retained (rather than dimensions alone) so both deletion and checkpoint
    /// serialization are O(nnz) with no reconstruction pass over `postings`.
    doc_entries: HashMap<u32, Vec<(u32, f32)>>,
    /// String doc ID → internal doc ID.
    doc_id_forward: HashMap<String, u32>,
    /// Internal doc ID → string doc ID.
    doc_id_reverse: HashMap<u32, String>,
    /// Next internal doc ID to hand out.
    next_id: u32,
}

impl Default for SparseInvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseInvertedIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_entries: HashMap::new(),
            doc_id_forward: HashMap::new(),
            doc_id_reverse: HashMap::new(),
            next_id: 0,
        }
    }

    /// Insert or replace the sparse vector for `doc_id`.
    ///
    /// Upsert semantics: any postings previously recorded for `doc_id` are
    /// removed before the new ones are appended, so re-indexing a document
    /// never duplicates its postings.
    pub fn insert(&mut self, doc_id: &str, vector: &SparseVector) {
        let internal_id = match self.doc_id_forward.get(doc_id).copied() {
            Some(existing) => {
                self.detach_postings(existing);
                existing
            }
            None => {
                let id = self.next_id;
                self.next_id = self.next_id.saturating_add(1);
                self.doc_id_forward.insert(doc_id.to_owned(), id);
                self.doc_id_reverse.insert(id, doc_id.to_owned());
                id
            }
        };

        let entries = vector.entries().to_vec();
        for &(dim, weight) in &entries {
            self.postings
                .entry(dim)
                .or_default()
                .push((internal_id, weight));
        }
        self.doc_entries.insert(internal_id, entries);
    }

    /// Remove a document entirely. Returns `true` when it was present.
    pub fn delete(&mut self, doc_id: &str) -> bool {
        let Some(internal_id) = self.doc_id_forward.remove(doc_id) else {
            return false;
        };
        self.detach_postings(internal_id);
        self.doc_entries.remove(&internal_id);
        self.doc_id_reverse.remove(&internal_id);
        true
    }

    /// Drop every posting-list entry pointing at `internal_id`, leaving the
    /// doc-ID mappings intact (the caller decides whether the document stays).
    fn detach_postings(&mut self, internal_id: u32) {
        let Some(entries) = self.doc_entries.get(&internal_id) else {
            return;
        };
        let dims: Vec<u32> = entries.iter().map(|&(dim, _)| dim).collect();
        for dim in dims {
            if let Some(list) = self.postings.get_mut(&dim) {
                list.retain(|&(id, _)| id != internal_id);
                if list.is_empty() {
                    self.postings.remove(&dim);
                }
            }
        }
    }

    /// Top-`k` documents by dot product against `query`, descending.
    ///
    /// Only the query's non-zero dimensions are visited. Documents sharing no
    /// dimension with the query never enter the accumulator and are therefore
    /// excluded, as are documents whose weights cancel to exactly zero.
    /// Ties on score are broken by ascending `doc_id` so ordering is stable
    /// across runs and across a checkpoint round-trip.
    pub fn search(&self, query: &SparseVector, top_k: usize) -> Vec<SparseHit> {
        if top_k == 0 || query.is_empty() {
            return Vec::new();
        }

        let mut accumulator: HashMap<u32, f32> = HashMap::new();
        for &(dim, query_weight) in query.entries() {
            let Some(list) = self.postings.get(&dim) else {
                continue;
            };
            for &(internal_id, doc_weight) in list {
                *accumulator.entry(internal_id).or_insert(0.0) += query_weight * doc_weight;
            }
        }

        let mut hits: Vec<SparseHit> = accumulator
            .into_iter()
            .filter(|&(_, score)| score != 0.0)
            .filter_map(|(internal_id, score)| {
                self.doc_id_reverse
                    .get(&internal_id)
                    .map(|doc_id| SparseHit {
                        doc_id: doc_id.clone(),
                        score,
                    })
            })
            .collect();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        hits.truncate(top_k);
        hits
    }

    /// Number of documents currently indexed.
    pub fn doc_count(&self) -> usize {
        self.doc_id_forward.len()
    }

    /// Whether the index holds no documents.
    pub fn is_empty(&self) -> bool {
        self.doc_id_forward.is_empty()
    }

    /// Every indexed document as `(doc_id, entries)`, sorted by `doc_id` so the
    /// checkpoint blob is byte-stable for a given logical state.
    pub fn documents(&self) -> Vec<(String, Vec<(u32, f32)>)> {
        let mut out: Vec<(String, Vec<(u32, f32)>)> = self
            .doc_id_forward
            .iter()
            .filter_map(|(doc_id, internal_id)| {
                self.doc_entries
                    .get(internal_id)
                    .map(|entries| (doc_id.clone(), entries.clone()))
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Rebuild an index from checkpointed `(doc_id, entries)` pairs.
    ///
    /// Entries are already normalized (sorted, deduplicated, finite) by
    /// [`SparseVector`] at insert time; malformed rows are dropped rather than
    /// failing the whole restore, since a partially readable index is strictly
    /// better than none.
    pub fn from_documents(documents: Vec<(String, Vec<(u32, f32)>)>) -> Self {
        let mut index = Self::new();
        for (doc_id, entries) in documents {
            let Ok(vector) = SparseVector::from_entries(entries) else {
                tracing::warn!(
                    doc_id = %doc_id,
                    "sparse checkpoint: document entries rejected — skipping"
                );
                continue;
            };
            index.insert(&doc_id, &vector);
        }
        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).expect("valid sparse vector")
    }

    #[test]
    fn search_ranks_by_dot_product_descending() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("low", &sv(&[(1, 0.1)]));
        idx.insert("high", &sv(&[(1, 5.0)]));
        idx.insert("mid", &sv(&[(1, 1.0)]));

        let hits = idx.search(&sv(&[(1, 1.0)]), 10);
        let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
        assert_eq!(ids, vec!["high", "mid", "low"]);
    }

    #[test]
    fn disjoint_document_is_excluded() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("overlap", &sv(&[(1, 1.0)]));
        idx.insert("disjoint", &sv(&[(99, 1.0)]));

        let hits = idx.search(&sv(&[(1, 1.0)]), 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, "overlap");
    }

    #[test]
    fn upsert_replaces_postings_instead_of_duplicating() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("d", &sv(&[(1, 1.0)]));
        idx.insert("d", &sv(&[(1, 2.0)]));

        assert_eq!(idx.doc_count(), 1);
        let hits = idx.search(&sv(&[(1, 1.0)]), 10);
        assert_eq!(hits.len(), 1);
        assert!((hits[0].score - 2.0).abs() < 1e-6);
    }

    #[test]
    fn upsert_drops_dimensions_no_longer_present() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("d", &sv(&[(1, 1.0), (2, 1.0)]));
        idx.insert("d", &sv(&[(1, 1.0)]));

        assert!(idx.search(&sv(&[(2, 1.0)]), 10).is_empty());
    }

    #[test]
    fn delete_removes_document() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("d", &sv(&[(1, 1.0)]));
        assert!(idx.delete("d"));
        assert!(!idx.delete("d"));
        assert!(idx.search(&sv(&[(1, 1.0)]), 10).is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn ties_broken_by_doc_id() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("b", &sv(&[(1, 1.0)]));
        idx.insert("a", &sv(&[(1, 1.0)]));
        idx.insert("c", &sv(&[(1, 1.0)]));

        let hits = idx.search(&sv(&[(1, 1.0)]), 10);
        let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn top_k_truncates() {
        let mut idx = SparseInvertedIndex::new();
        for i in 0..10u32 {
            idx.insert(&format!("d{i}"), &sv(&[(1, i as f32 + 1.0)]));
        }
        assert_eq!(idx.search(&sv(&[(1, 1.0)]), 3).len(), 3);
        assert!(idx.search(&sv(&[(1, 1.0)]), 0).is_empty());
    }

    #[test]
    fn documents_round_trip_through_from_documents() {
        let mut idx = SparseInvertedIndex::new();
        idx.insert("a", &sv(&[(1, 0.5), (7, 0.25)]));
        idx.insert("b", &sv(&[(7, 1.0)]));

        let restored = SparseInvertedIndex::from_documents(idx.documents());
        assert_eq!(restored.doc_count(), 2);
        assert_eq!(
            restored.search(&sv(&[(7, 1.0)]), 10),
            idx.search(&sv(&[(7, 1.0)]), 10)
        );
    }
}
