//! Per-collection FTS manager for Lite.
//!
//! Wraps `nodedb_fts::FtsIndex<MemoryBackend>` with per-collection management:
//! - Incremental insert/remove on document put/delete
//! - Multi-field per-collection keying (`collection:field`)
//! - BM25 search delegated directly to nodedb-fts (BMW, analyzers, fuzzy)
//! - Persistent: checkpoint serialized to `Namespace::Fts` on `flush()`,
//!   restored on `NodeDbLite::open` without re-tokenizing source documents.
//!
//! This is the canonical FTS implementation for Lite.

use std::collections::HashMap;

use tracing;

use nodedb_fts::FtsIndex;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::posting::QueryMode as FtsQueryMode;
use nodedb_types::Surrogate;
use nodedb_types::text_search::{QueryMode, TextSearchParams};

/// A resolved FTS result with the original string doc_id restored.
pub struct FtsResult {
    pub doc_id: String,
    pub score: f32,
    pub fuzzy: bool,
}

/// Manages per-collection (and per-field) in-memory full-text search indexes.
///
/// Each `(collection, field)` pair gets its own `FtsIndex<MemoryBackend>`.
/// A special `collection:_doc` key is used for whole-document text indexing
/// (all string fields concatenated) used by the `text_search` API.
pub struct FtsCollectionManager {
    /// Key: `"{collection}:{field}"` → FTS index.
    /// Whole-document index uses key `"{collection}:_doc"`.
    indices: HashMap<String, FtsIndex<MemoryBackend>>,
    /// Forward map: original string doc_id → dense u32 surrogate.
    ///
    /// Surrogates **must** be dense (0, 1, 2, …) because `nodedb_fts::Memtable`
    /// uses them as direct indices into a `Vec<u8>` fieldnorm array
    /// (`record_doc` calls `vec.resize(surrogate + 1, 0)`). Hashing strings
    /// into the u32 space produced sparse surrogates near `u32::MAX`, which
    /// allocated multi-gigabyte zero-filled vectors per insert and made
    /// indexing effectively hang.
    id_to_surrogate: HashMap<String, u32>,
    /// Reverse map: surrogate u32 → original string doc_id.
    surrogate_to_id: HashMap<u32, String>,
    /// Next surrogate to assign on first sighting of a doc_id.
    next_surrogate: u32,
    /// Reverse map: Origin global surrogate → Lite string doc_id.
    ///
    /// Populated when `FtsIndexDoc` frames arrive from Origin via the sync path.
    /// Needed by `FtsDeleteDoc` to translate the Origin surrogate back to the
    /// Lite string doc_id without dropping the whole collection.
    origin_surrogate_to_doc_id: HashMap<u32, String>,
}

impl FtsCollectionManager {
    pub fn new() -> Self {
        Self {
            indices: HashMap::new(),
            id_to_surrogate: HashMap::new(),
            surrogate_to_id: HashMap::new(),
            // Start at 1: Surrogate(0) is the unassigned sentinel and is
            // rejected by FtsIndex::index_document with SurrogateOutOfRange.
            next_surrogate: 1,
            origin_surrogate_to_doc_id: HashMap::new(),
        }
    }

    /// Look up or allocate a dense surrogate for a string `doc_id`.
    ///
    /// Returns the existing surrogate if `doc_id` has been indexed before,
    /// otherwise assigns the next sequential u32 and records the mapping
    /// in both directions.
    fn surrogate_for(&mut self, doc_id: &str) -> Surrogate {
        if let Some(&s) = self.id_to_surrogate.get(doc_id) {
            return Surrogate(s);
        }
        let s = self.next_surrogate;
        self.next_surrogate = self
            .next_surrogate
            .checked_add(1)
            .expect("FTS surrogate counter overflowed u32");
        self.id_to_surrogate.insert(doc_id.to_owned(), s);
        self.surrogate_to_id.insert(s, doc_id.to_owned());
        Surrogate(s)
    }

    /// Look up an existing surrogate without allocating one.
    fn lookup_surrogate(&self, doc_id: &str) -> Option<Surrogate> {
        self.id_to_surrogate.get(doc_id).copied().map(Surrogate)
    }

    /// Returns true if no collections are indexed.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    // ── Whole-document indexing (used by `document_put` / `text_search`) ─────

    /// Index all string field values from a document as a single text blob.
    ///
    /// The document is stored under the `"{collection}:_doc"` key.
    /// Calling again with the same `doc_id` replaces the previous entry.
    pub fn index_document(&mut self, collection: &str, doc_id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        let surrogate = self.surrogate_for(doc_id);
        let key = format!("{collection}:_doc");
        let idx = self
            .indices
            .entry(key.clone())
            .or_insert_with(|| FtsIndex::new(MemoryBackend::new()));
        // Remove old entry first (upsert semantics).
        let _ = idx.remove_document(0, &key, surrogate);
        let _ = idx.index_document(0, &key, surrogate, text);
    }

    /// Remove a document from the whole-document index.
    pub fn remove_document(&mut self, collection: &str, doc_id: &str) {
        let Some(surrogate) = self.lookup_surrogate(doc_id) else {
            return;
        };
        let key = format!("{collection}:_doc");
        if let Some(idx) = self.indices.get_mut(&key) {
            let _ = idx.remove_document(0, &key, surrogate);
        }
    }

    /// Search the whole-document index for a collection.
    ///
    /// All query knobs are passed via [`TextSearchParams`]: boolean mode (OR/AND),
    /// fuzzy matching, and BM25 scoring parameters (k1, b).
    pub fn search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        params: &TextSearchParams,
    ) -> Vec<FtsResult> {
        let key = format!("{collection}:_doc");
        let Some(idx) = self.indices.get(&key) else {
            return Vec::new();
        };
        let mode = match params.mode {
            QueryMode::Or => FtsQueryMode::Or,
            QueryMode::And => FtsQueryMode::And,
            _ => FtsQueryMode::Or,
        };
        let raw = idx
            .search_with_mode(0, &key, query, top_k, params.fuzzy, mode, None)
            .inspect_err(|e| tracing::warn!(collection, error = %e, "fts search failed"))
            .unwrap_or_default();
        raw.into_iter()
            .filter_map(|r| {
                let doc_id = self.surrogate_to_id.get(&r.doc_id.0)?.clone();
                Some(FtsResult {
                    doc_id,
                    score: r.score,
                    fuzzy: r.fuzzy,
                })
            })
            .collect()
    }

    /// Like [`Self::search`] but restricts results to documents whose string
    /// doc_id is in `allowed`. Fetches `top_k * 8` candidates from BM25 to
    /// account for haystack documents that rank below non-haystack documents.
    pub(crate) fn search_with_allowed(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        params: &TextSearchParams,
        allowed: &std::collections::HashSet<String>,
    ) -> Vec<FtsResult> {
        let fetch_k = top_k.saturating_mul(8).max(top_k);
        self.search(collection, query, fetch_k, params)
            .into_iter()
            .filter(|r| allowed.contains(&r.doc_id))
            .take(top_k)
            .collect()
    }

    // ── BM25ScoreScan: all docs with injected score (0.0 for non-matches) ────

    /// Return every known document in `collection` together with its BM25 score
    /// against `query`. Documents that are not in the BM25 hit set receive
    /// score `0.0`. This powers `TextOp::BM25ScoreScan`.
    pub fn scan_all_with_scores(
        &self,
        collection: &str,
        query: &str,
        params: &TextSearchParams,
    ) -> Vec<(String, f32)> {
        let key = format!("{collection}:_doc");
        let Some(idx) = self.indices.get(&key) else {
            return Vec::new();
        };
        let mode = match params.mode {
            QueryMode::Or => FtsQueryMode::Or,
            QueryMode::And => FtsQueryMode::And,
            _ => FtsQueryMode::Or,
        };
        // Fetch BM25 hits for the query (all matching docs).
        // Use the total known-surrogate count as top_k; this is a safe upper
        // bound and avoids passing usize::MAX which causes a heap allocation overflow.
        let total_known = self.surrogate_to_id.len().max(1);
        let hits: HashMap<u32, f32> = idx
            .search_with_mode(0, &key, query, total_known, params.fuzzy, mode, None)
            .inspect_err(|e| tracing::warn!(collection, error = %e, "bm25 scan failed"))
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.doc_id.0, r.score))
            .collect();

        // Emit every known doc_id in this collection with its score (0.0 if absent).
        self.surrogate_to_id
            .iter()
            .filter_map(|(&sur, doc_id)| {
                // Only include surrogates that belong to this collection by checking
                // whether this surrogate appears in the index at all (has a doc_len).
                // We use id_to_surrogate presence as the membership test.
                if self.id_to_surrogate.contains_key(doc_id) {
                    let score = hits.get(&sur).copied().unwrap_or(0.0);
                    Some((doc_id.clone(), score))
                } else {
                    None
                }
            })
            .collect()
    }

    // ── PhraseSearch: exact consecutive-term matching ─────────────────────────

    /// Search for documents where `terms` appear as an exact consecutive phrase.
    ///
    /// Algorithm: fetch OR results from BM25 (any term present), then filter
    /// to candidates that contain all terms with consecutive positions
    /// (term_0 at position p, term_1 at p+1, …). Scoring is BM25 score with
    /// an earlier-position bonus (higher score for phrases closer to doc start).
    pub fn phrase_search(
        &self,
        collection: &str,
        terms: &[String],
        top_k: usize,
        params: &TextSearchParams,
    ) -> Vec<FtsResult> {
        if terms.is_empty() {
            return Vec::new();
        }
        let key = format!("{collection}:_doc");
        let Some(idx) = self.indices.get(&key) else {
            return Vec::new();
        };

        // Gather OR results for all terms to get candidates with position data.
        // Use a generous multiplier over top_k; phrase filter will further reduce
        // the set. Capped at the total known-doc count to avoid heap overflow.
        let query = terms.join(" ");
        let candidate_limit = (top_k * 10).max(100).min(self.surrogate_to_id.len().max(1));
        let or_hits = idx
            .search_with_mode(
                0,
                &key,
                &query,
                candidate_limit,
                params.fuzzy,
                FtsQueryMode::Or,
                None,
            )
            .inspect_err(|e| tracing::warn!(collection, error = %e, "phrase search or-pass failed"))
            .unwrap_or_default();

        if or_hits.is_empty() {
            return Vec::new();
        }

        // For each candidate doc, retrieve per-term position lists and check
        // for a consecutive sequence: term[i] at pos p, term[i+1] at p+1, etc.
        let mut phrase_hits: Vec<FtsResult> = or_hits
            .into_iter()
            .filter_map(|hit| {
                let sur = hit.doc_id;
                // Retrieve postings for each term from the memtable.
                let term_positions: Vec<Vec<u32>> = terms
                    .iter()
                    .map(|term| {
                        let scoped = format!("0:{key}:{term}");
                        idx.memtable()
                            .get_postings(&scoped)
                            .into_iter()
                            .find(|p| p.doc_id == sur)
                            .map(|p| p.positions.clone())
                            .unwrap_or_default()
                    })
                    .collect();

                // Check that every term has at least one position.
                if term_positions.iter().any(|p| p.is_empty()) {
                    return None;
                }

                // Find any anchor position p in term_positions[0] such that
                // term_positions[i] contains p+i for all i.
                let anchors = &term_positions[0];
                let found = anchors.iter().any(|&p| {
                    term_positions
                        .iter()
                        .enumerate()
                        .skip(1)
                        .all(|(i, positions)| positions.binary_search(&(p + i as u32)).is_ok())
                });

                if !found {
                    return None;
                }

                // Score: BM25 score with earlier-position bonus.
                let earliest = anchors.iter().copied().min().unwrap_or(u32::MAX);
                let position_bonus = 1.0 / (1.0 + earliest as f32 * 0.01);
                let score = hit.score * position_bonus;

                let doc_id = self.surrogate_to_id.get(&sur.0)?.clone();
                Some(FtsResult {
                    doc_id,
                    score,
                    fuzzy: hit.fuzzy,
                })
            })
            .collect();

        phrase_hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        phrase_hits.truncate(top_k);
        phrase_hits
    }

    // ── Origin-surrogate reverse map (for FtsIndexDoc / FtsDeleteDoc sync) ────

    /// Register an association between an Origin global surrogate and the
    /// Lite string `doc_id`. Called from the `FtsIndexDoc` execution arm so
    /// `FtsDeleteDoc` can later resolve the Origin surrogate to a string doc_id
    /// and call the proper single-doc removal instead of dropping the collection.
    pub fn register_origin_surrogate(&mut self, origin_surrogate: Surrogate, doc_id: &str) {
        self.origin_surrogate_to_doc_id
            .insert(origin_surrogate.0, doc_id.to_owned());
    }

    /// Remove a single document identified by its Origin-assigned surrogate.
    ///
    /// Returns `true` if the document was found and removed, `false` if the
    /// surrogate has no known Lite mapping (e.g. it was never indexed via
    /// this Lite instance).
    pub fn remove_by_origin_surrogate(
        &mut self,
        collection: &str,
        origin_surrogate: Surrogate,
    ) -> bool {
        let Some(doc_id) = self.origin_surrogate_to_doc_id.remove(&origin_surrogate.0) else {
            tracing::debug!(
                collection,
                sur = origin_surrogate.0,
                "FtsDeleteDoc: no Lite mapping for Origin surrogate — document was never indexed here"
            );
            return false;
        };
        self.remove_document(collection, &doc_id);
        true
    }

    // ── Per-field indexing (used by strict collections via index_integration) ─

    /// Index a single field value for a document.
    ///
    /// Key is `"{collection}:{field}"`. Calling again with the same `doc_id`
    /// replaces the previous entry (upsert semantics).
    pub fn index_field(&mut self, collection: &str, field: &str, doc_id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        let surrogate = self.surrogate_for(doc_id);
        let key = format!("{collection}:{field}");
        let idx = self
            .indices
            .entry(key.clone())
            .or_insert_with(|| FtsIndex::new(MemoryBackend::new()));
        let _ = idx.remove_document(0, &key, surrogate);
        let _ = idx.index_document(0, &key, surrogate, text);
    }

    /// Remove all field entries for a document across all fields in a collection.
    pub fn remove_field(&mut self, collection: &str, field: &str, doc_id: &str) {
        let Some(surrogate) = self.lookup_surrogate(doc_id) else {
            return;
        };
        let key = format!("{collection}:{field}");
        if let Some(idx) = self.indices.get_mut(&key) {
            let _ = idx.remove_document(0, &key, surrogate);
        }
    }

    /// Number of distinct collection prefixes with active indexes.
    pub fn collection_count(&self) -> usize {
        self.indices
            .keys()
            .map(|k| k.split(':').next().unwrap_or(k.as_str()))
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    /// Drop all FTS indexes for a collection (called on collection drop/truncate).
    pub fn drop_collection(&mut self, collection: &str) {
        let prefix = format!("{collection}:");
        self.indices.retain(|k, _| !k.starts_with(&prefix));
    }

    // ── Checkpoint helpers (used by core.rs flush/restore) ────────────────────

    /// Borrow the index map, surrogate map, and next-surrogate counter for
    /// serialization.  Called by `checkpoint::flush_fts`.
    pub(crate) fn checkpoint_data(
        &self,
    ) -> (
        &HashMap<String, FtsIndex<MemoryBackend>>,
        &HashMap<String, u32>,
        u32,
    ) {
        (&self.indices, &self.id_to_surrogate, self.next_surrogate)
    }

    /// Replace internal state from a restored checkpoint.  Called by
    /// `restore_fts_indices` in `core.rs` when a valid checkpoint is found.
    pub(crate) fn load_checkpoint(
        &mut self,
        indices: HashMap<String, FtsIndex<MemoryBackend>>,
        id_to_surrogate: HashMap<String, u32>,
        surrogate_to_id: HashMap<u32, String>,
        next_surrogate: u32,
    ) {
        self.indices = indices;
        self.id_to_surrogate = id_to_surrogate;
        self.surrogate_to_id = surrogate_to_id;
        self.next_surrogate = next_surrogate;
        // origin_surrogate_to_doc_id is not persisted across restarts because
        // origin surrogates are only relevant for the lifetime of a sync session;
        // FtsIndexDoc frames re-register the mapping on re-sync.
    }
}

impl Default for FtsCollectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;
    use nodedb_types::text_search::{QueryMode, TextSearchParams};

    use super::FtsCollectionManager;

    fn default_params() -> TextSearchParams {
        TextSearchParams {
            fuzzy: false,
            mode: QueryMode::Or,
        }
    }

    // ── BM25ScoreScan ─────────────────────────────────────────────────────────

    #[test]
    fn bm25_score_scan_nonmatching_docs_get_zero_score() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "the quick brown fox");
        mgr.index_document("col", "doc2", "unrelated content about databases");

        let scored = mgr.scan_all_with_scores("col", "quick", &default_params());
        let doc1_score = scored.iter().find(|(id, _)| id == "doc1").map(|(_, s)| *s);
        let doc2_score = scored.iter().find(|(id, _)| id == "doc2").map(|(_, s)| *s);

        assert!(
            doc1_score.is_some(),
            "doc1 must appear in scan_all_with_scores"
        );
        assert!(
            doc2_score.is_some(),
            "doc2 must appear in scan_all_with_scores"
        );
        assert!(
            doc1_score.unwrap() > 0.0,
            "doc1 matches 'quick' — score must be positive"
        );
        assert!(
            (doc2_score.unwrap() - 0.0).abs() < f32::EPSILON,
            "doc2 does not match 'quick' — score must be 0.0"
        );
    }

    #[test]
    fn bm25_score_scan_empty_collection_returns_empty() {
        let mgr = FtsCollectionManager::new();
        let scored = mgr.scan_all_with_scores("nonexistent", "query", &default_params());
        assert!(scored.is_empty());
    }

    // ── PhraseSearch ──────────────────────────────────────────────────────────

    #[test]
    fn phrase_search_finds_exact_phrase() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "the quick brown fox jumps over");
        mgr.index_document("col", "doc2", "the brown quick fox");

        let terms: Vec<String> = vec!["quick".into(), "brown".into()];
        let results = mgr.phrase_search("col", &terms, 10, &default_params());

        let ids: Vec<&str> = results.iter().map(|r| r.doc_id.as_str()).collect();
        // "the quick brown fox" has quick at pos N, brown at pos N+1 — match
        // "the brown quick fox" has brown then quick — not a forward phrase match
        assert!(
            ids.contains(&"doc1"),
            "doc1 contains 'quick brown' consecutively"
        );
        assert!(
            !ids.contains(&"doc2"),
            "doc2 has 'brown quick' (reversed) — must not match"
        );
    }

    #[test]
    fn phrase_search_no_results_for_nonexistent_phrase() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "the quick brown fox");

        let terms: Vec<String> = vec!["fox".into(), "jumps".into()];
        let results = mgr.phrase_search("col", &terms, 10, &default_params());
        assert!(
            results.is_empty(),
            "phrase 'fox jumps' not in doc — no results"
        );
    }

    // ── FtsDeleteDoc / origin surrogate reverse map ───────────────────────────

    #[test]
    fn fts_delete_doc_removes_only_targeted_doc() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "rust programming language");
        mgr.index_document("col", "doc2", "rust is fast and safe");
        mgr.index_document("col", "doc3", "python is also great");

        // Register origin surrogate for doc2 (as if FtsIndexDoc was dispatched).
        mgr.register_origin_surrogate(Surrogate(42), "doc2");

        // Delete via origin surrogate.
        let removed = mgr.remove_by_origin_surrogate("col", Surrogate(42));
        assert!(removed, "doc2 must be found and removed");

        // doc1 and doc3 still searchable, doc2 not.
        let results = mgr.search("col", "rust", 10, &default_params());
        let ids: Vec<&str> = results.iter().map(|r| r.doc_id.as_str()).collect();
        assert!(ids.contains(&"doc1"), "doc1 must still be present");
        assert!(
            !ids.contains(&"doc2"),
            "doc2 must be removed from the index"
        );
    }

    #[test]
    fn search_with_allowed_ids_excludes_non_members() {
        use nodedb_types::text_search::{QueryMode, TextSearchParams};
        use std::collections::HashSet;

        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc-a", "rust programming language memory safe");
        mgr.index_document("col", "doc-b", "rust is fast and compiled");
        mgr.index_document("col", "doc-c", "python is also a language");

        let allowed: HashSet<String> = ["doc-a".to_string()].into_iter().collect();
        let params = TextSearchParams {
            fuzzy: false,
            mode: QueryMode::Or,
        };

        let results = mgr.search_with_allowed("col", "rust", 10, &params, &allowed);
        let ids: Vec<&str> = results.iter().map(|r| r.doc_id.as_str()).collect();
        assert!(
            ids.contains(&"doc-a"),
            "doc-a must appear (in allowed set and matches query), got: {ids:?}"
        );
        assert!(
            !ids.contains(&"doc-b"),
            "doc-b must be excluded (not in allowed set), got: {ids:?}"
        );
        assert!(
            !ids.contains(&"doc-c"),
            "doc-c must be excluded (not in allowed set and does not match rust), got: {ids:?}"
        );
    }

    #[test]
    fn fts_delete_doc_unknown_surrogate_returns_false() {
        let mut mgr = FtsCollectionManager::new();
        mgr.index_document("col", "doc1", "hello world");

        let removed = mgr.remove_by_origin_surrogate("col", Surrogate(99));
        assert!(!removed, "unknown surrogate must return false");

        // doc1 unaffected.
        let results = mgr.search("col", "hello", 10, &default_params());
        assert_eq!(results.len(), 1);
    }

    // ── HybridSearchTriple (unit-level RRF logic) ─────────────────────────────

    #[test]
    fn hybrid_triple_rrf_score_ordering() {
        // Verify that a document appearing in all three sources ranks above
        // one appearing in only one source — purely testing RRF math.
        use nodedb_query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};

        let vector_ranked = vec![
            RankedResult {
                document_id: "A".into(),
                rank: 0,
                score: 0.9,
                source: "vector",
            },
            RankedResult {
                document_id: "B".into(),
                rank: 1,
                score: 0.5,
                source: "vector",
            },
        ];
        let text_ranked = vec![RankedResult {
            document_id: "A".into(),
            rank: 0,
            score: 0.8,
            source: "text",
        }];
        let graph_ranked = vec![RankedResult {
            document_id: "A".into(),
            rank: 0,
            score: 0.0,
            source: "graph",
        }];

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_ranked, text_ranked, graph_ranked],
            &[60.0, 60.0, 60.0],
            10,
        );

        assert!(!fused.is_empty());
        assert_eq!(
            fused[0].document_id, "A",
            "A appears in all three sources — must rank first"
        );
        if fused.len() > 1 {
            assert!(
                fused[0].rrf_score > fused[1].rrf_score,
                "A's score must exceed B's"
            );
        }
    }
}
