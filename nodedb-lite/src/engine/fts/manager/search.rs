// SPDX-License-Identifier: Apache-2.0

//! BM25 read side of [`FtsCollectionManager`]: index resolution, ranked
//! search, allowed-set search, and the all-documents score scan.

use std::collections::{HashMap, HashSet};

use nodedb_fts::TextSearchResult;
use nodedb_fts::backend::FtsBackend;
use nodedb_fts::posting::QueryMode as FtsQueryMode;
use nodedb_fts::{FtsSearchParams, IndexScope};
use nodedb_types::text_search::{QueryMode, TextSearchParams};

use super::registry::{FtsCollectionManager, FtsResult, index_key, read_err, search_err};
use crate::engine::fts::LiteFtsIndex;
use crate::error::LiteError;

/// Map the public query mode onto the index's boolean mode.
pub(super) fn fts_mode(mode: QueryMode) -> FtsQueryMode {
    FtsQueryMode::from(mode)
}

/// An index resolved for one read: its key and the index itself.
pub(super) struct Resolved<'a> {
    pub key: String,
    pub idx: &'a LiteFtsIndex,
}

impl FtsCollectionManager {
    /// Resolve the index a read of `field` in `collection` runs against.
    ///
    /// An empty `field` names the whole-document index. `None` means nothing
    /// of the collection was ever text-indexed, so the read matches nothing.
    /// Fails with [`LiteError::TextIndexMissing`] when the collection has
    /// text-indexed documents but none under the named field.
    pub(super) fn resolve(
        &self,
        collection: &str,
        field: &str,
    ) -> Result<Option<Resolved<'_>>, LiteError> {
        let key = index_key(collection, field);
        if let Some(idx) = self.indices.get(&key) {
            return Ok(Some(Resolved { key, idx }));
        }
        if field.is_empty() || !self.has_text_index(collection) {
            return Ok(None);
        }
        Err(LiteError::TextIndexMissing {
            collection: collection.to_owned(),
            field: field.to_owned(),
        })
    }

    /// Run one BM25 query against a resolved index.
    pub(super) fn bm25(
        collection: &str,
        index: &Resolved<'_>,
        query: &str,
        top_k: usize,
        fuzzy: bool,
        mode: FtsQueryMode,
    ) -> Result<Vec<TextSearchResult>, LiteError> {
        Self::bm25_query(
            collection,
            index,
            FtsSearchParams {
                query,
                top_k,
                fuzzy_enabled: fuzzy,
                mode,
                prefilter: None,
            },
        )
    }

    fn bm25_query(
        collection: &str,
        index: &Resolved<'_>,
        params: FtsSearchParams<'_>,
    ) -> Result<Vec<TextSearchResult>, LiteError> {
        index
            .idx
            .search(0, 0, &index.key, params)
            .map_err(|e| search_err(collection, e))
    }

    fn materialize_results(
        &self,
        collection: &str,
        results: Vec<TextSearchResult>,
    ) -> Result<Vec<FtsResult>, LiteError> {
        results
            .into_iter()
            .map(|r| {
                Ok(FtsResult {
                    doc_id: self.doc_id_of(collection, r.doc_id)?.to_owned(),
                    score: r.score,
                    fuzzy: r.fuzzy,
                })
            })
            .collect()
    }

    /// Search the collection's `field` index. An empty `field` searches the whole-document index.
    /// Collections without indexed text return an empty list.
    ///
    /// [`TextSearchParams`] controls boolean mode and fuzzy matching.
    /// Queries without matching documents return an empty list. Read errors propagate.
    pub fn search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: &TextSearchParams,
    ) -> Result<Vec<FtsResult>, LiteError> {
        let Some(index) = self.resolve(collection, field)? else {
            return Ok(Vec::new());
        };
        let raw = Self::bm25(
            collection,
            &index,
            query,
            top_k,
            params.fuzzy,
            fts_mode(params.mode),
        )?;
        self.materialize_results(collection, raw)
    }

    /// Search only allowed surrogate IDs before ranking candidates.
    pub(crate) fn search_with_allowed(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: &TextSearchParams,
        allowed: &HashSet<String>,
    ) -> Result<Vec<FtsResult>, LiteError> {
        let Some(index) = self.resolve(collection, field)? else {
            return Ok(Vec::new());
        };
        if top_k == 0 || allowed.is_empty() {
            return Ok(Vec::new());
        }
        let prefilter: nodedb_types::SurrogateBitmap = allowed
            .iter()
            .filter_map(|id| self.lookup_surrogate(id))
            .collect();
        if prefilter.is_empty() {
            return Ok(Vec::new());
        }
        let raw = Self::bm25_query(
            collection,
            &index,
            FtsSearchParams {
                query,
                top_k,
                fuzzy_enabled: params.fuzzy,
                mode: fts_mode(params.mode),
                prefilter: Some(&prefilter),
            },
        )?;
        self.materialize_results(collection, raw)
    }

    // ── BM25ScoreScan: all docs with injected score (0.0 for non-matches) ────

    /// Return every document the `field` index of `collection` holds, with
    /// its BM25 score against `query`. Documents outside the BM25 hit set
    /// score `0.0`. This powers the score columns of text reads.
    ///
    /// `allowed` restricts the documents to the named ones. The query's
    /// AND-mode fallback is then decided over those documents only.
    pub fn scan_all_with_scores(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        params: &TextSearchParams,
        allowed: Option<&HashSet<String>>,
    ) -> Result<Vec<(String, f32)>, LiteError> {
        let Some(index) = self.resolve(collection, field)? else {
            return Ok(Vec::new());
        };

        // An index holds a document exactly when it records its length.
        let mut members: Vec<(u32, &str)> = Vec::new();
        for (&sur, doc_id) in &self.surrogate_to_id {
            if allowed.is_some_and(|ids| !ids.contains(doc_id)) {
                continue;
            }
            let held = index
                .idx
                .backend()
                .read_doc_length(
                    0,
                    0,
                    IndexScope::document(&index.key),
                    nodedb_types::Surrogate(sur),
                )
                .map_err(|e| read_err(collection, e))?
                .is_some();
            if held {
                members.push((sur, doc_id.as_str()));
            }
        }
        if members.is_empty() {
            return Ok(Vec::new());
        }

        let prefilter: Option<nodedb_types::SurrogateBitmap> = allowed.map(|_| {
            members
                .iter()
                .map(|&(sur, _)| nodedb_types::Surrogate(sur))
                .collect()
        });
        // The member count bounds the hit set; `usize::MAX` would overflow
        // the result heap allocation.
        let hits: HashMap<u32, f32> = Self::bm25_query(
            collection,
            &index,
            FtsSearchParams {
                query,
                top_k: members.len(),
                fuzzy_enabled: params.fuzzy,
                mode: fts_mode(params.mode),
                prefilter: prefilter.as_ref(),
            },
        )?
        .into_iter()
        .map(|r| (r.doc_id.0, r.score))
        .collect();

        Ok(members
            .into_iter()
            .map(|(sur, doc_id)| (doc_id.to_owned(), hits.get(&sur).copied().unwrap_or(0.0)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use nodedb_types::text_search::{QueryMode, TextSearchParams};

    use super::super::registry::test_governor;
    use super::FtsCollectionManager;
    use crate::error::LiteError;

    fn default_params() -> TextSearchParams {
        TextSearchParams {
            fuzzy: false,
            mode: QueryMode::Or,
        }
    }

    #[test]
    fn bm25_score_scan_nonmatching_docs_get_zero_score() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "the quick brown fox")
            .expect("index update must succeed");
        mgr.index_document("col", "doc2", "unrelated content about databases")
            .expect("index update must succeed");

        let scored = mgr
            .scan_all_with_scores("col", "", "quick", &default_params(), None)
            .expect("scan must succeed");
        let score_of = |id: &str| scored.iter().find(|(d, _)| d == id).map(|(_, s)| *s);

        assert!(
            score_of("doc1").is_some_and(|s| s > 0.0),
            "doc1 matches 'quick'"
        );
        assert!(
            score_of("doc2").is_some_and(|s| s.abs() < f32::EPSILON),
            "doc2 does not match 'quick' — score must be 0.0"
        );
    }

    #[test]
    fn bm25_score_scan_lists_only_the_collections_own_documents() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "the quick brown fox")
            .expect("index update must succeed");
        mgr.index_document("other", "doc9", "quick thinking")
            .expect("index update must succeed");
        mgr.index_document("col", "gone", "quick exit")
            .expect("index update must succeed");
        mgr.remove_document("col", "gone")
            .expect("removal must succeed");

        let ids: Vec<String> = mgr
            .scan_all_with_scores("col", "", "quick", &default_params(), None)
            .expect("scan must succeed")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec!["doc1".to_owned()]);
    }

    #[test]
    fn reading_an_unindexed_field_of_an_indexed_collection_is_an_error() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_field("col", "title", "doc1", "hello")
            .expect("index update must succeed");

        let err = mgr
            .search("col", "body", "hello", 10, &default_params())
            .err()
            .expect("an unindexed field must be refused");
        assert!(
            matches!(&err, LiteError::TextIndexMissing { field, .. } if field == "body"),
            "{err:?}"
        );
        for (top_k, allowed) in [
            (10, HashSet::new()),
            (0, HashSet::from(["doc1".to_owned()])),
        ] {
            let err = mgr
                .search_with_allowed("col", "body", "hello", top_k, &default_params(), &allowed)
                .err()
                .expect("an unindexed field must be refused regardless of result limits");
            assert!(
                matches!(&err, LiteError::TextIndexMissing { field, .. } if field == "body"),
                "{err:?}"
            );
        }
    }

    #[test]
    fn reading_a_collection_with_no_text_is_an_empty_result() {
        let mgr = FtsCollectionManager::new(test_governor());
        assert!(
            mgr.search("nothing", "", "query", 10, &default_params())
                .expect("search must succeed")
                .is_empty()
        );
        assert!(
            mgr.search("nothing", "title", "query", 10, &default_params())
                .expect("search must succeed")
                .is_empty()
        );
        assert!(
            mgr.scan_all_with_scores("nothing", "", "query", &default_params(), None)
                .expect("scan must succeed")
                .is_empty()
        );
    }

    #[test]
    fn a_query_the_index_rejects_is_an_error_not_an_empty_result() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "rust and python")
            .expect("index update must succeed");

        let err = mgr
            .search("col", "", "NOT python", 10, &default_params())
            .err()
            .expect("a negation-only query must fail the search");
        assert!(matches!(err, LiteError::FtsQueryInvalid { .. }), "{err:?}");

        let err = mgr
            .scan_all_with_scores("col", "", "NOT python", &default_params(), None)
            .expect_err("a negation-only query must fail the scan");
        assert!(matches!(err, LiteError::FtsQueryInvalid { .. }), "{err:?}");
    }

    #[test]
    fn a_term_no_document_holds_is_an_empty_result() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "rust and python")
            .expect("index update must succeed");

        let results = mgr
            .search("col", "", "haskell", 10, &default_params())
            .expect("search must succeed");
        assert!(results.is_empty());
    }

    #[test]
    fn search_with_allowed_ids_excludes_non_members() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc-a", "rust programming language memory safe")
            .expect("index update must succeed");
        mgr.index_document("col", "doc-b", "rust is fast and compiled")
            .expect("index update must succeed");
        mgr.index_document("col", "doc-c", "python is also a language")
            .expect("index update must succeed");

        let allowed: HashSet<String> = ["doc-a".to_string()].into_iter().collect();
        let results = mgr
            .search_with_allowed("col", "", "rust", 10, &default_params(), &allowed)
            .expect("search must succeed");
        let ids: Vec<&str> = results.iter().map(|r| r.doc_id.as_str()).collect();
        assert_eq!(ids, vec!["doc-a"], "only allowed members may appear");
    }

    #[test]
    fn allowed_search_preserves_unknown_ids_and_zero_limits() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "known", "rust")
            .expect("index update must succeed");
        let unknown = HashSet::from(["unknown".to_owned()]);
        assert!(
            mgr.search_with_allowed("col", "", "rust", 1, &default_params(), &unknown)
                .expect("search must succeed")
                .is_empty()
        );
        assert!(mgr.lookup_surrogate("unknown").is_none());
        let known = HashSet::from(["known".to_owned()]);
        assert!(
            mgr.search_with_allowed("col", "", "rust", 0, &default_params(), &known)
                .expect("search must succeed")
                .is_empty()
        );
    }

    #[test]
    fn hybrid_triple_rrf_score_ordering() {
        // A document appearing in all three sources ranks above one appearing
        // in only one source — purely testing RRF math.
        use nodedb_query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};

        let ranked = |ids: &[(&str, f32)], source: &'static str| -> Vec<RankedResult<String>> {
            ids.iter()
                .enumerate()
                .map(|(rank, (id, score))| RankedResult {
                    document_id: (*id).into(),
                    rank,
                    score: *score,
                    source,
                })
                .collect()
        };
        let fused = reciprocal_rank_fusion_weighted(
            &[
                ranked(&[("A", 0.9), ("B", 0.5)], "vector"),
                ranked(&[("A", 0.8)], "text"),
                ranked(&[("A", 0.0)], "graph"),
            ],
            &[60.0, 60.0, 60.0],
            10,
        );

        assert_eq!(fused[0].document_id, "A", "A appears in all three sources");
        assert!(
            fused[0].rrf_score > fused[1].rrf_score,
            "A's score must exceed B's"
        );
    }
}
