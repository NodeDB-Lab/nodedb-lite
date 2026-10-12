// SPDX-License-Identifier: Apache-2.0

//! Exact phrase search for [`FtsCollectionManager`].

use std::collections::HashMap;

use nodedb_fts::IndexScope;
use nodedb_fts::posting::QueryMode as FtsQueryMode;
use nodedb_types::Surrogate;
use nodedb_types::text_search::TextSearchParams;

use super::registry::{FtsCollectionManager, FtsResult, read_err};
use super::search::Resolved;
use crate::error::LiteError;

/// Positions of one analyzed term, per document.
type TermPositions = HashMap<Surrogate, Vec<u32>>;

/// Postings of `term` in the resolved index, as document → positions.
///
/// The resolved index key is the memtable collection, with the
/// whole-document scope. Lite is single-database and single-tenant, so both
/// ids are 0.
fn term_positions(index: &Resolved<'_>, term: &str) -> TermPositions {
    index
        .idx
        .memtable()
        .get_postings(0, 0, IndexScope::document(&index.key), term)
        .into_iter()
        .map(|p| (p.doc_id, p.positions))
        .collect()
}

/// The earliest position `p` at which term `i` of the phrase sits at
/// `p + i` for every term, if any.
fn phrase_anchor(doc: Surrogate, per_term: &[TermPositions]) -> Option<u32> {
    let (first, rest) = per_term.split_first()?;
    let anchors = first.get(&doc)?;
    let rest: Vec<&Vec<u32>> = rest.iter().map(|t| t.get(&doc)).collect::<Option<_>>()?;
    anchors.iter().copied().find(|&p| {
        rest.iter().enumerate().all(|(i, positions)| {
            let offset = i as u32 + 1;
            p.checked_add(offset)
                .is_some_and(|want| positions.binary_search(&want).is_ok())
        })
    })
}

impl FtsCollectionManager {
    /// Search the `field` index for documents where `terms` appear as an
    /// exact consecutive phrase. An empty `field` searches the
    /// whole-document index.
    ///
    /// The phrase runs through the index's analyzer first, so it matches the
    /// same stemmed, lower-cased tokens the documents were indexed as.
    /// Candidates are the documents holding every token at consecutive
    /// positions. Each one scores its BM25 score for the phrase with an
    /// earlier-position bonus.
    pub fn phrase_search(
        &self,
        collection: &str,
        field: &str,
        terms: &[String],
        top_k: usize,
        params: &TextSearchParams,
    ) -> Result<Vec<FtsResult>, LiteError> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let Some(index) = self.resolve(collection, field)? else {
            return Ok(Vec::new());
        };
        let phrase = terms.join(" ");
        let tokens = index
            .idx
            .analyze_for_collection(0, 0, &index.key, &phrase)
            .map_err(|e| read_err(collection, e))?;
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let per_term: Vec<TermPositions> =
            tokens.iter().map(|t| term_positions(&index, t)).collect();
        let Some(first) = per_term.first() else {
            return Ok(Vec::new());
        };
        let anchored: Vec<(Surrogate, u32)> = first
            .keys()
            .filter_map(|&doc| phrase_anchor(doc, &per_term).map(|p| (doc, p)))
            .collect();
        if anchored.is_empty() {
            return Ok(Vec::new());
        }

        // Every phrase match holds all tokens, so an AND query sized to the
        // first token's document count returns a score for each of them.
        let scores: HashMap<Surrogate, (f32, bool)> = Self::bm25(
            collection,
            &index,
            &phrase,
            first.len(),
            params.fuzzy,
            FtsQueryMode::And,
        )?
        .into_iter()
        .map(|r| (r.doc_id, (r.score, r.fuzzy)))
        .collect();

        let mut hits: Vec<FtsResult> = anchored
            .into_iter()
            .map(|(doc, earliest)| -> Result<FtsResult, LiteError> {
                let (score, fuzzy) = scores.get(&doc).copied().unwrap_or((0.0, false));
                let position_bonus = 1.0 / (1.0 + earliest as f32 * 0.01);
                Ok(FtsResult {
                    doc_id: self.doc_id_of(collection, doc)?.to_owned(),
                    score: score * position_bonus,
                    fuzzy,
                })
            })
            .collect::<Result<_, _>>()?;

        hits.sort_by(|a, b| b.score.total_cmp(&a.score));
        hits.truncate(top_k);
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::text_search::{QueryMode, TextSearchParams};

    use super::super::registry::test_governor;
    use super::FtsCollectionManager;

    fn default_params() -> TextSearchParams {
        TextSearchParams {
            fuzzy: false,
            mode: QueryMode::Or,
        }
    }

    fn phrase(mgr: &FtsCollectionManager, field: &str, words: &[&str]) -> Vec<String> {
        let terms: Vec<String> = words.iter().map(|w| (*w).to_owned()).collect();
        mgr.phrase_search("col", field, &terms, 10, &default_params())
            .expect("phrase search must succeed")
            .into_iter()
            .map(|r| r.doc_id)
            .collect()
    }

    #[test]
    fn phrase_search_finds_exact_phrase() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "the quick brown fox jumps over")
            .expect("index update must succeed");
        mgr.index_document("col", "doc2", "the brown quick fox")
            .expect("index update must succeed");

        let ids = phrase(&mgr, "", &["quick", "brown"]);
        assert!(ids.contains(&"doc1".to_owned()), "doc1 holds 'quick brown'");
        assert!(
            !ids.contains(&"doc2".to_owned()),
            "doc2 holds 'brown quick'"
        );
    }

    #[test]
    fn phrase_search_no_results_for_nonexistent_phrase() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "the quick brown fox")
            .expect("index update must succeed");
        assert!(phrase(&mgr, "", &["fox", "jumps"]).is_empty());
    }

    #[test]
    fn phrase_terms_match_the_analyzed_tokens() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_document("col", "doc1", "the quick brown fox")
            .expect("index update must succeed");
        assert_eq!(phrase(&mgr, "", &["Quick", "BROWN"]), vec!["doc1"]);
    }

    #[test]
    fn phrase_search_is_scoped_to_the_named_field() {
        let mut mgr = FtsCollectionManager::new(test_governor());
        mgr.index_field("col", "title", "doc1", "quick brown fox")
            .expect("index update must succeed");
        mgr.index_field("col", "body", "doc2", "quick brown fox")
            .expect("index update must succeed");
        assert_eq!(phrase(&mgr, "title", &["quick", "brown"]), vec!["doc1"]);
    }
}
