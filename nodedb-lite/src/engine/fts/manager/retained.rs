// SPDX-License-Identifier: Apache-2.0

//! Conservative retained-index leases survive retraction and move with publication.

use crate::engine::fts::LiteFtsIndex;
use crate::error::LiteError;
use nodedb_fts::IndexScope;
use nodedb_mem::{EngineId, MemoryGovernor, ReservationToken, ScopedMemory};
use nodedb_types::{DatabaseId, Surrogate, TenantId};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub(super) struct RetainedIndex {
    chunks: Vec<ReservationToken>,
    dense_slots: usize,
    row_estimated_bytes: HashMap<u32, usize>,
}

fn memory(governor: Arc<MemoryGovernor>) -> ScopedMemory {
    ScopedMemory::new(
        governor,
        DatabaseId::new(0),
        TenantId::new(0),
        EngineId::Fts,
    )
}

fn overflow(key: &str) -> LiteError {
    LiteError::Backpressure {
        detail: format!(
            "retained full-text index '{key}' exceeds addressable memory. Reduce indexed fields or text"
        ),
    }
}

fn estimate_row(key: &str, tokens: &[String]) -> Result<usize, LiteError> {
    // Each occurrence reserves a distinct scoped key and posting. Normalized
    // strings, hash buckets, positions, and geometric Vec capacity remain charged.
    let scoped_prefix = key.len().checked_add(5).ok_or_else(|| overflow(key))?;
    let per_token = scoped_prefix
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(320))
        .ok_or_else(|| overflow(key))?;
    tokens.iter().try_fold(0usize, |bytes, token| {
        token
            .len()
            .checked_mul(4)
            .and_then(|normalized| normalized.checked_add(per_token))
            .and_then(|cost| bytes.checked_add(cost))
            .ok_or_else(|| overflow(key))
    })
}

impl RetainedIndex {
    pub(super) fn reserve_base(
        &mut self,
        governor: Arc<MemoryGovernor>,
        key: &str,
    ) -> Result<(), LiteError> {
        let bytes = key
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| overflow(key))?;
        self.chunks.push(memory(governor).reserve(bytes).map_err(|error| LiteError::Backpressure { detail: format!("retained full-text index '{key}': {error}. Increase the FTS budget or reduce indexed fields") })?);
        Ok(())
    }

    pub(super) fn reserve_write(
        &mut self,
        governor: Arc<MemoryGovernor>,
        key: &str,
        surrogate: Surrogate,
        tokens: &[String],
    ) -> Result<(), LiteError> {
        if tokens.is_empty() {
            return Ok(());
        }
        let slots = (surrogate.0 as usize)
            .checked_add(1)
            .ok_or_else(|| overflow(key))?;
        let dense = slots
            .saturating_sub(self.dense_slots)
            .checked_mul(16)
            .ok_or_else(|| overflow(key))?;
        let row = estimate_row(key, tokens)?;
        let previous = self
            .row_estimated_bytes
            .get(&surrogate.0)
            .copied()
            .unwrap_or(0);
        let row_metadata = if self.row_estimated_bytes.contains_key(&surrogate.0) {
            0
        } else {
            128
        };
        let bytes = row
            .saturating_sub(previous)
            .checked_add(dense)
            .and_then(|bytes| bytes.checked_add(row_metadata))
            .ok_or_else(|| overflow(key))?;
        if bytes == 0 {
            return Ok(());
        }
        let token = memory(governor).reserve(bytes).map_err(|error| LiteError::Backpressure { detail: format!("retained full-text index '{key}' needs {bytes} bytes: {error}. Increase the FTS budget or reduce indexed text") })?;
        self.chunks.push(token);
        self.dense_slots = self.dense_slots.max(slots);
        self.row_estimated_bytes
            .insert(surrogate.0, previous.max(row));
        Ok(())
    }

    pub(super) fn restored(
        governor: Arc<MemoryGovernor>,
        key: &str,
        index: &LiteFtsIndex,
        next_surrogate: u32,
    ) -> Self {
        let mut bytes = 1024usize.saturating_add(key.len().saturating_mul(4));
        let mut row_estimated_bytes: HashMap<u32, usize> = HashMap::new();
        // Same per-occurrence charge as `estimate_row`.
        let per_token = key
            .len()
            .saturating_add(5)
            .saturating_mul(2)
            .saturating_add(320);
        let scope = IndexScope::document(key);
        for term in index.memtable().terms(0, 0, scope) {
            for posting in index.memtable().get_postings(0, 0, scope, &term) {
                let occurrences = if posting.positions.is_empty() {
                    posting.term_freq as usize
                } else {
                    posting.positions.len()
                };
                let cost = per_token
                    .saturating_add(term.len().saturating_mul(4))
                    .saturating_mul(occurrences);
                let row = row_estimated_bytes.entry(posting.doc_id.0).or_default();
                *row = row.saturating_add(cost);
            }
        }
        for row in row_estimated_bytes.values() {
            bytes = bytes.saturating_add(*row).saturating_add(128);
        }
        bytes = bytes.saturating_add((next_surrogate as usize).saturating_mul(16));
        Self {
            chunks: vec![memory(governor).charge(bytes)],
            dense_slots: next_surrogate as usize,
            row_estimated_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_mem::{EngineLimits, GovernorConfig};

    #[test]
    fn dense_surrogate_growth_is_reserved_before_writes() {
        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 1024 * 1024,
                engine_limits: EngineLimits::uniform(4096),
            })
            .unwrap(),
        );
        let mut lease = RetainedIndex::default();
        lease.reserve_base(Arc::clone(&governor), "docs").unwrap();
        lease
            .reserve_write(Arc::clone(&governor), "docs", Surrogate(1), &["x".into()])
            .unwrap();
        assert!(matches!(
            lease.reserve_write(
                Arc::clone(&governor),
                "docs",
                Surrogate(1000),
                &["x".into()]
            ),
            Err(LiteError::Backpressure { .. })
        ));
        let used = governor
            .snapshot()
            .into_iter()
            .find(|entry| entry.engine == EngineId::Fts)
            .unwrap()
            .allocated;
        lease
            .reserve_write(Arc::clone(&governor), "docs", Surrogate(1), &["x".into()])
            .unwrap();
        assert_eq!(
            governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated,
            used
        );
        drop(lease);
        assert_eq!(
            governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated,
            0
        );
    }
    #[test]
    fn anchored_term_migration_reuses_leases_and_admits_a_replacement() {
        use crate::engine::fts::manager::FtsCollectionManager;
        use nodedb_types::{Value, text_search::TextSearchParams};
        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 64 * 1024 * 1024,
                engine_limits: EngineLimits::uniform(768 * 1024),
            })
            .unwrap(),
        );
        let mut manager = FtsCollectionManager::new(Arc::clone(&governor));
        for term in 0..12 {
            manager
                .index_document("docs", &format!("anchor{term}"), &format!("word{term:02}"))
                .unwrap();
        }
        let mut warm = None;
        for term in 0..12 {
            let text = format!("word{term:02}");
            for mover in 0..64 {
                manager
                    .index_document("docs", &format!("mover{mover}"), &text)
                    .unwrap();
            }
            let used = governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated;
            if let Some(warm) = warm {
                assert_eq!(used, warm);
            } else {
                warm = Some(used);
            }
        }
        for term in 0..12 {
            let hits = manager
                .search(
                    "docs",
                    "",
                    &format!("word{term:02}"),
                    100,
                    &TextSearchParams::default(),
                )
                .unwrap();
            assert!(hits.iter().any(|hit| hit.doc_id == format!("anchor{term}")));
        }
        let mut candidate = manager
            .begin_replacement("docs", manager.current_declaration_record("docs"))
            .unwrap();
        for term in 0..12 {
            let id = format!("anchor{term}");
            let surrogate = manager.reserve_surrogate("docs", &id).unwrap();
            candidate
                .index_document_fields(
                    &id,
                    surrogate,
                    &HashMap::from([("body".into(), Value::String(format!("word{term:02}")))]),
                )
                .unwrap();
        }
        for mover in 0..64 {
            let id = format!("mover{mover}");
            let surrogate = manager.reserve_surrogate("docs", &id).unwrap();
            candidate
                .index_document_fields(
                    &id,
                    surrogate,
                    &HashMap::from([("body".into(), Value::String("word11".into()))]),
                )
                .unwrap();
        }
        manager.publish_replacement(candidate);
        assert_eq!(
            manager
                .search("docs", "body", "word11", 100, &TextSearchParams::default())
                .unwrap()
                .len(),
            65
        );
    }
    #[test]
    fn restored_identical_rows_keep_credit_while_token_diversification_reserves_growth() {
        use crate::engine::fts::manager::resident_index;
        let governor = crate::engine::fts::manager::test_governor();
        let index = resident_index(Arc::clone(&governor));
        let tokens = index
            .analyze_for_collection(0, 0, "docs", "abcdef")
            .unwrap();
        index
            .index_analyzed_document(0, 0, "docs", Surrogate(1), &tokens)
            .unwrap();
        let mut lease = RetainedIndex::restored(Arc::clone(&governor), "docs", &index, 2);
        let before = governor
            .snapshot()
            .into_iter()
            .find(|entry| entry.engine == EngineId::Fts)
            .unwrap()
            .allocated;
        lease
            .reserve_write(Arc::clone(&governor), "docs", Surrogate(1), &tokens)
            .unwrap();
        assert_eq!(
            governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated,
            before
        );
        let diverse = index
            .analyze_for_collection(0, 0, "docs", "ab cde")
            .unwrap();
        assert!(diverse.len() > tokens.len());
        lease
            .reserve_write(Arc::clone(&governor), "docs", Surrogate(1), &diverse)
            .unwrap();
        assert!(
            governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated
                > before
        );
    }

    #[test]
    fn whitespace_and_stopwords_charge_no_postings_from_raw_text_size() {
        use crate::engine::fts::manager::FtsCollectionManager;
        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 1024 * 1024,
                engine_limits: EngineLimits::uniform(4096),
            })
            .unwrap(),
        );
        let mut manager = FtsCollectionManager::new(Arc::clone(&governor));
        manager
            .index_document("docs", "id", &" ".repeat(16384))
            .unwrap();
        let base = governor
            .snapshot()
            .into_iter()
            .find(|entry| entry.engine == EngineId::Fts)
            .unwrap()
            .allocated;
        manager
            .index_document("docs", "id", &"the and ".repeat(2048))
            .unwrap();
        assert_eq!(
            governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated,
            base
        );
        assert!(manager.indices["docs"].memtable().is_empty());
    }

    #[test]
    fn candidate_budget_refusal_preserves_prior_search_and_releases_private_credit() {
        use crate::engine::fts::manager::FtsCollectionManager;
        use nodedb_types::{Value, text_search::TextSearchParams};
        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: 1024 * 1024,
                engine_limits: EngineLimits::uniform(4096),
            })
            .unwrap(),
        );
        let mut manager = FtsCollectionManager::new(Arc::clone(&governor));
        manager.index_document("docs", "id", "alpha").unwrap();
        let prior = governor
            .snapshot()
            .into_iter()
            .find(|entry| entry.engine == EngineId::Fts)
            .unwrap()
            .allocated;
        let mut candidate = manager
            .begin_replacement("docs", manager.current_declaration_record("docs"))
            .unwrap();
        let surrogate = manager.reserve_surrogate("docs", "id").unwrap();
        assert!(matches!(
            candidate.index_document_fields(
                "id",
                surrogate,
                &HashMap::from([(
                    "body".into(),
                    Value::String("beta gamma delta epsilon zeta eta theta iota".into())
                )])
            ),
            Err(LiteError::Backpressure { .. })
        ));
        assert_eq!(
            manager
                .search("docs", "", "alpha", 10, &TextSearchParams::default())
                .unwrap()
                .len(),
            1
        );
        drop(candidate);
        assert_eq!(
            governor
                .snapshot()
                .into_iter()
                .find(|entry| entry.engine == EngineId::Fts)
                .unwrap()
                .allocated,
            prior
        );
    }
}
