// SPDX-License-Identifier: Apache-2.0

//! [`FtsCollectionManager`] state: the index map, the dense surrogate maps,
//! index-key naming, error mapping, and checkpoint accessors.
//!
//! Wraps `nodedb_fts::FtsIndex<MemoryBackend>` with per-collection management:
//! - Incremental insert/remove on document put/delete
//! - Per-collection keying: `"{collection}:{field}"` per field, plus the bare
//!   `"{collection}"` for the whole-document index
//! - BM25 search delegated directly to nodedb-fts (BMW, analyzers, fuzzy)
//! - Persistent: checkpoint serialized to `Namespace::Fts` on `flush()`,
//!   restored on `NodeDbLite::open` without re-tokenizing source documents.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use nodedb_fts::FtsIndex;
use nodedb_fts::FtsIndexError;
use nodedb_fts::backend::memory::MemoryBackend;
use nodedb_fts::lsm::memtable::MemtableConfig;
use nodedb_mem::MemoryGovernor;
use nodedb_types::Surrogate;

use crate::engine::fts::LiteFtsIndex;
use crate::error::LiteError;

/// A resolved FTS result with the original string doc_id restored.
pub struct FtsResult {
    pub doc_id: String,
    pub score: f32,
    pub fuzzy: bool,
}

/// Index key for `field` of `collection`. An empty `field` names the
/// whole-document index.
///
/// The whole-document key is the bare collection name. Every field key
/// carries a `:` after the collection name, so no non-empty field name —
/// `_doc` included — can produce the whole-document key.
pub fn index_key(collection: &str, field: &str) -> String {
    if field.is_empty() {
        collection.to_owned()
    } else {
        format!("{collection}:{field}")
    }
}

/// Whether index `key` belongs to `collection`: its whole-document key or
/// one of its `"{collection}:{field}"` keys.
pub(crate) fn key_of_collection(key: &str, collection: &str) -> bool {
    key.strip_prefix(collection)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
}

/// Create an index that keeps every posting in its memtable.
///
/// Lite serves phrase positions and checkpoints postings from the memtable
/// alone. A memtable that spilled into a backend segment would drop those
/// postings from both, so the spill thresholds are set out of reach.
pub(crate) fn resident_index(governor: Arc<MemoryGovernor>) -> LiteFtsIndex {
    FtsIndex::with_memtable_config(
        MemoryBackend::new(),
        MemtableConfig {
            max_postings: usize::MAX,
            max_terms: usize::MAX,
        },
        governor,
    )
}

/// Wrap an index-layer write failure as the typed error the write path
/// propagates.
pub(in crate::engine::fts) fn fts_err(collection: &str, e: impl Display) -> LiteError {
    LiteError::FtsIndex {
        collection: collection.to_owned(),
        detail: e.to_string(),
    }
}

/// Classify an index-layer read failure: a query the index cannot run is
/// the caller's error, a memory budget refusal is backpressure, anything
/// else is a failed search.
pub(in crate::engine::fts) fn search_err<E: Display>(
    collection: &str,
    e: FtsIndexError<E>,
) -> LiteError {
    match e {
        FtsIndexError::InvalidQuery(q) => LiteError::FtsQueryInvalid {
            collection: collection.to_owned(),
            detail: q.to_string(),
        },
        FtsIndexError::BudgetExhausted(m) => LiteError::Backpressure {
            detail: format!("full-text search on {collection}: {m}"),
        },
        other => read_err(collection, other),
    }
}

/// Wrap a backend read failure as a failed search.
pub(in crate::engine::fts) fn read_err(collection: &str, e: impl Display) -> LiteError {
    LiteError::FtsSearch {
        collection: collection.to_owned(),
        detail: e.to_string(),
    }
}

/// Manages per-collection (and per-field) in-memory full-text search indexes.
///
/// Each `(collection, field)` pair gets its own `FtsIndex<MemoryBackend>`.
/// The bare `"{collection}"` key holds whole-document text (all string
/// fields concatenated), searched when a caller names no field.
pub struct FtsCollectionManager {
    pub(super) retained: HashMap<String, super::retained::RetainedIndex>,
    pub(super) declarations:
        std::collections::BTreeMap<String, crate::engine::fts::catalog::SearchDeclarationRecord>,
    /// Key: `"{collection}:{field}"` → FTS index.
    /// Whole-document index uses the bare key `"{collection}"`.
    pub(in crate::engine::fts) indices: HashMap<String, FtsIndex<MemoryBackend>>,
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
    pub(super) surrogate_to_id: HashMap<u32, String>,
    /// Next surrogate to assign on first sighting of a doc_id.
    next_surrogate: u32,
    /// Reverse map: Origin global surrogate → Lite string doc_id.
    ///
    /// Populated when `FtsIndexDoc` frames arrive from Origin via the sync path.
    /// Needed by `FtsDeleteDoc` to translate the Origin surrogate back to the
    /// Lite string doc_id without dropping the whole collection.
    pub(super) origin_surrogate_to_doc_id: HashMap<u32, String>,
    /// Collection name → bound analyzer name, from `TextOp::SetTextConfig`.
    ///
    /// Retained so indexes created after the analyzer was bound inherit it —
    /// DDL normally binds the analyzer before the first document is written,
    /// when none of the collection's indexes exist yet. See
    /// `super::super::analyzer` for the binding logic.
    pub(in crate::engine::fts) collection_analyzers: HashMap<String, String>,
    /// Collection name → default fuzzy matching, from `TextOp::SetTextConfig`.
    ///
    /// Retained for the same reason as `collection_analyzers`: the DDL that
    /// sets it usually runs before any of the collection's indexes exist.
    pub(in crate::engine::fts) collection_fuzzy_defaults: HashMap<String, bool>,
    /// Memory governor bound into every `FtsIndex` this manager creates.
    pub(in crate::engine::fts) governor: Arc<MemoryGovernor>,
}

impl FtsCollectionManager {
    pub fn new(governor: Arc<MemoryGovernor>) -> Self {
        Self {
            retained: HashMap::new(),
            declarations: std::collections::BTreeMap::new(),
            indices: HashMap::new(),
            id_to_surrogate: HashMap::new(),
            surrogate_to_id: HashMap::new(),
            // Start at 1: Surrogate(0) is the unassigned sentinel and is
            // rejected by FtsIndex::index_document with SurrogateOutOfRange.
            next_surrogate: 1,
            origin_surrogate_to_doc_id: HashMap::new(),
            collection_analyzers: HashMap::new(),
            collection_fuzzy_defaults: HashMap::new(),
            governor,
        }
    }

    /// Look up or allocate a dense surrogate for a string `doc_id`.
    ///
    /// Returns the existing surrogate if `doc_id` has been indexed before,
    /// otherwise assigns the next sequential u32 and records the mapping
    /// in both directions. Fails when the u32 surrogate space is used up.
    pub(super) fn surrogate_for(
        &mut self,
        collection: &str,
        doc_id: &str,
    ) -> Result<Surrogate, LiteError> {
        if let Some(&s) = self.id_to_surrogate.get(doc_id) {
            return Ok(Surrogate(s));
        }
        let s = self.next_surrogate;
        self.next_surrogate = s.checked_add(1).ok_or_else(|| {
            fts_err(
                collection,
                format!(
                    "no surrogate left for document '{doc_id}': all u32 surrogates are assigned"
                ),
            )
        })?;
        self.id_to_surrogate.insert(doc_id.to_owned(), s);
        self.surrogate_to_id.insert(s, doc_id.to_owned());
        Ok(Surrogate(s))
    }

    /// Look up an existing surrogate without allocating one.
    pub(super) fn lookup_surrogate(&self, doc_id: &str) -> Option<Surrogate> {
        self.id_to_surrogate.get(doc_id).copied().map(Surrogate)
    }

    /// The string doc_id a surrogate from `key`'s postings stands for.
    ///
    /// Surrogates are never released, so a posting whose surrogate has no
    /// doc_id means the index and the surrogate maps disagree.
    pub(super) fn doc_id_of(&self, collection: &str, s: Surrogate) -> Result<&str, LiteError> {
        self.surrogate_to_id
            .get(&s.0)
            .map(String::as_str)
            .ok_or_else(|| {
                read_err(
                    collection,
                    format!("posting references surrogate {} with no document id", s.0),
                )
            })
    }

    /// Returns true if no collections are indexed.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Number of distinct collection prefixes with active indexes.
    pub fn collection_count(&self) -> usize {
        self.indices
            .keys()
            .map(|k| k.split(':').next().unwrap_or(k.as_str()))
            .collect::<HashSet<_>>()
            .len()
    }

    /// Keys of every index belonging to `collection`, per-field and
    /// whole-document alike.
    pub(in crate::engine::fts) fn collection_keys(&self, collection: &str) -> Vec<String> {
        self.indices
            .keys()
            .filter(|k| key_of_collection(k, collection))
            .cloned()
            .collect()
    }

    /// Whether any document of `collection` was ever text-indexed.
    pub fn has_text_index(&self, collection: &str) -> bool {
        self.indices
            .keys()
            .any(|k| key_of_collection(k, collection))
    }

    /// Drop all FTS indexes for a collection (called on collection drop/truncate).
    pub fn drop_collection(&mut self, collection: &str) {
        self.remove_collection_postings(collection);
    }

    // ── Checkpoint helpers (used by core.rs flush/restore) ────────────────────

    /// Borrow the index map, surrogate map, and next-surrogate counter for
    /// serialization.  Called by `checkpoint::serialize_fts`.
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
    /// `restore_fts_indices` when a valid checkpoint is found.
    pub(crate) fn load_checkpoint(
        &mut self,
        indices: HashMap<String, FtsIndex<MemoryBackend>>,
        id_to_surrogate: HashMap<String, u32>,
        surrogate_to_id: HashMap<u32, String>,
        next_surrogate: u32,
    ) {
        self.retained = indices
            .iter()
            .map(|(key, index)| {
                (
                    key.clone(),
                    super::retained::RetainedIndex::restored(
                        Arc::clone(&self.governor),
                        key,
                        index,
                    ),
                )
            })
            .collect();
        self.indices = indices;
        self.id_to_surrogate = id_to_surrogate;
        self.surrogate_to_id = surrogate_to_id;
        self.next_surrogate = next_surrogate;
        // origin_surrogate_to_doc_id is not persisted across restarts because
        // origin surrogates are only relevant for the lifetime of a sync session;
        // FtsIndexDoc frames re-register the mapping on re-sync.
    }
}

/// Build a real, uncapped governor for FTS tests across `engine::fts`.
#[cfg(test)]
pub(crate) fn test_governor() -> Arc<MemoryGovernor> {
    use nodedb_mem::{EngineLimits, GovernorConfig};

    let per_engine = usize::MAX / nodedb_mem::EngineId::ALL.len();
    Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: per_engine * nodedb_mem::EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(per_engine),
        })
        .expect("test governor"),
    )
}

#[cfg(test)]
mod tests {
    use super::{index_key, key_of_collection};

    #[test]
    fn no_field_name_produces_the_whole_document_key() {
        let whole = index_key("col", "");
        assert_eq!(whole, "col");
        for field in ["_doc", ":", "doc", "col"] {
            assert_ne!(index_key("col", field), whole, "field {field:?}");
        }
        assert_eq!(index_key("col", "_doc"), "col:_doc");
    }

    #[test]
    fn keys_are_owned_by_their_collection_only() {
        assert!(key_of_collection("col", "col"));
        assert!(key_of_collection("col:title", "col"));
        assert!(!key_of_collection("column", "col"));
        assert!(!key_of_collection("column:title", "col"));
    }
}
