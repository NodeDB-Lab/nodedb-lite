// SPDX-License-Identifier: Apache-2.0

//! The index catalog: definitions and entries held in memory, persisted with
//! the CRDT state they derive from.
//!
//! Schemaless rows live in memory in the CRDT store and reach disk only when
//! a flush writes the CRDT state. Index entries follow the same contract. The
//! entries a write changes are applied here under the CRDT lock that guards
//! the row write, and recorded as dirty. A flush stages the dirty keys into
//! the same storage batch as the CRDT state, under the same lock hold. The
//! stored rows and the stored entries therefore always describe the same
//! instant: a crash between flushes loses the row write and its entries
//! together, and no post-crash repair is needed for rows held in the CRDT
//! store.
//!
//! Index definitions are persisted the same way, so an index created or
//! dropped after the last flush is lost with the rows written after it.
//!
//! Strict and key-value rows are written to storage on every write. Their
//! index entries and definitions are written in the same storage batch as
//! the change that causes them (see `durable` and `ddl`), never through the
//! flush.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::catalog::{IndexDef, IndexEngine};
use super::key;
use super::maintain::IndexWriteOp;
use super::sorted_def::SortedIndexDef;

/// A key whose stored form is behind memory: `Some` bytes to put, `None` to
/// delete. `seq` identifies the change so a flush acknowledges only the
/// version it wrote.
struct Dirty {
    seq: u64,
    value: Option<Vec<u8>>,
}

/// Mutable catalog state behind [`IndexCatalog`]'s lock.
#[derive(Default)]
pub(crate) struct IndexState {
    /// Every definition, by index name.
    pub(super) defs: BTreeMap<String, Arc<IndexDef>>,
    /// Every sorted-index definition, by index name. Its entries live only
    /// in storage.
    pub(super) sorted: BTreeMap<String, Arc<SortedIndexDef>>,
    /// Every entry key, in storage order.
    pub(super) entries: BTreeSet<Vec<u8>>,
    /// Entry keys per `(collection, document id)`: what a row currently
    /// contributes, so a changed row's old entries are known without its old
    /// value.
    pub(super) postings: HashMap<(String, String), BTreeSet<Vec<u8>>>,
    /// Bitemporal rows deleted in history whose CRDT copy remains. They hold
    /// no entries whatever their CRDT copy holds.
    pub(super) tombstoned: HashSet<(String, String)>,
    dirty: HashMap<Vec<u8>, Dirty>,
    next_seq: u64,
    /// While set, changes are collected as storage writes for the caller to
    /// commit instead of being left for the flush.
    sink: Option<Vec<WriteOp>>,
}

impl IndexState {
    /// Definitions on `collection` that cover `engine` rows.
    pub(super) fn defs_on(&self, collection: &str, engine: IndexEngine) -> Vec<Arc<IndexDef>> {
        self.defs
            .values()
            .filter(|d| d.collection == collection && d.engine == engine)
            .cloned()
            .collect()
    }

    fn mark(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        if let Some(sink) = &mut self.sink {
            sink.push(match value {
                Some(value) => WriteOp::Put {
                    ns: Namespace::Meta,
                    key,
                    value,
                },
                None => WriteOp::Delete {
                    ns: Namespace::Meta,
                    key,
                },
            });
            return;
        }
        self.next_seq += 1;
        let seq = self.next_seq;
        self.dirty.insert(key, Dirty { seq, value });
    }

    /// A state holding only `defs` and their entries: DDL runs a change here
    /// first to learn the storage writes it needs, and applies the change to
    /// the real state only once those writes are stored.
    pub(super) fn scratch(&self, defs: &[Arc<IndexDef>]) -> IndexState {
        let mut scratch = IndexState::default();
        for def in defs {
            scratch.defs.insert(def.name.clone(), Arc::clone(def));
            let prefix = def.entry_prefix();
            for entry in self
                .entries
                .range(prefix.clone()..)
                .take_while(|k| k.starts_with(&prefix))
            {
                if let Some(parsed) = key::parse_entry(entry) {
                    scratch
                        .postings
                        .entry((parsed.collection.to_string(), parsed.doc_id.to_string()))
                        .or_default()
                        .insert(entry.clone());
                }
                scratch.entries.insert(entry.clone());
            }
        }
        scratch
    }

    /// Run `f`, returning the storage writes of its changes instead of
    /// leaving them for the flush when `durable`. For an index whose rows
    /// are written to storage directly: the caller commits the writes.
    pub(super) fn collecting<R>(
        &mut self,
        durable: bool,
        f: impl FnOnce(&mut Self) -> R,
    ) -> (R, Vec<WriteOp>) {
        if !durable {
            return (f(self), Vec::new());
        }
        let outer = self.sink.replace(Vec::new());
        let result = f(self);
        let ops = self.sink.take().unwrap_or_default();
        self.sink = outer;
        (result, ops)
    }

    /// Apply entry changes for one row of `collection`.
    pub(super) fn apply(&mut self, collection: &str, doc_id: &str, ops: Vec<IndexWriteOp>) {
        if ops.is_empty() {
            return;
        }
        let posting_key = (collection.to_string(), doc_id.to_string());
        for op in ops {
            match op {
                IndexWriteOp::Put(entry) => {
                    self.postings
                        .entry(posting_key.clone())
                        .or_default()
                        .insert(entry.clone());
                    self.entries.insert(entry.clone());
                    self.mark(entry, Some(Vec::new()));
                }
                IndexWriteOp::Delete(entry) => {
                    if let Some(keys) = self.postings.get_mut(&posting_key) {
                        keys.remove(&entry);
                        if keys.is_empty() {
                            self.postings.remove(&posting_key);
                        }
                    }
                    self.entries.remove(&entry);
                    self.mark(entry, None);
                }
            }
        }
    }

    /// Remove every entry whose key starts with `prefix`.
    pub(super) fn clear_prefix(&mut self, prefix: &[u8]) {
        let doomed: Vec<Vec<u8>> = self
            .entries
            .range(prefix.to_vec()..)
            .take_while(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for entry in doomed {
            if let Some(parsed) = key::parse_entry(&entry) {
                let posting_key = (parsed.collection.to_string(), parsed.doc_id.to_string());
                if let Some(keys) = self.postings.get_mut(&posting_key) {
                    keys.remove(&entry);
                    if keys.is_empty() {
                        self.postings.remove(&posting_key);
                    }
                }
            }
            self.entries.remove(&entry);
            self.mark(entry, None);
        }
    }

    /// Record a definition as present.
    pub(super) fn put_def(&mut self, def: Arc<IndexDef>) -> Result<(), LiteError> {
        let bytes = def.encode()?;
        self.mark(key::def_key(&def.collection, &def.name), Some(bytes));
        self.defs.insert(def.name.clone(), def);
        Ok(())
    }

    /// Remove a definition and every entry it owns.
    pub(super) fn remove_def(&mut self, name: &str) -> Option<Arc<IndexDef>> {
        let def = self.defs.remove(name)?;
        self.clear_prefix(&def.entry_prefix());
        self.mark(key::def_key(&def.collection, &def.name), None);
        Some(def)
    }
}

/// Everything a flush staged, acknowledged once its batch commits.
pub(crate) struct IndexFlush {
    written: Vec<(Vec<u8>, u64)>,
}

/// What loading the stored catalog found.
pub(crate) struct LoadOutcome {
    /// The stored entries were written in another format (or none were
    /// recorded as written in this one) and every index must be rebuilt.
    pub rebuild_all: bool,
}

/// Secondary-index definitions and entries, shared by the query engine, the
/// CRDT store's write path and flush.
///
/// Lock order: the CRDT lock, when held, is taken before this catalog's lock.
#[derive(Default)]
pub struct IndexCatalog {
    state: Mutex<IndexState>,
    /// Held for reading by every strict and key-value row write, and for
    /// writing by index DDL on those engines, so no row write lands between
    /// a build's row read and its install, or runs against a definition
    /// being dropped or moved.
    pub(crate) durable_ddl: tokio::sync::RwLock<()>,
    /// Serializes strict and key-value row writes to indexed collections from
    /// entry planning through commit, so each plans from the entries the
    /// previous one left.
    pub(crate) durable_rows: tokio::sync::Mutex<()>,
    /// Held for writing while an index on a bitemporal collection is built from
    /// its history, and for reading by every bitemporal document write, so no
    /// write lands between the history read and the build.
    pub(crate) bitemporal_build: tokio::sync::RwLock<()>,
}

impl IndexCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, IndexState> {
        self.state.lock_or_recover()
    }

    /// Load stored definitions and entries. Call once at open, before any
    /// write reaches the catalog.
    pub(crate) async fn load<S: StorageEngine>(
        &self,
        storage: &S,
    ) -> Result<LoadOutcome, LiteError> {
        let format = storage.get(Namespace::Meta, &key::format_key()).await?;
        let defs = storage
            .scan_prefix(Namespace::Meta, &key::defs_prefix())
            .await?;
        let entries = storage
            .scan_prefix(Namespace::Meta, &key::entries_prefix())
            .await?;
        let sorted = storage
            .scan_prefix(Namespace::Meta, &key::sorted_defs_prefix())
            .await?
            .iter()
            .map(|(_, bytes)| SortedIndexDef::decode(bytes))
            .collect::<Result<Vec<_>, _>>()?;

        let current = format.as_deref() == Some(key::FORMAT_VERSION.to_be_bytes().as_slice());
        let stored_nothing = defs.is_empty() && entries.is_empty();
        let decoded = defs
            .iter()
            .map(|(_, bytes)| IndexDef::decode(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let (rebuild_all, fresh) = {
            let mut state = self.lock();
            for def in decoded {
                state.defs.insert(def.name.clone(), Arc::new(def));
            }
            for def in sorted {
                state.sorted.insert(def.name.clone(), Arc::new(def));
            }
            for (entry, _) in entries {
                if current && let Some(parsed) = key::parse_entry(&entry) {
                    state
                        .postings
                        .entry((parsed.collection.to_string(), parsed.doc_id.to_string()))
                        .or_default()
                        .insert(entry.clone());
                    state.entries.insert(entry);
                } else {
                    // Written in another format: unreadable here, removed at the
                    // next flush and rebuilt from rows.
                    state.mark(entry, None);
                }
            }
            let rebuild_all = !current && !state.defs.is_empty();
            let fresh = !current && stored_nothing;
            if !current && !fresh {
                // Rewritten with the rebuilt entries at the next flush.
                state.mark(
                    key::format_key(),
                    Some(key::FORMAT_VERSION.to_be_bytes().to_vec()),
                );
            }
            (rebuild_all, fresh)
        };
        if fresh {
            // Nothing is stored in any format: record the current one now,
            // so strict and key-value entries written before the first flush
            // are read as current at the next open.
            storage
                .put(
                    Namespace::Meta,
                    &key::format_key(),
                    &key::FORMAT_VERSION.to_be_bytes(),
                )
                .await?;
        }
        Ok(LoadOutcome { rebuild_all })
    }

    /// Stage every dirty key into `ops`. Call under the CRDT lock, in the
    /// same hold that plans the CRDT writes of the same batch.
    pub(crate) fn stage_flush(&self, ops: &mut Vec<WriteOp>) -> IndexFlush {
        let state = self.lock();
        let mut written = Vec::with_capacity(state.dirty.len());
        for (k, dirty) in &state.dirty {
            ops.push(match &dirty.value {
                Some(value) => WriteOp::Put {
                    ns: Namespace::Meta,
                    key: k.clone(),
                    value: value.clone(),
                },
                None => WriteOp::Delete {
                    ns: Namespace::Meta,
                    key: k.clone(),
                },
            });
            written.push((k.clone(), dirty.seq));
        }
        IndexFlush { written }
    }

    /// Forget the dirty marks a committed flush wrote. A key changed again
    /// since it was staged keeps its newer mark.
    pub(crate) fn mark_flushed(&self, flush: IndexFlush) {
        let mut state = self.lock();
        for (k, seq) in flush.written {
            if state.dirty.get(&k).is_some_and(|d| d.seq == seq) {
                state.dirty.remove(&k);
            }
        }
    }

    /// Whether `collection` has an index whose entries its CRDT rows feed.
    pub(crate) fn is_indexed(&self, collection: &str) -> bool {
        self.has_defs(collection, IndexEngine::Document)
    }

    /// Whether `collection` has an index over `engine` rows.
    pub(crate) fn has_defs(&self, collection: &str, engine: IndexEngine) -> bool {
        self.lock()
            .defs
            .values()
            .any(|d| d.collection == collection && d.engine == engine)
    }

    /// Every definition, in name order.
    pub(crate) fn defs(&self) -> Vec<Arc<IndexDef>> {
        self.lock().defs.values().cloned().collect()
    }

    /// The definition named `name`.
    pub(crate) fn def_named(&self, name: &str) -> Option<Arc<IndexDef>> {
        self.lock().defs.get(name).cloned()
    }

    /// The index on `collection` over the field `field_spec` (canonical path,
    /// `[]`-suffixed for an array index).
    pub(crate) fn def_on_field(&self, collection: &str, field_spec: &str) -> Option<Arc<IndexDef>> {
        self.lock()
            .defs
            .values()
            .find(|d| d.collection == collection && d.field_spec() == field_spec)
            .cloned()
    }

    /// The sorted indexes on `collection`, in name order.
    pub(crate) fn sorted_on(&self, collection: &str) -> Vec<Arc<SortedIndexDef>> {
        self.lock()
            .sorted
            .values()
            .filter(|d| d.collection == collection)
            .cloned()
            .collect()
    }

    /// The sorted index named `name`.
    pub(crate) fn sorted_named(&self, name: &str) -> Option<Arc<SortedIndexDef>> {
        self.lock().sorted.get(name).cloned()
    }

    /// Whether `collection` has a sorted index.
    pub(crate) fn has_sorted(&self, collection: &str) -> bool {
        self.lock()
            .sorted
            .values()
            .any(|d| d.collection == collection)
    }

    /// Every sorted index, grouped by the collection it ranks.
    pub(crate) fn sorted_by_collection(&self) -> HashMap<String, Vec<Arc<SortedIndexDef>>> {
        let mut grouped: HashMap<String, Vec<Arc<SortedIndexDef>>> = HashMap::new();
        for def in self.lock().sorted.values() {
            grouped
                .entry(def.collection.clone())
                .or_default()
                .push(Arc::clone(def));
        }
        grouped
    }

    /// What the planner sees for `collection`: its document and strict
    /// indexes, the engines whose equality lookups the planner rewrites.
    pub(crate) fn planner_specs(&self, collection: &str) -> Vec<nodedb_sql::types::IndexSpec> {
        self.lock()
            .defs
            .values()
            .filter(|d| {
                d.collection == collection
                    && matches!(d.engine, IndexEngine::Document | IndexEngine::Strict)
            })
            .map(|d| d.planner_spec())
            .collect()
    }
}
