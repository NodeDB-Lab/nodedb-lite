// SPDX-License-Identifier: Apache-2.0

//! Row writes of engines that store every write directly — strict and
//! key-value — with their index entries in the same storage batch.
//!
//! The write's unique check runs before the batch is committed, and the
//! catalog's memory follows only once it is, so a refused or failed write
//! leaves rows, stored entries and memory as they were.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;

use nodedb_types::Namespace;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::catalog::IndexEngine;
use super::maintain::{IndexWriteOp, plan_index_ops, unique_candidates};
use super::store::IndexCatalog;

/// One row a write changes: its contents after the write, `None` when the
/// write removes it.
pub(crate) struct RowImage {
    pub collection: String,
    pub doc_id: String,
    pub row: Option<Value>,
}

/// Entry changes planned for one row.
type RowEntryOps = (String, String, Vec<IndexWriteOp>);

type CollectionRowImages<'a> = (&'a str, Vec<(&'a str, Option<Value>)>);

impl IndexCatalog {
    /// The entry changes `images` imply, after refusing any a unique index
    /// forbids. Call with `durable_rows` held.
    async fn plan_checked<R, F>(
        &self,
        engine: IndexEngine,
        images: &[RowImage],
        read_row: &R,
    ) -> Result<Vec<RowEntryOps>, LiteError>
    where
        R: Fn(String, String) -> F,
        F: Future<Output = Result<Option<Value>, LiteError>>,
    {
        let (changes, candidates) = {
            let state = self.lock();
            let mut collection_slots: HashMap<&str, usize> = HashMap::new();
            let mut collections: Vec<CollectionRowImages<'_>> = Vec::new();
            for image in images {
                let next_slot = collections.len();
                let slot = *collection_slots
                    .entry(image.collection.as_str())
                    .or_insert_with(|| {
                        collections.push((&image.collection, Vec::new()));
                        next_slot
                    });
                collections[slot]
                    .1
                    .push((image.doc_id.as_str(), image.row.clone()));
            }
            let mut candidates = Vec::new();
            for (collection, rows) in &collections {
                candidates.extend(unique_candidates(&state, collection, engine, rows)?);
            }

            // A row can appear twice in one write (a key moved, then written),
            // so each plans from the entries the previous image left.
            let mut held: HashMap<(String, String), BTreeSet<Vec<u8>>> = HashMap::new();
            let mut changes: Vec<RowEntryOps> = Vec::with_capacity(images.len());
            for image in images {
                let defs = state.defs_on(&image.collection, engine);
                if defs.is_empty() {
                    continue;
                }
                let posting_key = (image.collection.clone(), image.doc_id.clone());
                let old = held
                    .get(&posting_key)
                    .or_else(|| state.postings.get(&posting_key))
                    .cloned()
                    .unwrap_or_default();
                let planned = plan_index_ops(&defs, &image.doc_id, &old, image.row.as_ref());
                let mut now = old;
                for op in &planned {
                    match op {
                        IndexWriteOp::Put(k) => now.insert(k.clone()),
                        IndexWriteOp::Delete(k) => now.remove(k),
                    };
                }
                held.insert(posting_key, now);
                changes.push((image.collection.clone(), image.doc_id.clone(), planned));
            }
            (changes, candidates)
        };

        for candidate in &candidates {
            let stored =
                read_row(candidate.def.collection.clone(), candidate.other.clone()).await?;
            if stored.is_some_and(|row| candidate.held_by(&row)) {
                return Err(candidate.violation());
            }
        }
        Ok(changes)
    }

    /// Refuse the rows in `images` as [`Self::commit_rows`] would, without
    /// writing anything: for a caller that must know before it records the
    /// write elsewhere, and that keeps other writers out until it commits.
    pub(crate) async fn check_rows<R, F>(
        &self,
        engine: IndexEngine,
        images: &[RowImage],
        read_row: R,
    ) -> Result<(), LiteError>
    where
        R: Fn(String, String) -> F,
        F: Future<Output = Result<Option<Value>, LiteError>>,
    {
        let _ddl = self.durable_ddl.read().await;
        if !images
            .iter()
            .any(|image| self.has_defs(&image.collection, engine))
        {
            return Ok(());
        }
        let _rows = self.durable_rows.lock().await;
        self.plan_checked(engine, images, &read_row)
            .await
            .map(|_| ())
    }

    /// Commit `ops` — the row writes of `engine` — with the index entries the
    /// rows in `images` imply, as one storage batch.
    ///
    /// Before committing, a unique index a row would give a value another row
    /// already holds refuses the write; `read_row(collection, doc_id)` supplies
    /// the stored row each candidate conflict is confirmed against.
    pub(crate) async fn commit_rows<S, R, F>(
        &self,
        storage: &S,
        engine: IndexEngine,
        ops: Vec<WriteOp>,
        images: Vec<RowImage>,
        read_row: R,
    ) -> Result<(), LiteError>
    where
        S: StorageEngine,
        R: Fn(String, String) -> F,
        F: Future<Output = Result<Option<Value>, LiteError>>,
    {
        self.commit_rows_with(storage, engine, ops, images, read_row, |ops| async move {
            Ok(ops)
        })
        .await
    }

    /// [`Self::commit_rows`], with `extra` completing the batch.
    ///
    /// `extra` receives the row writes and their secondary-index entries
    /// once the unique checks pass, and returns the whole batch to commit.
    /// It runs only when a written collection has a secondary or sorted
    /// index, with the definitions fixed and no other row write of an
    /// indexed collection in progress, so it plans from stored state no
    /// other write changes before the batch commits.
    pub(crate) async fn commit_rows_with<S, R, F, X, XF>(
        &self,
        storage: &S,
        engine: IndexEngine,
        ops: Vec<WriteOp>,
        images: Vec<RowImage>,
        read_row: R,
        extra: X,
    ) -> Result<(), LiteError>
    where
        S: StorageEngine,
        R: Fn(String, String) -> F,
        F: Future<Output = Result<Option<Value>, LiteError>>,
        X: FnOnce(Vec<WriteOp>) -> XF,
        XF: Future<Output = Result<Vec<WriteOp>, LiteError>>,
    {
        let _ddl = self.durable_ddl.read().await;
        self.commit_rows_held(storage, engine, ops, images, read_row, extra)
            .await
    }

    /// [`Self::commit_rows_with`] for a caller that holds `durable_ddl`,
    /// for reading or for writing.
    pub(crate) async fn commit_rows_held<S, R, F, X, XF>(
        &self,
        storage: &S,
        engine: IndexEngine,
        mut ops: Vec<WriteOp>,
        images: Vec<RowImage>,
        read_row: R,
        extra: X,
    ) -> Result<(), LiteError>
    where
        S: StorageEngine,
        R: Fn(String, String) -> F,
        F: Future<Output = Result<Option<Value>, LiteError>>,
        X: FnOnce(Vec<WriteOp>) -> XF,
        XF: Future<Output = Result<Vec<WriteOp>, LiteError>>,
    {
        let indexed = images
            .iter()
            .any(|image| self.has_defs(&image.collection, engine));
        let sorted_touched = images
            .iter()
            .any(|image| self.has_sorted(&image.collection));
        if !indexed && !sorted_touched {
            return if ops.is_empty() {
                Ok(())
            } else {
                storage.batch_write(&ops).await
            };
        }
        let _rows = self.durable_rows.lock().await;
        let changes = if indexed {
            self.plan_checked(engine, &images, &read_row).await?
        } else {
            Vec::new()
        };

        for (_, _, entry_ops) in &changes {
            for op in entry_ops {
                ops.push(match op {
                    IndexWriteOp::Put(k) => WriteOp::Put {
                        ns: Namespace::Meta,
                        key: k.clone(),
                        value: Vec::new(),
                    },
                    IndexWriteOp::Delete(k) => WriteOp::Delete {
                        ns: Namespace::Meta,
                        key: k.clone(),
                    },
                });
            }
        }
        let ops = extra(ops).await?;
        if !ops.is_empty() {
            storage.batch_write(&ops).await?;
        }

        // Committed: memory follows. The writes above already stored it.
        let mut state = self.lock();
        let ((), _stored) = state.collecting(true, |state| {
            for (collection, doc_id, entry_ops) in changes {
                state.apply(&collection, &doc_id, entry_ops);
            }
        });
        Ok(())
    }
}
