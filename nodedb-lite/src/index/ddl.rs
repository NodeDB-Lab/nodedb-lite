// SPDX-License-Identifier: Apache-2.0

//! Catalog changes made by DDL: dropping and moving indexes, and
//! clearing a collection's entries.
//!
//! A document index's changes are left for the flush, like the CRDT state
//! they follow. A strict or key-value index's changes are written to storage
//! before the call returns, like the rows they follow. Storage changes
//! first: each change runs on a scratch copy of the indexes it touches to
//! learn its storage writes, and reaches memory only once they are stored,
//! so a failed write leaves memory and storage as they were.

use std::sync::Arc;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::catalog::{IndexDef, IndexEngine};
use super::store::{IndexCatalog, IndexState};

fn durable(def: &IndexDef) -> bool {
    def.engine != IndexEngine::Document
}

/// The definitions on `collection`.
fn defs_of(state: &IndexState, collection: &str) -> Vec<Arc<IndexDef>> {
    state
        .defs
        .values()
        .filter(|d| d.collection == collection)
        .cloned()
        .collect()
}

async fn write<S: StorageEngine>(storage: &S, ops: Vec<WriteOp>) -> Result<(), LiteError> {
    if ops.is_empty() {
        Ok(())
    } else {
        storage.batch_write(&ops).await
    }
}

impl IndexCatalog {
    /// Apply `change` to the definitions `select` picks: on a scratch copy
    /// first, to learn the storage writes of its strict and key-value
    /// indexes; then, once those are stored, on the catalog itself.
    async fn apply_ddl<S, R>(
        &self,
        storage: &S,
        select: impl Fn(&IndexState) -> Vec<Arc<IndexDef>>,
        change: impl Fn(&mut IndexState, &[Arc<IndexDef>]) -> Result<(R, Vec<WriteOp>), LiteError>,
    ) -> Result<R, LiteError>
    where
        S: StorageEngine,
    {
        // No strict or key-value row write runs until the change is applied.
        let _ddl = self.durable_ddl.write().await;
        self.apply_locked(storage, Vec::new(), select, change).await
    }

    /// [`Self::apply_ddl`] with `durable_ddl` already held for write, and
    /// `extra` committed in the same storage batch as the index writes.
    async fn apply_locked<S, R>(
        &self,
        storage: &S,
        extra: Vec<WriteOp>,
        select: impl Fn(&IndexState) -> Vec<Arc<IndexDef>>,
        change: impl Fn(&mut IndexState, &[Arc<IndexDef>]) -> Result<(R, Vec<WriteOp>), LiteError>,
    ) -> Result<R, LiteError>
    where
        S: StorageEngine,
    {
        let (defs, ops) = {
            let state = self.lock();
            let defs = select(&state);
            let mut scratch = state.scratch(&defs);
            let (_, ops) = change(&mut scratch, &defs)?;
            (defs, ops)
        };
        let mut batch = extra;
        batch.extend(ops);
        write(storage, batch).await?;
        let mut state = self.lock();
        let (result, _stored) = change(&mut state, &defs)?;
        Ok(result)
    }

    /// Remove the index named `name` and its entries.
    pub(crate) async fn drop_index<S: StorageEngine>(
        &self,
        storage: &S,
        name: &str,
    ) -> Result<Option<Arc<IndexDef>>, LiteError> {
        self.apply_ddl(
            storage,
            |state| state.defs.get(name).cloned().into_iter().collect(),
            |state, defs| {
                let mut ops = Vec::new();
                let mut dropped = None;
                for def in defs {
                    let (removed, written) =
                        state.collecting(durable(def), |s| s.remove_def(&def.name));
                    dropped = removed;
                    ops.extend(written);
                }
                Ok((dropped, ops))
            },
        )
        .await
    }

    /// Remove the index named `name` from memory only: an install whose
    /// storage write failed, so nothing of it was stored.
    pub(crate) fn forget(&self, name: &str) {
        let mut state = self.lock();
        let ((), _unstored) = state.collecting(true, |s| {
            s.remove_def(name);
        });
    }

    /// Remove every entry of every document index on `collection`, keeping
    /// the definitions: what clearing the collection's CRDT rows does to its
    /// indexes.
    pub(crate) fn clear_document_entries(&self, collection: &str) {
        let mut state = self.lock();
        for def in state.defs_on(collection, IndexEngine::Document) {
            state.clear_prefix(&def.entry_prefix());
        }
    }

    /// Remove every index on `collection`, definitions and entries, and its
    /// tombstones: what dropping the collection does to its indexes.
    pub(crate) async fn drop_collection<S: StorageEngine>(
        &self,
        storage: &S,
        collection: &str,
    ) -> Result<(), LiteError> {
        self.drop_collection_with(storage, collection, || async { Ok(Vec::new()) })
            .await
    }

    /// [`Self::drop_collection`], committing the writes `rows` returns —
    /// the drop of the collection's rows — in the same storage batch. `rows`
    /// runs with no strict or key-value row write in progress, and none
    /// starts until the drop is applied.
    pub(crate) async fn drop_collection_with<S, F, Fut>(
        &self,
        storage: &S,
        collection: &str,
        rows: F,
    ) -> Result<(), LiteError>
    where
        S: StorageEngine,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<WriteOp>, LiteError>>,
    {
        let _ddl = self.durable_ddl.write().await;
        let extra = rows().await?;
        self.apply_locked(
            storage,
            extra,
            |state| defs_of(state, collection),
            |state, defs| {
                let mut ops = Vec::new();
                for def in defs {
                    let (_, written) = state.collecting(durable(def), |s| s.remove_def(&def.name));
                    ops.extend(written);
                }
                state.tombstoned.retain(|(c, _)| c != collection);
                Ok(((), ops))
            },
        )
        .await
    }

    /// Re-declare every index on `collection` as covering `engine` rows and
    /// remove its entries: the rows it was built from now live in another
    /// engine. Returns the re-declared definitions, for the caller to build.
    pub(crate) async fn move_collection<S: StorageEngine>(
        &self,
        storage: &S,
        collection: &str,
        engine: IndexEngine,
    ) -> Result<Vec<Arc<IndexDef>>, LiteError> {
        self.apply_ddl(
            storage,
            |state| defs_of(state, collection),
            |state, defs| {
                let mut ops = Vec::new();
                let mut moved = Vec::new();
                for def in defs {
                    let (_, cleared) = state.collecting(durable(def), |s| {
                        s.clear_prefix(&def.entry_prefix());
                    });
                    ops.extend(cleared);
                    let next = Arc::new(IndexDef {
                        engine,
                        ..(**def).clone()
                    });
                    let (put, declared) =
                        state.collecting(durable(&next), |s| s.put_def(Arc::clone(&next)));
                    put?;
                    ops.extend(declared);
                    moved.push(next);
                }
                Ok((moved, ops))
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::Namespace;
    use nodedb_types::value::Value;

    use super::*;
    use crate::index::catalog::canonical_field;
    use crate::storage::engine::KvPair;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    /// Storage that holds nothing and rejects every write.
    struct RejectingStorage;

    fn rejected() -> LiteError {
        LiteError::Storage {
            detail: "writes rejected".into(),
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for RejectingStorage {
        async fn get(&self, _: Namespace, _: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
            Ok(None)
        }
        async fn put(&self, _: Namespace, _: &[u8], _: &[u8]) -> Result<(), LiteError> {
            Err(rejected())
        }
        async fn delete(&self, _: Namespace, _: &[u8]) -> Result<(), LiteError> {
            Err(rejected())
        }
        async fn scan_prefix(&self, _: Namespace, _: &[u8]) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }
        async fn batch_write(&self, _: &[WriteOp]) -> Result<(), LiteError> {
            Err(rejected())
        }
        async fn count(&self, _: Namespace) -> Result<u64, LiteError> {
            Ok(0)
        }
        async fn scan_range(
            &self,
            _: Namespace,
            _: &[u8],
            _: usize,
        ) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }
        async fn scan_range_bounded(
            &self,
            _: Namespace,
            _: Option<&[u8]>,
            _: Option<&[u8]>,
            _: Option<usize>,
        ) -> Result<Vec<KvPair>, LiteError> {
            Ok(Vec::new())
        }
    }

    fn email_row(email: &str) -> Value {
        Value::Object(HashMap::from([(
            "email".to_string(),
            Value::String(email.into()),
        )]))
    }

    fn email_index(engine: IndexEngine) -> Arc<IndexDef> {
        let (path, is_array) = canonical_field("email");
        Arc::new(IndexDef {
            name: "idx_email".into(),
            collection: "users".into(),
            path,
            unique: true,
            case_insensitive: false,
            is_array,
            predicate: None,
            engine,
        })
    }

    /// The catalog still holds `before` on `users`, with its one entry.
    fn assert_unchanged(catalog: &IndexCatalog, before: &IndexDef) {
        let def = catalog.def_named("idx_email").expect("definition kept");
        assert_eq!(def.collection, before.collection);
        assert_eq!(def.engine, before.engine);
        assert_eq!(
            catalog.lookup_eq(&def, &Value::String("a@x".into())),
            vec!["6b31"]
        );
    }

    #[tokio::test]
    async fn a_rejected_write_leaves_durable_indexes_unchanged() {
        for engine in [IndexEngine::Strict, IndexEngine::KeyValue] {
            let catalog = IndexCatalog::new();
            let def = email_index(engine);
            catalog
                .install(Arc::clone(&def), [("6b31".to_string(), email_row("a@x"))])
                .expect("install");

            assert!(
                catalog
                    .drop_index(&RejectingStorage, "idx_email")
                    .await
                    .is_err()
            );
            assert_unchanged(&catalog, &def);

            assert!(
                catalog
                    .drop_collection(&RejectingStorage, "users")
                    .await
                    .is_err()
            );
            assert_unchanged(&catalog, &def);

            assert!(
                catalog
                    .move_collection(&RejectingStorage, "users", IndexEngine::Document)
                    .await
                    .is_err()
            );
            assert_unchanged(&catalog, &def);
        }
    }

    #[tokio::test]
    async fn a_stored_drop_reaches_memory() {
        let storage = PagedbStorageMem::open_in_memory().await.expect("storage");
        let catalog = IndexCatalog::new();
        let def = email_index(IndexEngine::Strict);
        catalog
            .install(def, [("6b31".to_string(), email_row("a@x"))])
            .expect("install");

        let dropped = catalog
            .drop_index(&storage, "idx_email")
            .await
            .expect("drop");

        assert!(dropped.is_some());
        assert!(catalog.def_named("idx_email").is_none());
    }
}
