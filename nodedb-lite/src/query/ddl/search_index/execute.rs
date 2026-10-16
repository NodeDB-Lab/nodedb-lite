// SPDX-License-Identifier: Apache-2.0

//! Declaration tasks retain admission through durable storage and publication.

use std::sync::{Arc, Mutex};

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use super::SearchIndexStatement;
use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::engine::fts::catalog::{SearchDeclaration, persist_declaration};
use crate::engine::fts::coordinator::TextMutationPermit;
use crate::engine::fts::rebuild::build_collection_replacement;
use crate::engine::strict::StrictEngine;
use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

struct DeclarationContext<S: StorageEngine> {
    storage: Arc<S>,
    crdt: Arc<Mutex<CrdtEngine>>,
    strict: Arc<StrictEngine<S>>,
    columnar: Arc<ColumnarEngine<S>>,
    fts: Arc<FtsState>,
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    pub(in crate::query) async fn handle_search_index(
        &self,
        statement: SearchIndexStatement,
    ) -> Result<QueryResult, LiteError> {
        let context = DeclarationContext {
            storage: Arc::clone(&self.storage),
            crdt: Arc::clone(&self.crdt),
            strict: Arc::clone(&self.strict),
            columnar: Arc::clone(&self.columnar),
            fts: Arc::clone(&self.fts_state),
        };
        let permit = self.fts_state.admit_exclusive().await;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Caller cancellation cannot interrupt a committed declaration's publication.
        crate::runtime::spawn(async move {
            let guard = context.fts.mutation_guard(permit);
            let result = context.execute(statement, guard.permit()).await;
            let result = guard.finish(result);
            if let Err(Err(error)) = sender.send(result) {
                tracing::error!(%error, "detached SEARCH INDEX declaration returned an error");
            }
        });
        receiver.await.map_err(|error| LiteError::JoinError {
            detail: format!("SEARCH INDEX task ended before its result: {error}; reopen the database before retrying"),
        })?
    }
}

impl<S: StorageEngine> DeclarationContext<S> {
    async fn execute(
        self,
        statement: SearchIndexStatement,
        permit: &TextMutationPermit,
    ) -> Result<QueryResult, LiteError> {
        let (collection, declaration, command) = match statement {
            SearchIndexStatement::Create {
                collection,
                fields,
                analyzer,
                fuzzy,
            } => {
                let manager = self
                    .fts
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?;
                if manager.declaration_for(&collection).is_some() {
                    return Err(LiteError::Query(format!(
                        "search index 'fts_{collection}' already exists: drop it before creating another declaration"
                    )));
                }
                drop(manager);
                let declaration = SearchDeclaration {
                    name: format!("fts_{collection}"),
                    fields,
                    analyzer,
                    fuzzy,
                };
                (collection, Some(declaration), "created")
            }
            SearchIndexStatement::Drop { name, if_exists } => {
                let collection = name.strip_prefix("fts_").unwrap_or("");
                let exists = self
                    .fts
                    .manager
                    .lock()
                    .map_err(|_| LiteError::LockPoisoned)?
                    .declaration_for(collection)
                    .is_some_and(|declaration| declaration.name == name);
                if !exists {
                    if if_exists {
                        return Ok(result(format!("search index '{name}' does not exist")));
                    }
                    return Err(LiteError::Query(format!(
                        "search index '{name}' does not exist: use IF EXISTS or its declared name"
                    )));
                }
                (collection.to_string(), None, "dropped")
            }
        };
        if self.columnar.schema(&collection).is_some() {
            return Err(LiteError::Unsupported {
                detail: format!(
                    "SEARCH INDEX on columnar collection '{collection}': use a document or strict collection"
                ),
            });
        }
        let record = self
            .fts
            .manager
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .next_declaration_record(&collection, &declaration)?;
        let candidate = build_collection_replacement(
            self.storage.as_ref(),
            self.crdt.as_ref(),
            self.strict.as_ref(),
            self.fts.as_ref(),
            &collection,
            record.clone(),
            permit,
        )
        .await?;
        persist_declaration(self.storage.as_ref(), &collection, &record).await?;
        // No await separates the durable record from synchronous publication.
        self.fts
            .manager
            .lock_or_recover()
            .publish_replacement(candidate);
        Ok(result(format!("search index 'fts_{collection}' {command}")))
    }
}

fn result(message: String) -> QueryResult {
    QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec![Value::String(message)]],
        rows_affected: 0,
        command: None,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;

    use async_trait::async_trait;
    use nodedb_client::NodeDb;
    use nodedb_types::Namespace;
    use nodedb_types::document::Document;
    use nodedb_types::text_search::TextSearchParams;
    use tokio::sync::Notify;

    use super::*;
    use crate::storage::engine::{KvPair, PrefixScan, WriteOp};
    use crate::{LiteConfig, NodeDbLite, PagedbStorageMem};

    #[derive(Clone)]
    struct ControlledStorage {
        inner: Arc<PagedbStorageMem>,
        pause_next: Arc<AtomicBool>,
        reject_next: Arc<AtomicBool>,
        committed: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl ControlledStorage {
        async fn new() -> Self {
            Self {
                inner: Arc::new(PagedbStorageMem::open_in_memory().await.expect("storage")),
                pause_next: Arc::new(AtomicBool::new(false)),
                reject_next: Arc::new(AtomicBool::new(false)),
                committed: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            }
        }
    }

    #[async_trait]
    impl StorageEngine for ControlledStorage {
        async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
            self.inner.get(ns, key).await
        }
        async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
            let declaration = ns == Namespace::Meta && key.starts_with(b"search_declaration:");
            if declaration && self.reject_next.swap(false, Ordering::AcqRel) {
                return Err(LiteError::Storage {
                    detail: "declaration write rejected by test storage".into(),
                });
            }
            self.inner.put(ns, key, value).await?;
            if declaration && self.pause_next.swap(false, Ordering::AcqRel) {
                self.committed.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
        async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
            self.inner.delete(ns, key).await
        }
        async fn scan_prefix(
            &self,
            ns: Namespace,
            prefix: &[u8],
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_prefix(ns, prefix).await
        }
        async fn scan_prefix_bounded(
            &self,
            ns: Namespace,
            prefix: &[u8],
            limit: usize,
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_prefix_bounded(ns, prefix, limit).await
        }
        async fn scan_prefix_budgeted(
            &self,
            ns: Namespace,
            prefix: &[u8],
            max_records: usize,
            max_bytes: usize,
        ) -> Result<PrefixScan, LiteError> {
            self.inner
                .scan_prefix_budgeted(ns, prefix, max_records, max_bytes)
                .await
        }
        async fn scan_prefix_from_budgeted(
            &self,
            ns: Namespace,
            prefix: &[u8],
            after_key: Option<&[u8]>,
            max_records: usize,
            max_bytes: usize,
        ) -> Result<PrefixScan, LiteError> {
            self.inner
                .scan_prefix_from_budgeted(ns, prefix, after_key, max_records, max_bytes)
                .await
        }
        async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
            self.inner.batch_write(ops).await
        }
        async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
            self.inner.count(ns).await
        }
        async fn scan_range(
            &self,
            ns: Namespace,
            start: &[u8],
            limit: usize,
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_range(ns, start, limit).await
        }
        async fn scan_range_bounded(
            &self,
            ns: Namespace,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
            limit: Option<usize>,
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_range_bounded(ns, start, end, limit).await
        }
    }

    fn config() -> LiteConfig {
        LiteConfig {
            auto_flush_ms: 0,
            sync_enabled: false,
            ..LiteConfig::default()
        }
    }

    async fn seed(db: &NodeDbLite<ControlledStorage>) {
        let mut document = Document::new("one");
        document.set("title", Value::String("rust".into()));
        document.set("body", Value::String("python".into()));
        db.document_put("articles", document)
            .await
            .expect("document");
    }

    async fn matches(db: &NodeDbLite<ControlledStorage>, query: &str) -> bool {
        !db.text_search("articles", "", query, 10, TextSearchParams::default(), None)
            .await
            .expect("search")
            .is_empty()
    }

    #[tokio::test]
    async fn caller_cancellation_after_catalog_commit_keeps_publication_and_reopen_consistent() {
        let storage = ControlledStorage::new().await;
        let db = NodeDbLite::open_with_config(storage.clone(), config())
            .await
            .expect("database");
        seed(&db).await;
        db.flush().await.expect("durable source");
        storage.pause_next.store(true, Ordering::Release);
        let caller = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                db.execute_sql("CREATE SEARCH INDEX ON articles(title)", &[])
                    .await
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            storage.committed.notified(),
        )
        .await
        .expect("catalog commit reached");
        caller.abort();
        assert!(caller.await.expect_err("caller cancelled").is_cancelled());
        assert!(
            matches(&db, "python").await,
            "previous index remains available during publication pause"
        );
        storage.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), db.flush())
            .await
            .expect("declaration gate drains")
            .expect("flush");
        assert!(!matches(&db, "python").await);
        assert!(matches(&db, "rust").await);
        db.shutdown().await;
        drop(db);
        let reopened = NodeDbLite::open_with_config(storage, config())
            .await
            .expect("reopen");
        assert!(!matches(&reopened, "python").await);
        assert!(matches(&reopened, "rust").await);
    }

    #[tokio::test]
    async fn declaration_storage_error_preserves_previous_index_and_allows_retry() {
        let storage = ControlledStorage::new().await;
        let db = NodeDbLite::open_with_config(storage.clone(), config())
            .await
            .expect("database");
        seed(&db).await;
        storage.reject_next.store(true, Ordering::Release);
        assert!(
            db.execute_sql("CREATE SEARCH INDEX ON articles(title)", &[])
                .await
                .is_err()
        );
        assert!(matches(&db, "python").await);
        db.execute_sql("CREATE SEARCH INDEX ON articles(title)", &[])
            .await
            .expect("retry declaration");
        assert!(!matches(&db, "python").await);
    }

    #[tokio::test]
    async fn declaration_publication_serializes_flush_and_truncate() {
        let storage = ControlledStorage::new().await;
        let db = NodeDbLite::open_with_config(storage.clone(), config())
            .await
            .expect("database");
        seed(&db).await;
        storage.pause_next.store(true, Ordering::Release);
        let declaration = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                db.execute_sql("CREATE SEARCH INDEX ON articles(title)", &[])
                    .await
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            storage.committed.notified(),
        )
        .await
        .expect("declaration paused after catalog commit");
        let mut flush = Box::pin(db.flush());
        let mut truncate = Box::pin(db.execute_sql("TRUNCATE articles", &[]));
        std::future::poll_fn(|context| {
            assert!(flush.as_mut().poll(context).is_pending());
            assert!(truncate.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(matches(&db, "python").await);
        storage.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            declaration
                .await
                .expect("declaration task")
                .expect("declaration");
            flush.await.expect("flush");
            truncate.await.expect("truncate");
        })
        .await
        .expect("queued operations drain");
        assert!(!matches(&db, "rust").await);
        db.text_search(
            "articles",
            "title",
            "rust",
            10,
            TextSearchParams::default(),
            None,
        )
        .await
        .expect("declared empty field remains searchable");
        seed(&db).await;
        assert!(matches(&db, "rust").await);
        assert!(!matches(&db, "python").await);
    }
}
