// SPDX-License-Identifier: Apache-2.0

//! Full-text search index restore.

use std::sync::Arc;

use nodedb_mem::MemoryGovernor;
use nodedb_types::error::NodeDbResult;

use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Load declarations before reading compatible posting artifacts.
    /// Incomplete or outdated checkpoints rebuild from authoritative sources.
    pub(in crate::nodedb::core::open) async fn restore_fts_indices(
        storage: &Arc<S>,
        governor: &Arc<MemoryGovernor>,
    ) -> NodeDbResult<(crate::engine::fts::FtsCollectionManager, bool)> {
        let mut mgr = crate::engine::fts::FtsCollectionManager::new(Arc::clone(governor));
        let declarations = crate::engine::fts::catalog::load_declarations(storage.as_ref()).await?;
        mgr.load_declarations(declarations);
        if !crate::engine::fts::checkpoint::checkpoint_compatible(
            storage.as_ref(),
            &mgr.declaration_revisions(),
        )
        .await?
        {
            mgr.ensure_declaration_indices()?;
            return Ok((mgr, false));
        }

        match crate::engine::fts::checkpoint::restore_fts(storage.as_ref(), Arc::clone(governor))
            .await
        {
            Ok(restored) if !restored.indices.is_empty() => {
                let complete = restored.per_field_layout;
                mgr.load_checkpoint(
                    restored.indices,
                    restored.id_to_surrogate,
                    restored.surrogate_to_id,
                    restored.next_surrogate,
                );
                mgr.ensure_declaration_indices()?;
                Ok((mgr, complete))
            }
            // No checkpoint found — caller will rebuild from CRDT state.
            Ok(_) => {
                mgr.ensure_declaration_indices()?;
                Ok((mgr, false))
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "FTS checkpoint restore failed — starting with empty index; \
                     will rebuild from CRDT state on cold open"
                );
                mgr.ensure_declaration_indices()?;
                Ok((mgr, false))
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use nodedb_client::NodeDb;
    use nodedb_types::document::Document;
    use nodedb_types::text_search::TextSearchParams;
    use nodedb_types::{Namespace, Value};

    use crate::storage::engine::StorageEngine;
    use crate::{Encryption, LiteConfig, NodeDbLite, PagedbStorageDefault};

    fn config() -> LiteConfig {
        LiteConfig {
            auto_flush_ms: 0,
            sync_enabled: false,
            ..LiteConfig::default()
        }
    }

    async fn open(path: &std::path::Path) -> std::sync::Arc<NodeDbLite<PagedbStorageDefault>> {
        NodeDbLite::open_at_path_with_config(path, Encryption::Plaintext, config())
            .await
            .unwrap()
    }

    async fn hits(
        db: &NodeDbLite<PagedbStorageDefault>,
        collection: &str,
        term: &str,
    ) -> Vec<String> {
        db.text_search(collection, "", term, 10, TextSearchParams::default(), None)
            .await
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect()
    }

    #[tokio::test]
    async fn complete_checkpoint_replaces_strict_postings_from_durable_rows() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("strict_text.db");
        let db = open(&path).await;
        db.execute_sql(
            "CREATE COLLECTION articles (id BIGINT PRIMARY KEY, body TEXT) WITH storage = 'strict'",
            &[],
        )
        .await
        .unwrap();
        db.strict_insert(
            "articles",
            &[Value::Integer(1), Value::String("retiredtoken".into())],
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        assert_eq!(hits(&db, "articles", "retiredtoken").await.len(), 1);
        let strict = db.strict_engine();
        strict.delete("articles", &Value::Integer(1)).await.unwrap();
        strict
            .insert(
                "articles",
                &[Value::Integer(2), Value::String("durabletoken".into())],
            )
            .await
            .unwrap();
        drop(db);

        let reopened = open(&path).await;
        assert!(hits(&reopened, "articles", "retiredtoken").await.is_empty());
        assert_eq!(hits(&reopened, "articles", "durabletoken").await.len(), 1);
    }

    #[tokio::test]
    async fn complete_checkpoint_replaces_valid_time_postings_without_crdt_overlay() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("valid_time_text.db");
        let db = open(&path).await;
        db.execute_sql("CREATE COLLECTION articles WITH (bitemporal=true)", &[])
            .await
            .unwrap();
        let mut document = Document::new("one");
        document.set("body", Value::String("retiredtoken".into()));
        db.document_put("articles", document).await.unwrap();
        {
            use crate::nodedb::LockExt;
            db.crdt
                .lock_or_recover()
                .upsert(
                    "articles",
                    "ghost",
                    &[("body", loro::LoroValue::String("stalecrdttoken".into()))],
                )
                .unwrap();
        }
        db.flush().await.unwrap();
        crate::engine::document::history::ops::versioned_tombstone(
            db.storage.as_ref(),
            "articles",
            "one",
            i64::MAX - 2,
            None,
        )
        .await
        .unwrap();
        let body = nodedb_types::json_msgpack::value_to_msgpack(&Value::Object(
            std::collections::HashMap::from([(
                "body".into(),
                Value::String("durabletoken".into()),
            )]),
        ))
        .unwrap();
        crate::engine::document::history::ops::versioned_put(
            db.storage.as_ref(),
            "articles",
            "two",
            &body,
            i64::MAX - 1,
            None,
            None,
        )
        .await
        .unwrap();
        drop(db);

        let reopened = open(&path).await;
        assert!(hits(&reopened, "articles", "retiredtoken").await.is_empty());
        assert!(
            hits(&reopened, "articles", "stalecrdttoken")
                .await
                .is_empty()
        );
        assert_eq!(hits(&reopened, "articles", "durabletoken").await, ["two"]);
    }

    #[tokio::test]
    async fn incomplete_checkpoint_skips_obsolete_posting_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("incomplete_text.db");
        let db = open(&path).await;
        let mut document = Document::new("one");
        document.set("body", Value::String("recoverabletoken".into()));
        db.document_put("articles", document).await.unwrap();
        db.flush().await.unwrap();
        crate::engine::fts::checkpoint::persist_checkpoint_incomplete(db.storage.as_ref())
            .await
            .unwrap();
        db.storage
            .put(
                Namespace::Fts,
                b"fts:_collections",
                b"unreadable postings catalog",
            )
            .await
            .unwrap();
        drop(db);

        let reopened = open(&path).await;
        assert_eq!(
            hits(&reopened, "articles", "recoverabletoken").await,
            ["one"]
        );
    }

    #[tokio::test]
    async fn declaration_revision_rebuilds_older_automatic_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("revision_text.db");
        let db = open(&path).await;
        let mut document = Document::new("one");
        document.set("title", Value::String("selectedtoken".into()));
        document.set("body", Value::String("excludedtoken".into()));
        db.document_put("articles", document).await.unwrap();
        db.flush().await.unwrap();
        db.execute_sql("CREATE SEARCH INDEX ON articles (title)", &[])
            .await
            .unwrap();
        drop(db);

        let reopened = open(&path).await;
        assert_eq!(hits(&reopened, "articles", "selectedtoken").await, ["one"]);
        assert!(
            hits(&reopened, "articles", "excludedtoken")
                .await
                .is_empty()
        );
    }

    /// Allocated and limit bytes of the FTS budget.
    fn fts_usage(db: &NodeDbLite<PagedbStorageDefault>) -> (usize, usize) {
        let entry = db
            .governor()
            .snapshot()
            .into_iter()
            .find(|entry| entry.engine == nodedb_mem::EngineId::Fts)
            .unwrap();
        (entry.allocated, entry.limit)
    }

    /// A strict collection whose text fills more than half the FTS budget
    /// reopens: open charges the rebuilt postings once, not on top of the
    /// checkpoint's.
    #[tokio::test]
    async fn strict_text_admitted_live_reopens_within_the_same_fts_budget() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("strict_budget.db");
        let db = open(&path).await;
        db.execute_sql(
            "CREATE COLLECTION runs (id BIGINT PRIMARY KEY, payload TEXT) WITH storage = 'strict'",
            &[],
        )
        .await
        .unwrap();
        let (_, limit) = fts_usage(&db);
        let mut row = 0i64;
        while fts_usage(&db).0 <= limit / 10 * 6 {
            assert!(row < 10_000, "the FTS budget never filled");
            let payload: Vec<String> = (0..64).map(|t| format!("r{row:05}t{t:03}")).collect();
            db.strict_insert(
                "runs",
                &[Value::Integer(row), Value::String(payload.join(" "))],
            )
            .await
            .unwrap();
            row += 1;
        }
        let (live, _) = fts_usage(&db);
        assert!(live < limit);
        db.flush().await.unwrap();
        drop(db);

        let reopened = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config())
            .await
            .expect("a store that accepted every write must reopen");
        let (restored, _) = fts_usage(&reopened);
        assert!(
            restored <= live,
            "open charged {restored} bytes, live admitted {live}"
        );
        assert_eq!(hits(&reopened, "runs", "r00000t000").await.len(), 1);
    }
}
