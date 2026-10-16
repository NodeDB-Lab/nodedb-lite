// SPDX-License-Identifier: Apache-2.0

//! SEARCH INDEX declarations govern current and future document and strict text.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::storage::engine::StorageEngine;
use nodedb_lite::{LiteConfig, NodeDbLite, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

fn config() -> LiteConfig {
    LiteConfig {
        auto_flush_ms: 0,
        sync_enabled: false,
        ..LiteConfig::default()
    }
}

async fn open() -> Arc<NodeDbLite<PagedbStorageMem>> {
    NodeDbLite::open_with_config(
        PagedbStorageMem::open_in_memory().await.expect("storage"),
        config(),
    )
    .await
    .expect("database")
}

async fn sql<S: StorageEngine>(db: &NodeDbLite<S>, statement: &str) {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        db.execute_sql(statement, &[]),
    )
    .await
    .expect("SQL mutation completes without nested admission")
    .expect(statement);
}

async fn ids<S: StorageEngine>(
    db: &NodeDbLite<S>,
    collection: &str,
    field: &str,
    query: &str,
) -> Vec<String> {
    let mut ids: Vec<_> = db
        .text_search(
            collection,
            field,
            query,
            1024,
            TextSearchParams::default(),
            None,
        )
        .await
        .expect("search")
        .into_iter()
        .map(|row| row.id)
        .collect();
    ids.sort();
    ids
}

fn document(id: &str, title: &str, body: &str) -> Document {
    let mut document = Document::new(id);
    document.set("title", Value::String(title.into()));
    document.set("body", Value::String(body.into()));
    document
}

#[tokio::test]
async fn selected_fields_replace_existing_postings_and_cover_future_document_paths() {
    let db = open().await;
    db.document_put("articles", document("before", "rust", "python"))
        .await
        .expect("put");
    sql(&db, "CREATE SEARCH INDEX ON articles(title)").await;
    assert_eq!(ids(&db, "articles", "", "rust").await, vec!["before"]);
    assert!(ids(&db, "articles", "", "python").await.is_empty());
    assert!(
        db.text_search(
            "articles",
            "body",
            "python",
            10,
            TextSearchParams::default(),
            None
        )
        .await
        .is_err()
    );
    db.document_put("articles", document("after", "rust", "python"))
        .await
        .expect("future put");
    sql(
        &db,
        "INSERT INTO articles(id, title, body) VALUES ('sql', 'rust', 'python')",
    )
    .await;
    assert_eq!(
        ids(&db, "articles", "title", "rust").await,
        vec!["after", "before", "sql"]
    );
    assert!(ids(&db, "articles", "", "python").await.is_empty());
    assert!(
        db.execute_sql("CREATE SEARCH INDEX ON articles(body)", &[])
            .await
            .is_err()
    );
    sql(&db, "DROP SEARCH INDEX fts_articles").await;
    assert_eq!(
        ids(&db, "articles", "body", "python").await,
        vec!["after", "before", "sql"]
    );
    sql(&db, "DROP SEARCH INDEX IF EXISTS fts_articles").await;
    assert!(
        db.execute_sql("DROP SEARCH INDEX fts_articles", &[])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn strict_declarations_cover_existing_rows_and_future_rust_and_sql_writes() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION shelf (id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT) WITH storage = 'strict'").await;
    db.strict_insert(
        "shelf",
        &[
            Value::String("before".into()),
            Value::String("rust".into()),
            Value::String("python".into()),
        ],
    )
    .await
    .expect("strict insert");
    sql(&db, "CREATE SEARCH INDEX ON shelf(title)").await;
    db.strict_insert(
        "shelf",
        &[
            Value::String("rustapi".into()),
            Value::String("rust".into()),
            Value::String("python".into()),
        ],
    )
    .await
    .expect("future strict insert");
    sql(
        &db,
        "INSERT INTO shelf(id, title, body) VALUES ('sql', 'rust', 'python')",
    )
    .await;
    assert_eq!(
        ids(&db, "shelf", "", "rust").await,
        vec!["before", "rustapi", "sql"]
    );
    assert!(ids(&db, "shelf", "", "python").await.is_empty());
    sql(
        &db,
        "UPDATE shelf SET title = 'go', body = 'erlang' WHERE id = 'sql'",
    )
    .await;
    assert_eq!(ids(&db, "shelf", "title", "go").await, vec!["sql"]);
    assert!(ids(&db, "shelf", "", "erlang").await.is_empty());
    sql(&db, "DROP SEARCH INDEX fts_shelf").await;
    assert_eq!(ids(&db, "shelf", "body", "erlang").await, vec!["sql"]);
    assert_eq!(
        ids(&db, "shelf", "body", "python").await,
        vec!["before", "rustapi"]
    );
}

#[tokio::test]
async fn bitemporal_declarations_rebuild_current_document_versions() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION history WITH (bitemporal=true)").await;
    db.document_put("history", document("one", "oldtitle", "oldbody"))
        .await
        .expect("old version");
    db.document_put("history", document("one", "newtitle", "newbody"))
        .await
        .expect("current version");
    sql(&db, "CREATE SEARCH INDEX ON history(title)").await;
    assert_eq!(ids(&db, "history", "title", "newtitle").await, vec!["one"]);
    assert!(ids(&db, "history", "", "oldtitle").await.is_empty());
    assert!(ids(&db, "history", "", "newbody").await.is_empty());
    sql(&db, "DROP SEARCH INDEX fts_history").await;
    assert_eq!(ids(&db, "history", "body", "newbody").await, vec!["one"]);
    assert!(ids(&db, "history", "", "oldbody").await.is_empty());
}

#[tokio::test]
async fn quoted_declaration_names_preserve_collection_case() {
    let db = open().await;
    db.document_put("ArticleCase", document("one", "rust", "python"))
        .await
        .expect("document");
    sql(&db, "CREATE SEARCH INDEX ON \"ArticleCase\" (title)").await;
    assert!(ids(&db, "ArticleCase", "", "python").await.is_empty());
    assert!(
        db.execute_sql("DROP SEARCH INDEX fts_ArticleCase", &[])
            .await
            .is_err()
    );
    sql(&db, "DROP SEARCH INDEX \"fts_ArticleCase\"").await;
    assert_eq!(ids(&db, "ArticleCase", "body", "python").await, vec!["one"]);
}

#[tokio::test]
async fn empty_declarations_apply_analyzer_and_fuzzy_defaults_to_future_rows() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION deutsch").await;
    sql(
        &db,
        "CREATE SEARCH INDEX ON deutsch(body) ANALYZER ' GERMAN ' FUZZY true",
    )
    .await;
    db.document_put(
        "deutsch",
        document("one", "excluded", "Die Datenbanken sind schnell"),
    )
    .await
    .expect("future document");
    assert!(ids(&db, "deutsch", "body", "die").await.is_empty());
    assert_eq!(ids(&db, "deutsch", "body", "schnel").await, vec!["one"]);
    sql(&db, "DROP SEARCH INDEX fts_deutsch").await;
    assert_eq!(ids(&db, "deutsch", "body", "die").await, vec!["one"]);
    assert!(ids(&db, "deutsch", "body", "schnel").await.is_empty());
    assert_eq!(ids(&db, "deutsch", "title", "excluded").await, vec!["one"]);
}

#[tokio::test]
async fn declaration_rebuild_consumes_every_live_document_page() {
    let db = NodeDbLite::open_with_config(
        PagedbStorageMem::open_in_memory().await.expect("storage"),
        LiteConfig {
            fts_percent: 10,
            hnsw_percent: 31,
            ..config()
        },
    )
    .await
    .expect("database with 10 MiB FTS budget");
    for index in 0..300 {
        db.document_put(
            "pages",
            document(&format!("{index:04}"), "selectedterm", "excludedterm"),
        )
        .await
        .expect("page document");
    }
    sql(&db, "CREATE SEARCH INDEX ON pages(title)").await;
    assert_eq!(ids(&db, "pages", "title", "selectedterm").await.len(), 300);
    assert!(ids(&db, "pages", "", "excludedterm").await.is_empty());
}

#[tokio::test]
async fn oversized_strict_rebuild_preserves_previous_search_and_declaration_state() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION large_rows (id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT) WITH storage = 'strict'").await;
    db.strict_insert(
        "large_rows",
        &[
            Value::String("small".into()),
            Value::String("rust".into()),
            Value::String("python".into()),
        ],
    )
    .await
    .expect("small row");
    db.strict_engine()
        .insert(
            "large_rows",
            &[
                Value::String("large".into()),
                Value::String("rust".into()),
                Value::String("x".repeat(9 * 1024 * 1024)),
            ],
        )
        .await
        .expect("oversized source row");
    let error = db
        .execute_sql("CREATE SEARCH INDEX ON large_rows(title)", &[])
        .await
        .expect_err("bounded rebuild rejects oversized row");
    assert!(error.to_string().contains("backpressure"), "{error}");
    assert_eq!(
        ids(&db, "large_rows", "body", "python").await,
        vec!["small"]
    );
    db.strict_engine()
        .delete("large_rows", &Value::String("large".into()))
        .await
        .expect("remove oversized source");
    sql(&db, "CREATE SEARCH INDEX ON large_rows(title)").await;
    assert!(ids(&db, "large_rows", "", "python").await.is_empty());
}

#[tokio::test]
async fn columnar_declarations_return_errors_without_changing_automatic_search() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION events (id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT) WITH storage = 'columnar'").await;
    sql(
        &db,
        "INSERT INTO events(id, title, body) VALUES ('one', 'rust', 'python')",
    )
    .await;
    let error = db
        .execute_sql("CREATE SEARCH INDEX ON events(title)", &[])
        .await
        .expect_err("columnar unsupported");
    assert!(error.to_string().contains("columnar"), "{error}");
    assert_eq!(ids(&db, "events", "body", "python").await, vec!["one"]);
}

#[tokio::test]
async fn truncate_preserves_declared_empty_field_and_future_selection() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION articles").await;
    db.document_put("articles", document("before", "rust", "python"))
        .await
        .expect("document");
    sql(&db, "CREATE SEARCH INDEX ON articles(title)").await;
    sql(&db, "TRUNCATE articles").await;
    assert!(ids(&db, "articles", "title", "rust").await.is_empty());
    db.document_put("articles", document("after", "rust", "python"))
        .await
        .expect("future document");
    assert_eq!(ids(&db, "articles", "title", "rust").await, vec!["after"]);
    assert!(ids(&db, "articles", "", "python").await.is_empty());
    assert!(
        db.execute_sql("CREATE SEARCH INDEX ON articles(body)", &[])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn drop_collection_removes_declaration_and_recreation_uses_automatic_fields() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION articles").await;
    db.document_put("articles", document("before", "rust", "python"))
        .await
        .expect("document");
    sql(&db, "CREATE SEARCH INDEX ON articles(title)").await;
    sql(&db, "DROP COLLECTION articles").await;
    assert!(
        db.execute_sql("DROP SEARCH INDEX fts_articles", &[])
            .await
            .is_err()
    );
    sql(&db, "CREATE COLLECTION articles").await;
    db.document_put("articles", document("after", "rust", "python"))
        .await
        .expect("recreated document");
    assert_eq!(ids(&db, "articles", "body", "python").await, vec!["after"]);
    sql(&db, "CREATE SEARCH INDEX ON articles(body)").await;
    assert!(ids(&db, "articles", "", "rust").await.is_empty());
}

#[tokio::test]
async fn declared_document_conversion_to_columnar_returns_error_before_source_mutation() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION articles").await;
    db.document_put("articles", document("one", "rust", "python"))
        .await
        .expect("document");
    sql(&db, "CREATE SEARCH INDEX ON articles(title)").await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), db.execute_sql(
        "CONVERT COLLECTION articles TO columnar (id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)", &[]))
        .await.expect("conversion admission completes").expect_err("declared columnar unsupported");
    assert!(error.to_string().contains("columnar"), "{error}");
    assert!(db.columnar_engine().schema("articles").is_none());
    assert!(
        db.document_get("articles", "one")
            .await
            .expect("document read")
            .is_some()
    );
    assert_eq!(ids(&db, "articles", "title", "rust").await, vec!["one"]);
    assert!(ids(&db, "articles", "", "python").await.is_empty());
}

#[tokio::test]
async fn document_strict_document_conversion_preserves_selected_fields() {
    let db = open().await;
    sql(&db, "CREATE COLLECTION articles").await;
    let mut source = document("before", "rust", "python");
    source.set("id", Value::String("before".into()));
    db.document_put("articles", source).await.expect("document");
    sql(&db, "CREATE SEARCH INDEX ON articles(title)").await;
    sql(&db, "CONVERT COLLECTION articles TO strict (id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)").await;
    assert_eq!(ids(&db, "articles", "title", "rust").await, vec!["before"]);
    assert!(ids(&db, "articles", "", "python").await.is_empty());
    db.strict_insert(
        "articles",
        &[
            Value::String("strict".into()),
            Value::String("rust".into()),
            Value::String("python".into()),
        ],
    )
    .await
    .expect("strict target insert");
    sql(&db, "CONVERT COLLECTION articles TO document").await;
    let converted_ids = ids(&db, "articles", "title", "rust").await;
    assert_eq!(converted_ids.len(), 2);
    assert_ne!(converted_ids[0], converted_ids[1]);
    assert!(ids(&db, "articles", "", "python").await.is_empty());
    db.document_put("articles", document("after", "rust", "python"))
        .await
        .expect("future document");
    let future_ids = ids(&db, "articles", "title", "rust").await;
    assert_eq!(future_ids.len(), 3);
    assert!(future_ids.contains(&"after".to_string()));
    assert!(converted_ids.iter().all(|id| future_ids.contains(id)));
    assert!(ids(&db, "articles", "", "python").await.is_empty());
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn declarations_and_drop_defaults_survive_reopen_including_empty_collections() {
    use nodedb_lite::Encryption;
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("search.db");
    let db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config())
        .await
        .expect("database");
    db.document_put("articles", document("one", "rust", "python"))
        .await
        .expect("document");
    sql(&db, "CREATE SEARCH INDEX ON articles(title)").await;
    sql(&db, "CREATE COLLECTION empty_articles").await;
    sql(
        &db,
        "CREATE SEARCH INDEX ON empty_articles(body) FUZZY true",
    )
    .await;
    db.flush().await.expect("flush");
    db.shutdown().await;
    drop(db);
    let db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config())
        .await
        .expect("reopen");
    assert_eq!(ids(&db, "articles", "title", "rust").await, vec!["one"]);
    assert!(ids(&db, "articles", "", "python").await.is_empty());
    db.document_put("empty_articles", document("late", "excluded", "database"))
        .await
        .expect("empty collection put");
    assert_eq!(
        ids(&db, "empty_articles", "body", "databse").await,
        vec!["late"]
    );
    assert!(ids(&db, "empty_articles", "", "excluded").await.is_empty());
    sql(&db, "DROP SEARCH INDEX fts_articles").await;
    db.flush().await.expect("flush dropped declaration");
    db.shutdown().await;
    drop(db);
    let db = NodeDbLite::open_at_path_with_config(&path, Encryption::Plaintext, config())
        .await
        .expect("reopen automatic");
    assert_eq!(ids(&db, "articles", "body", "python").await, vec!["one"]);
}
