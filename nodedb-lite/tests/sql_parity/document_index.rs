// SPDX-License-Identifier: Apache-2.0

//! Secondary indexes on schemaless document collections.
//!
//! An equality on an indexed field is answered from the index, so a row the
//! index misses is a row the query misses. Every write path must therefore
//! keep the index current. These are Lite-only checks; Origin is not started.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

use crate::common::sql::open_lite;

type Db = Arc<NodeDbLite<PagedbStorageMem>>;

async fn exec(db: &Db, sql: &str) {
    db.execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn exec_err(db: &Db, sql: &str) -> String {
    match db.execute_sql(sql, &[]).await {
        Ok(_) => panic!("expected an error for {sql}"),
        Err(e) => e.to_string(),
    }
}

/// The sorted `id` column of `sql`.
async fn ids<S: nodedb_lite::StorageEngine>(db: &NodeDbLite<S>, sql: &str) -> Vec<String> {
    let result = db
        .execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut ids: Vec<String> = result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::String(id)) => id.clone(),
            other => panic!("{sql}: id column is {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

fn doc(id: &str, fields: &[(&str, Value)]) -> Document {
    let mut d = Document::new(id);
    for (k, v) in fields {
        d.set(*k, v.clone());
    }
    d
}

fn text(s: &str) -> Value {
    Value::String(s.to_string())
}

async fn put(db: &Db, collection: &str, d: Document) {
    db.document_put(collection, d)
        .await
        .unwrap_or_else(|e| panic!("put into {collection}: {e}"));
}

/// A `users` collection holding `u0` and an index on `email`.
async fn users_with_email_index(unique: bool) -> Db {
    let db = open_lite().await;
    put(&db, "users", doc("u0", &[("email", text("zero@x"))])).await;
    let kind = if unique { "UNIQUE INDEX" } else { "INDEX" };
    exec(&db, &format!("CREATE {kind} idx_email ON users (email)")).await;
    db
}

#[tokio::test]
async fn indexed_lookup_finds_row_inserted_after_create_index() {
    let db = users_with_email_index(false).await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u1', 'a@x')").await;
    put(&db, "users", doc("u2", &[("email", text("b@x"))])).await;

    let q = |email: &str| format!("SELECT id FROM users WHERE email = '{email}'");
    assert_eq!(ids(&db, &q("a@x")).await, vec!["u1"]);
    assert_eq!(ids(&db, &q("b@x")).await, vec!["u2"]);
    assert_eq!(ids(&db, &q("zero@x")).await, vec!["u0"]);
}

#[tokio::test]
async fn indexed_lookup_reflects_update_to_new_value() {
    let db = users_with_email_index(false).await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u1', 'old@x')").await;
    exec(&db, "UPDATE users SET email = 'new@x' WHERE id = 'u1'").await;
    put(&db, "users", doc("u0", &[("email", text("moved@x"))])).await;

    let q = |email: &str| format!("SELECT id FROM users WHERE email = '{email}'");
    assert!(ids(&db, &q("old@x")).await.is_empty());
    assert_eq!(ids(&db, &q("new@x")).await, vec!["u1"]);
    assert!(ids(&db, &q("zero@x")).await.is_empty());
    assert_eq!(ids(&db, &q("moved@x")).await, vec!["u0"]);
}

#[tokio::test]
async fn indexed_lookup_omits_deleted_row() {
    let db = users_with_email_index(false).await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u1', 'a@x')").await;
    put(&db, "users", doc("u2", &[("email", text("a@x"))])).await;
    exec(&db, "DELETE FROM users WHERE id = 'u1'").await;
    db.document_delete("users", "u2").await.expect("delete u2");

    assert!(
        ids(&db, "SELECT id FROM users WHERE email = 'a@x'")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn unique_index_rejects_duplicate_on_insert() {
    let db = users_with_email_index(true).await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u1', 'a@x')").await;

    let err = exec_err(&db, "INSERT INTO users (id, email) VALUES ('u2', 'a@x')").await;
    assert!(err.contains("unique"), "{err}");
    let err = db
        .document_put("users", doc("u3", &[("email", text("a@x"))]))
        .await
        .expect_err("duplicate through the document API");
    assert!(err.to_string().contains("unique"), "{err}");

    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'a@x'").await,
        vec!["u1"]
    );
    // Rewriting the holder with its own value is not a duplicate.
    put(&db, "users", doc("u1", &[("email", text("a@x"))])).await;
}

#[tokio::test]
async fn unique_index_rejects_duplicate_on_update() {
    let db = users_with_email_index(true).await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u1', 'a@x')").await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u2', 'b@x')").await;

    let err = exec_err(&db, "UPDATE users SET email = 'a@x' WHERE id = 'u2'").await;
    assert!(err.contains("unique"), "{err}");
    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'b@x'").await,
        vec!["u2"]
    );
}

#[tokio::test]
async fn unique_index_refuses_to_build_over_duplicates() {
    let db = open_lite().await;
    put(&db, "users", doc("u1", &[("email", text("a@x"))])).await;
    put(&db, "users", doc("u2", &[("email", text("a@x"))])).await;

    let err = exec_err(&db, "CREATE UNIQUE INDEX idx_email ON users (email)").await;
    assert!(err.contains("unique"), "{err}");
    // Nothing was declared: a non-unique index of the same name builds.
    exec(&db, "CREATE INDEX idx_email ON users (email)").await;
}

#[tokio::test]
async fn case_insensitive_index_matches_any_case() {
    let db = open_lite().await;
    put(&db, "users", doc("u1", &[("email", text("Alice@X.com"))])).await;
    exec(
        &db,
        "CREATE INDEX idx_email ON users (email) COLLATE NOCASE",
    )
    .await;
    put(&db, "users", doc("u2", &[("email", text("BOB@x.com"))])).await;

    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'alice@x.COM'").await,
        vec!["u1"]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'bob@X.com'").await,
        vec!["u2"]
    );
}

/// Import `posts` rows from NDJSON, which stores a JSON array as a list.
/// (The document API stores an array field as its JSON text.)
async fn import_posts(db: &Db, ndjson: &str) -> Result<u64, String> {
    db.copy_from_ndjson("posts", ndjson)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn array_index_matches_each_element() {
    let db = open_lite().await;
    import_posts(&db, r#"{"id":"p1","tags":["rust","db"]}"#)
        .await
        .expect("import p1");
    exec(&db, "CREATE UNIQUE INDEX idx_tags ON posts (\"tags[]\")").await;

    // Every element is its own entry: sharing any one of them is a duplicate.
    let err = import_posts(&db, r#"{"id":"p2","tags":["go","db"]}"#)
        .await
        .expect_err("element 'db' is taken");
    assert!(err.contains("unique"), "{err}");
    import_posts(&db, r#"{"id":"p2","tags":["go","web"]}"#)
        .await
        .expect("disjoint elements");
    // Dropping an element frees it.
    import_posts(&db, r#"{"id":"p1","tags":["rust"]}"#)
        .await
        .expect("rewrite p1");
    import_posts(&db, r#"{"id":"p3","tags":["db"]}"#)
        .await
        .expect("freed element");
}

#[tokio::test]
async fn partial_index_covers_only_matching_rows() {
    let db = open_lite().await;
    put(&db, "users", doc("u0", &[("active", Value::Bool(false))])).await;
    exec(
        &db,
        "CREATE UNIQUE INDEX idx_active_email ON users (email) WHERE active = true",
    )
    .await;

    let row = |email: &str, active: bool| [("email", text(email)), ("active", Value::Bool(active))];
    // Inactive rows are outside the index: they may share a value.
    put(&db, "users", doc("u1", &row("a@x", false))).await;
    put(&db, "users", doc("u2", &row("a@x", false))).await;
    put(&db, "users", doc("u3", &row("a@x", true))).await;
    let err = db
        .document_put("users", doc("u4", &row("a@x", true)))
        .await
        .expect_err("a second active row with the value");
    assert!(err.to_string().contains("unique"), "{err}");

    assert_eq!(
        ids(
            &db,
            "SELECT id FROM users WHERE email = 'a@x' AND active = true"
        )
        .await,
        vec!["u3"]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'a@x'").await,
        vec!["u1", "u2", "u3"]
    );
}

#[tokio::test]
async fn numeric_range_lookup_orders_numerically() {
    let db = open_lite().await;
    for (id, n) in [("a", 2), ("b", 5), ("c", 10), ("d", 100), ("e", -3)] {
        put(&db, "nums", doc(id, &[("n", Value::Integer(n))])).await;
    }
    put(&db, "nums", doc("f", &[("n", Value::Float(7.5))])).await;
    exec(&db, "CREATE INDEX idx_n ON nums (n)").await;

    // Compared as text, "10" and "100" would sort before "5".
    assert_eq!(
        ids(&db, "SELECT id FROM nums WHERE n > 5").await,
        vec!["c", "d", "f"]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM nums WHERE n >= 2 AND n < 50").await,
        vec!["a", "b", "c", "f"]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM nums WHERE n BETWEEN -5 AND 5").await,
        vec!["a", "b", "e"]
    );
    assert!(
        ids(&db, "SELECT id FROM nums WHERE n > 50 AND n < 5")
            .await
            .is_empty()
    );
    assert_eq!(
        ids(&db, "SELECT id FROM nums WHERE n = 10").await,
        vec!["c"]
    );
}

#[tokio::test]
async fn index_values_containing_separator_do_not_collide() {
    let db = open_lite().await;
    put(&db, "vals", doc("k0", &[("v", text("seed"))])).await;
    exec(&db, "CREATE INDEX idx_v ON vals (v)").await;
    put(&db, "vals", doc("k1", &[("v", text("a"))])).await;
    put(&db, "vals", doc("k2", &[("v", text("a:b"))])).await;
    put(&db, "vals", doc("k3:x", &[("v", text("a:b:c"))])).await;

    let q = |v: &str| format!("SELECT id FROM vals WHERE v = '{v}'");
    assert_eq!(ids(&db, &q("a")).await, vec!["k1"]);
    assert_eq!(ids(&db, &q("a:b")).await, vec!["k2"]);
    assert_eq!(ids(&db, &q("a:b:c")).await, vec!["k3:x"]);
    assert!(ids(&db, &q("b")).await.is_empty());
}

#[tokio::test]
async fn collections_with_prefix_names_do_not_share_entries() {
    let db = open_lite().await;
    // With keys spelled `{collection}:{field}:{value}`, `t` / `x` / `v:1` and
    // `t:x` / `v` / `1` are the same key.
    put(&db, "t", doc("a", &[("x", text("v:1"))])).await;
    put(&db, "t:x", doc("b", &[("v", text("1"))])).await;
    exec(&db, "CREATE INDEX idx_t ON t (x)").await;
    exec(&db, "CREATE INDEX idx_tx ON \"t:x\" (v)").await;

    assert_eq!(
        ids(&db, "SELECT id FROM t WHERE x = 'v:1'").await,
        vec!["a"]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM \"t:x\" WHERE v = '1'").await,
        vec!["b"]
    );

    exec(&db, "DROP INDEX idx_t").await;
    assert_eq!(
        ids(&db, "SELECT id FROM \"t:x\" WHERE v = '1'").await,
        vec!["b"]
    );
}

#[tokio::test]
async fn truncate_clears_index_entries_but_keeps_index() {
    let db = users_with_email_index(true).await;
    exec(&db, "INSERT INTO users (id, email) VALUES ('u1', 'a@x')").await;
    exec(&db, "TRUNCATE users").await;

    assert!(
        ids(&db, "SELECT id FROM users WHERE email = 'a@x'")
            .await
            .is_empty()
    );
    // The value is free again, and the index still covers new rows.
    exec(&db, "INSERT INTO users (id, email) VALUES ('u2', 'a@x')").await;
    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'a@x'").await,
        vec!["u2"]
    );
    let err = exec_err(&db, "INSERT INTO users (id, email) VALUES ('u3', 'a@x')").await;
    assert!(err.contains("unique"), "{err}");
    let err = exec_err(&db, "CREATE INDEX idx_email ON users (email)").await;
    assert!(err.contains("already exists"), "{err}");
}

#[tokio::test]
async fn drop_collection_removes_indexes() {
    let db = users_with_email_index(true).await;
    exec(&db, "DROP COLLECTION users").await;

    put(&db, "users", doc("u1", &[("email", text("a@x"))])).await;
    // The unique index went with the collection.
    put(&db, "users", doc("u2", &[("email", text("a@x"))])).await;
    // So did its name.
    exec(&db, "CREATE INDEX idx_email ON users (email)").await;
    assert_eq!(
        ids(&db, "SELECT id FROM users WHERE email = 'a@x'").await,
        vec!["u1", "u2"]
    );
}

#[tokio::test]
async fn dropping_a_missing_index_is_an_error_unless_if_exists() {
    let db = open_lite().await;
    let err = exec_err(&db, "DROP INDEX idx_missing").await;
    assert!(err.contains("does not exist"), "{err}");
    exec(&db, "DROP INDEX IF EXISTS idx_missing").await;
}

#[tokio::test]
async fn index_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("index.db");
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage).await.expect("open");
        db.document_put("users", doc("u1", &[("email", text("a@x"))]))
            .await
            .expect("put u1");
        db.execute_sql("CREATE UNIQUE INDEX idx_email ON users (email)", &[])
            .await
            .expect("create index");
        db.document_put("users", doc("u2", &[("email", text("b@x"))]))
            .await
            .expect("put u2");
        db.flush().await.expect("flush");
        db.shutdown().await;
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("reopen storage");
    let db = NodeDbLite::open(storage).await.expect("reopen");
    assert_eq!(
        ids(&*db, "SELECT id FROM users WHERE email = 'b@x'").await,
        vec!["u2"]
    );
    let err = db
        .document_put("users", doc("u3", &[("email", text("a@x"))]))
        .await
        .expect_err("the reloaded unique index still holds a@x");
    assert!(err.to_string().contains("unique"), "{err}");
    let err = db
        .execute_sql("CREATE INDEX idx_email ON users (email)", &[])
        .await
        .expect_err("the definition was reloaded");
    assert!(err.to_string().contains("already exists"), "{err}");
}

/// Import every delta `from` has not had acknowledged into `to`.
fn ship(from: &Db, to: &Db) {
    for delta in from.pending_crdt_deltas().expect("pending deltas") {
        to.import_remote_deltas(&delta.collection, &delta.delta_bytes)
            .expect("import delta");
    }
}

#[tokio::test]
async fn sync_applied_row_is_index_reachable() {
    let writer = open_lite().await;
    let reader = open_lite().await;
    exec(&reader, "CREATE COLLECTION notes").await;
    exec(&reader, "CREATE INDEX idx_tag ON notes (tag)").await;

    put(&writer, "notes", doc("n1", &[("tag", text("red"))])).await;
    put(&writer, "notes", doc("n2", &[("tag", text("blue"))])).await;
    ship(&writer, &reader);

    let q = |tag: &str| format!("SELECT id FROM notes WHERE tag = '{tag}'");
    assert_eq!(ids(&reader, &q("red")).await, vec!["n1"]);
    assert_eq!(ids(&reader, &q("blue")).await, vec!["n2"]);

    put(&writer, "notes", doc("n1", &[("tag", text("green"))])).await;
    writer.document_delete("notes", "n2").await.expect("delete");
    ship(&writer, &reader);

    assert!(ids(&reader, &q("red")).await.is_empty());
    assert_eq!(ids(&reader, &q("green")).await, vec!["n1"]);
    assert!(ids(&reader, &q("blue")).await.is_empty());
}
