// SPDX-License-Identifier: Apache-2.0

//! Secondary indexes on strict collections. Index entries are written in the
//! same storage batch as each row, so they need no flush to survive a
//! reopen. Lite-only checks; Origin is not started.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault, PagedbStorageMem, StorageEngine};
use nodedb_types::value::Value;

use crate::common::sql::open_lite;

const CREATE: &str = "CREATE COLLECTION people (
    id    BIGINT NOT NULL PRIMARY KEY,
    email TEXT,
    n     BIGINT
) WITH storage = 'strict'";

async fn exec<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str) {
    db.execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn exec_err<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str) -> String {
    match db.execute_sql(sql, &[]).await {
        Ok(_) => panic!("expected an error for {sql}"),
        Err(e) => e.to_string(),
    }
}

/// The sorted `id` column of `sql`.
async fn ids<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str) -> Vec<i64> {
    let result = db
        .execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut ids: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Integer(id)) => *id,
            other => panic!("{sql}: id column is {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

async fn people(unique: bool) -> Arc<NodeDbLite<PagedbStorageMem>> {
    let db = open_lite().await;
    exec(&*db, CREATE).await;
    let kind = if unique { "UNIQUE INDEX" } else { "INDEX" };
    exec(
        &*db,
        &format!("CREATE {kind} idx_people_email ON people (email)"),
    )
    .await;
    db
}

fn by_email(email: &str) -> String {
    format!("SELECT id FROM people WHERE email = '{email}'")
}

#[tokio::test]
async fn strict_indexed_lookup_after_insert() {
    let db = open_lite().await;
    exec(&*db, CREATE).await;
    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (1, 'Ann@X', 1)",
    )
    .await;
    // Rows stored before the index is created are built into it.
    exec(
        &*db,
        "CREATE INDEX idx_people_email ON people (email) COLLATE NOCASE",
    )
    .await;
    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (2, 'Bob@X', 2)",
    )
    .await;

    // Only the case-insensitive index matches another case: the lookup is
    // answered from the index.
    assert_eq!(ids(&*db, &by_email("ann@x")).await, vec![1]);
    assert_eq!(ids(&*db, &by_email("BOB@X")).await, vec![2]);
}

#[tokio::test]
async fn strict_indexed_lookup_after_update() {
    let db = people(false).await;
    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (1, 'old@x', 1)",
    )
    .await;
    exec(&*db, "UPDATE people SET email = 'new@x' WHERE id = 1").await;

    assert!(ids(&*db, &by_email("old@x")).await.is_empty());
    assert_eq!(ids(&*db, &by_email("new@x")).await, vec![1]);
}

#[tokio::test]
async fn strict_indexed_lookup_after_delete() {
    let db = people(false).await;
    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (1, 'a@x', 1), (2, 'a@x', 2)",
    )
    .await;
    exec(&*db, "DELETE FROM people WHERE id = 1").await;

    assert_eq!(ids(&*db, &by_email("a@x")).await, vec![2]);
    exec(&*db, "TRUNCATE people").await;
    assert!(ids(&*db, &by_email("a@x")).await.is_empty());
}

#[tokio::test]
async fn strict_unique_index_rejects_duplicate() {
    let db = people(true).await;
    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (1, 'a@x', 1)",
    )
    .await;

    let err = exec_err(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (2, 'a@x', 2)",
    )
    .await;
    assert!(err.contains("unique"), "{err}");

    // A statement breaking the index writes none of its rows.
    let err = exec_err(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (3, 'b@x', 3), (4, 'b@x', 4)",
    )
    .await;
    assert!(err.contains("unique"), "{err}");
    assert_eq!(ids(&*db, "SELECT id FROM people").await, vec![1]);

    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (5, 'c@x', 5)",
    )
    .await;
    let err = exec_err(&*db, "UPDATE people SET email = 'z@x'").await;
    assert!(err.contains("unique"), "{err}");
    assert_eq!(ids(&*db, &by_email("a@x")).await, vec![1]);
    assert_eq!(ids(&*db, &by_email("c@x")).await, vec![5]);
    assert!(ids(&*db, &by_email("z@x")).await.is_empty());

    // Rewriting a row with its own value is not a duplicate.
    exec(&*db, "UPDATE people SET email = 'a@x' WHERE id = 1").await;
}

#[tokio::test]
async fn strict_range_lookup() {
    let db = open_lite().await;
    exec(&*db, CREATE).await;
    exec(&*db, "CREATE INDEX idx_people_n ON people (n)").await;
    exec(
        &*db,
        "INSERT INTO people (id, email, n) VALUES \
         (1, 'a', 2), (2, 'b', 5), (3, 'c', 10), (4, 'd', 100), (5, 'e', -3)",
    )
    .await;

    assert_eq!(
        ids(&*db, "SELECT id FROM people WHERE n > 5").await,
        vec![3, 4]
    );
    assert_eq!(
        ids(&*db, "SELECT id FROM people WHERE n >= 2 AND n < 50").await,
        vec![1, 2, 3]
    );
    assert_eq!(
        ids(&*db, "SELECT id FROM people WHERE n BETWEEN -5 AND 5").await,
        vec![1, 2, 5]
    );
}

#[tokio::test]
async fn strict_index_survives_reopen_without_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("strict_index.db");
    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage).await.expect("open");
        exec(&*db, CREATE).await;
        exec(
            &*db,
            "CREATE UNIQUE INDEX idx_people_email ON people (email)",
        )
        .await;
        exec(
            &*db,
            "INSERT INTO people (id, email, n) VALUES (1, 'a@x', 1), (2, 'b@x', 2)",
        )
        .await;
        // No flush: strict rows and their entries are already stored.
        db.shutdown().await;
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("reopen storage");
    let db = NodeDbLite::open(storage).await.expect("reopen");
    assert_eq!(ids(&*db, &by_email("b@x")).await, vec![2]);
    let err = exec_err(
        &*db,
        "INSERT INTO people (id, email, n) VALUES (3, 'a@x', 3)",
    )
    .await;
    assert!(err.contains("unique"), "{err}");
}
