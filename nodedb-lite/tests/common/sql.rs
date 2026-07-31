//! SQL parity helpers shared by the sql_parity test suite.
//!
//! Provides `OriginPgwire` (a thin wrapper around a `tokio-postgres` client)
//! and `assert_lite_unsupported` for negative-test assertions.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use tokio_postgres::{Client, NoTls, Row};

// ── Lite DB helpers ───────────────────────────────────────────────────────────

/// Open a fresh in-memory Lite database.
pub async fn open_lite() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    NodeDbLite::open(storage).await.expect("NodeDbLite::open")
}

// ── Origin pgwire client ──────────────────────────────────────────────────────

/// Thin wrapper around a `tokio_postgres` connection to the running Origin.
pub struct OriginPgwire {
    client: Client,
    _conn_task: tokio::task::JoinHandle<()>,
}

impl OriginPgwire {
    /// Connect to Origin pgwire at the default address (127.0.0.1:6432)
    /// in trust mode (no password required).
    pub async fn connect() -> Self {
        let conn_str = "host=127.0.0.1 port=6432 user=nodedb dbname=nodedb sslmode=disable";
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .expect("connect to Origin pgwire");

        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                // Connection errors are expected when Origin is killed at end of test.
                let _ = e;
            }
        });

        OriginPgwire {
            client,
            _conn_task: task,
        }
    }

    /// Execute a SQL statement on Origin and return the raw rows.
    pub async fn query(&self, sql: &str) -> Vec<Row> {
        self.client.query(sql, &[]).await.unwrap_or_else(|e| {
            // `tokio_postgres::Error`'s Display is just "db error" — the
            // server's actual message lives in the attached DbError. Surface it
            // or the failure is undiagnosable from CI output alone.
            let detail = match e.as_db_error() {
                Some(db) => format!("{}: {}", db.severity(), db.message()),
                None => e.to_string(),
            };
            panic!("Origin query failed: {detail}\nSQL: {sql}")
        })
    }

    /// Execute a query, returning the raw `tokio_postgres` error instead of
    /// panicking. Useful for bounded-retry polling where "relation does not
    /// exist yet" is an expected transient state to be retried, not a failure.
    pub async fn try_query(&self, sql: &str) -> Result<Vec<Row>, tokio_postgres::Error> {
        self.client.query(sql, &[]).await
    }

    /// Query inside a bounded-retry poll loop, treating a transient server
    /// state as "no rows yet" rather than a failure.
    ///
    /// A collection that has just arrived over sync briefly reports
    /// `retryable schema change` (or "relation does not exist") while its
    /// schema settles on Origin. Those are exactly the states a poll loop
    /// exists to wait through — panicking on them turns a normal timing window
    /// into a red test. The loop's own deadline remains the real failure
    /// condition.
    pub async fn poll_query(&self, sql: &str) -> Vec<Row> {
        match self.try_query(sql).await {
            Ok(rows) => rows,
            Err(e) => {
                // Report rather than swallow. A poll loop that hides the error
                // reports "0 rows" on timeout and gives no clue whether the
                // data never arrived or the query never ran.
                let detail = match e.as_db_error() {
                    Some(db) => format!("{}: {}", db.severity(), db.message()),
                    None => e.to_string(),
                };
                eprintln!("poll_query transient error: {detail}\n  SQL: {sql}");
                Vec::new()
            }
        }
    }

    /// Execute a SQL statement that returns no rows (DDL/DML).
    pub async fn execute(&self, sql: &str) {
        self.client.execute(sql, &[]).await.unwrap_or_else(|e| {
            let detail = if let Some(db) = e.as_db_error() {
                format!(
                    "code={} message={} detail={:?}",
                    db.code().code(),
                    db.message(),
                    db.detail()
                )
            } else {
                format!("{e:#}")
            };
            panic!("Origin execute failed: {detail}\nSQL: {sql}")
        });
    }
}

// ── Normalisation helpers ─────────────────────────────────────────────────────

/// Convert a `QueryResult` row into a sorted key-value map for order-
/// independent comparison.
pub fn normalise_lite_row(result: &QueryResult, row_idx: usize) -> BTreeMap<String, String> {
    result
        .columns
        .iter()
        .zip(result.rows[row_idx].iter())
        .map(|(col, val)| (col.clone(), value_to_string(val)))
        .collect()
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".into(),
        _ => format!("{v:?}"),
    }
}

// ── Negative-test assertions ──────────────────────────────────────────────────

/// Assert that executing `sql` on Lite returns an `Unsupported` error.
///
/// Panics with a descriptive message if:
/// - The query succeeds (silent wrong-result is a bug).
/// - The query returns a different error kind.
pub async fn assert_lite_unsupported(db: &Arc<NodeDbLite<PagedbStorageMem>>, sql: &str) {
    let result = db.execute_sql(sql, &[]).await;
    match result {
        Err(e) => {
            // Walk the error chain: the public trait returns NodeDbError,
            // which wraps LiteError. Check the display string for "unsupported".
            let display = e.to_string();
            assert!(
                display.contains("unsupported")
                    || display.contains("Unsupported")
                    || display.contains("not supported"),
                "expected Unsupported error for SQL: {sql:?}\n  got: {display}"
            );
        }
        Ok(r) => {
            panic!(
                "expected Unsupported error but query succeeded for SQL: {sql:?}\n  \
                 columns: {:?}\n  rows: {}",
                r.columns,
                r.rows.len()
            );
        }
    }
}
