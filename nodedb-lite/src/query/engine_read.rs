// SPDX-License-Identifier: Apache-2.0
//! SQL reads the planner sends straight to an engine: the full scan behind
//! `SqlPlan::Scan` and the key lookup behind `SqlPlan::PointGet`.
//!
//! Both match every `EngineType`. An engine Lite cannot read this way
//! returns `Unsupported`, never an empty result.

use std::cmp::Ordering;

use nodedb_query::value_ops::compare_values;
use nodedb_sql::types::*;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use super::document_rows;
use super::engine::{LiteQueryEngine, sql_value_to_string, sql_value_to_value};
use super::kv_ops::sql_read::{kv_key_bytes, kv_select_all, kv_select_key};
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    pub(in crate::query) async fn execute_scan(
        &self,
        collection: &str,
        engine: &EngineType,
    ) -> Result<QueryResult, LiteError> {
        match engine {
            EngineType::DocumentSchemaless => {
                // For bitemporal collections the Loro snapshot may lag storage
                // (it is only saved on explicit flush).  Scan DocumentHistory
                // as the authoritative source for the current set of live IDs.
                let is_bt = crate::engine::document::history::ops::is_bitemporal(
                    &*self.storage,
                    collection,
                )
                .await
                .unwrap_or(false);

                if is_bt {
                    let live_docs = crate::engine::document::history::ops::scan_live_documents(
                        &*self.storage,
                        collection,
                    )
                    .await
                    .map_err(|e| LiteError::Query(e.to_string()))?;
                    let rows = live_docs
                        .iter()
                        .map(|(id, body)| document_rows::body_row(id, body))
                        .collect();
                    return Ok(QueryResult {
                        columns: document_rows::columns(),
                        rows,
                        rows_affected: 0,
                        command: None,
                    });
                }

                let crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
                let ids = crdt.list_ids(collection);
                let rows = ids
                    .iter()
                    .filter_map(|id| {
                        let val = crdt.read(collection, id)?;
                        Some(document_rows::crdt_row(id, &val))
                    })
                    .collect();
                Ok(QueryResult {
                    columns: document_rows::columns(),
                    rows,
                    rows_affected: 0,
                    command: None,
                })
            }
            EngineType::DocumentStrict => {
                let schema = self
                    .strict
                    .schema(collection)
                    .ok_or_else(|| LiteError::collection_not_found("strict", collection))?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let rows = self.strict.list_rows(collection).await?;
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                    command: None,
                })
            }
            // Every columnar-family profile keeps its rows in the columnar
            // engine, which applies the timeseries or spatial profile itself.
            EngineType::Columnar | EngineType::Timeseries | EngineType::Spatial => {
                let schema = self
                    .columnar
                    .schema(collection)
                    .ok_or_else(|| LiteError::collection_not_found("columnar", collection))?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let rows = self.columnar.list_rows(collection).await?;
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                    command: None,
                })
            }
            EngineType::KeyValue => kv_select_all(self, collection).await,
            EngineType::Array => Err(array_refusal("scan")),
            EngineType::ContinuousAggregate => Err(continuous_aggregate_refusal("scan")),
        }
    }

    /// Fetch the row whose `key_column` equals `key`.
    pub(in crate::query) async fn execute_point_get(
        &self,
        collection: &str,
        engine: &EngineType,
        key_column: &str,
        key: &SqlValue,
    ) -> Result<QueryResult, LiteError> {
        let key_str = sql_value_to_string(key);
        match engine {
            EngineType::DocumentSchemaless => {
                let crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
                match crdt.read(collection, &key_str) {
                    Some(val) => {
                        let row = document_rows::crdt_row(&key_str, &val);
                        Ok(QueryResult {
                            columns: document_rows::columns(),
                            rows: vec![row],
                            rows_affected: 0,
                            command: None,
                        })
                    }
                    None => Ok(QueryResult::empty()),
                }
            }
            EngineType::DocumentStrict => {
                let schema = self
                    .strict
                    .schema(collection)
                    .ok_or_else(|| LiteError::collection_not_found("strict", collection))?;
                let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                // The PK column type determines how to parse the key string.
                let pk_col = schema
                    .columns
                    .iter()
                    .find(|c| c.primary_key)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!(
                            "strict collection '{collection}' has no primary key column"
                        ),
                    })?;
                let pk_value = parse_pk_value(&key_str, &pk_col.column_type);
                match self.strict.get(collection, &pk_value).await? {
                    Some(values) => Ok(QueryResult {
                        columns,
                        rows: vec![values],
                        rows_affected: 0,
                        command: None,
                    }),
                    None => Ok(QueryResult {
                        columns,
                        rows: Vec::new(),
                        rows_affected: 0,
                        command: None,
                    }),
                }
            }
            EngineType::KeyValue => kv_select_key(self, collection, &kv_key_bytes(key)).await,
            // A columnar row is found by scanning for the key column's value.
            EngineType::Columnar | EngineType::Timeseries | EngineType::Spatial => {
                let mut result = self.execute_scan(collection, engine).await?;
                let column = result
                    .columns
                    .iter()
                    .position(|c| c == key_column)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!(
                            "collection '{collection}' has no key column '{key_column}'"
                        ),
                    })?;
                let wanted = sql_value_to_value(key);
                result.rows.retain(|row| {
                    row.get(column)
                        .is_some_and(|v| compare_values(v, &wanted) == Ordering::Equal)
                });
                Ok(result)
            }
            EngineType::Array => Err(array_refusal("point lookup")),
            EngineType::ContinuousAggregate => Err(continuous_aggregate_refusal("point lookup")),
        }
    }
}

/// The refusal for a table-shaped `what` on an array. Arrays are read with
/// the `NDARRAY_*` table functions.
fn array_refusal(what: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!(
            "{what} is not supported on an array; read it with NDARRAY_SLICE or \
             NDARRAY_PROJECT"
        ),
    }
}

/// The refusal for `what` on a continuous aggregate named as a relation.
/// Lite's catalog never resolves a continuous aggregate as a relation, so a
/// plan that names one is refused rather than read or written.
pub(in crate::query) fn continuous_aggregate_refusal(what: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!("{what} on a continuous aggregate relation is not supported in Lite"),
    }
}

/// Convert a primary-key string from a SQL literal into the appropriate `Value`
/// variant based on the column's declared type.
pub(super) fn parse_pk_value(
    key_str: &str,
    col_type: &nodedb_types::columnar::ColumnType,
) -> Value {
    use nodedb_types::columnar::ColumnType;
    match col_type {
        ColumnType::Int64 => key_str
            .parse::<i64>()
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::String(key_str.to_string())),
        ColumnType::Uuid => Value::Uuid(key_str.to_string()),
        _ => Value::String(key_str.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_client::NodeDb;

    use super::*;
    use crate::{NodeDbLite, PagedbStorageMem};

    async fn open_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory().await.expect("storage");
        let db = NodeDbLite::open(storage).await.expect("open");
        for sql in [
            "CREATE COLLECTION raw (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')",
            "CREATE COLLECTION items (key TEXT PRIMARY KEY, n INT, label TEXT) \
             WITH (engine='kv')",
            "INSERT INTO raw (key, value) VALUES ('a', 'alpha')",
            "INSERT INTO raw (key, value) VALUES ('b', 'beta')",
            "INSERT INTO raw (key, value) VALUES ('c', 'gamma')",
            "INSERT INTO items (key, n, label) VALUES ('i1', 1, 'one')",
            "INSERT INTO items (key, n, label) VALUES ('i2', 2, 'two')",
            "INSERT INTO items (key, n, label) VALUES ('i3', 3, 'three')",
        ] {
            db.execute_sql(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        db
    }

    async fn query(db: &NodeDbLite<PagedbStorageMem>, sql: &str) -> QueryResult {
        db.execute_sql(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn text(v: &str) -> Value {
        Value::String(v.to_owned())
    }

    #[tokio::test]
    async fn a_point_get_returns_the_raw_row() {
        let db = open_db().await;
        let r = query(&db, "SELECT * FROM raw WHERE key = 'b'").await;
        assert_eq!(r.columns, vec!["key", "value"]);
        assert_eq!(r.rows, vec![vec![text("b"), text("beta")]]);

        let r = query(&db, "SELECT value FROM raw WHERE key = 'b'").await;
        assert_eq!(r.columns, vec!["value"]);
        assert_eq!(r.rows, vec![vec![text("beta")]]);

        let r = query(&db, "SELECT * FROM raw WHERE key = 'missing'").await;
        assert!(r.rows.is_empty());
    }

    #[tokio::test]
    async fn an_in_list_returns_each_named_key() {
        let db = open_db().await;
        let r = query(
            &db,
            "SELECT key FROM raw WHERE key IN ('a', 'c') ORDER BY key",
        )
        .await;
        assert_eq!(r.rows, vec![vec![text("a")], vec![text("c")]]);
    }

    /// An `IN` list reaches the scan as an expression predicate. It must
    /// filter a document scan too, with an ORDER BY alongside.
    #[tokio::test]
    async fn an_in_list_with_order_by_filters_a_document_scan() {
        let db = open_db().await;
        for sql in [
            "CREATE COLLECTION docs",
            "INSERT INTO docs (id, name) VALUES ('d1', 'a')",
            "INSERT INTO docs (id, name) VALUES ('d2', 'b')",
            "INSERT INTO docs (id, name) VALUES ('d3', 'c')",
        ] {
            query(&db, sql).await;
        }
        let r = query(
            &db,
            "SELECT id FROM docs WHERE name IN ('a', 'c') ORDER BY id",
        )
        .await;
        assert_eq!(r.rows, vec![vec![text("d1")], vec![text("d3")]]);

        let r = query(
            &db,
            "SELECT id FROM docs WHERE name NOT IN ('a', 'c') ORDER BY id",
        )
        .await;
        assert_eq!(r.rows, vec![vec![text("d2")]]);
    }

    /// A `LIKE` inside a larger predicate and an `ARRAY[...]` with a column
    /// element both evaluate per row, as on Origin.
    #[tokio::test]
    async fn like_and_array_inside_expression_predicates() {
        let db = open_db().await;
        query(&db, "CREATE COLLECTION people").await;
        for (id, name, n) in [
            ("a1", "Alice", 1),
            ("a2", "amy", 3),
            ("b1", "bob", 9),
            ("c1", "Carl", 2),
        ] {
            query(
                &db,
                &format!("INSERT INTO people (id, name, n) VALUES ('{id}', '{name}', {n})"),
            )
            .await;
        }
        let ids = |r: QueryResult| -> Vec<Value> {
            r.rows.into_iter().map(|row| row[0].clone()).collect()
        };

        let r = query(
            &db,
            "SELECT id FROM people WHERE lower(name) LIKE 'a%' OR n > 5 ORDER BY id",
        )
        .await;
        assert_eq!(ids(r), vec![text("a1"), text("a2"), text("b1")]);

        let r = query(
            &db,
            "SELECT id FROM people WHERE lower(name) NOT LIKE 'a%' AND n < 5 ORDER BY id",
        )
        .await;
        assert_eq!(ids(r), vec![text("c1")]);

        let r = query(
            &db,
            "SELECT id FROM people WHERE name ILIKE 'A%' OR n > 100 ORDER BY id",
        )
        .await;
        assert_eq!(ids(r), vec![text("a1"), text("a2")]);

        let r = query(
            &db,
            "SELECT id FROM people WHERE array_contains(ARRAY[n, 100], 9) ORDER BY id",
        )
        .await;
        assert_eq!(ids(r), vec![text("b1")]);
    }

    #[tokio::test]
    async fn a_scan_filters_projects_orders_and_limits_typed_rows() {
        let db = open_db().await;
        let r = query(
            &db,
            "SELECT key, n FROM items WHERE n > 1 ORDER BY n DESC LIMIT 1",
        )
        .await;
        assert_eq!(r.columns, vec!["key", "n"]);
        assert_eq!(r.rows, vec![vec![text("i3"), Value::Integer(3)]]);

        let r = query(&db, "SELECT label FROM items ORDER BY key LIMIT 2 OFFSET 1").await;
        assert_eq!(r.rows, vec![vec![text("two")], vec![text("three")]]);
    }

    #[tokio::test]
    async fn a_count_aggregates_the_live_rows() {
        let db = open_db().await;
        let r = query(&db, "SELECT COUNT(*) FROM items WHERE n >= 2").await;
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Value::Integer(2));
    }

    #[tokio::test]
    async fn an_expired_row_is_not_returned() {
        let db = open_db().await;
        db.kv_put_with_ttl("raw", "short", b"soon gone", 1)
            .await
            .expect("put with ttl");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let r = query(&db, "SELECT key FROM raw ORDER BY key").await;
        assert_eq!(
            r.rows,
            vec![vec![text("a")], vec![text("b")], vec![text("c")]]
        );
        let r = query(&db, "SELECT * FROM raw WHERE key = 'short'").await;
        assert!(r.rows.is_empty());
    }

    #[tokio::test]
    async fn a_public_api_write_is_visible_to_sql() {
        let db = open_db().await;
        db.kv_put("raw", "d", b"delta").await.expect("put");
        let r = query(&db, "SELECT value FROM raw WHERE key = 'd'").await;
        assert_eq!(r.rows, vec![vec![text("delta")]]);
    }
}
