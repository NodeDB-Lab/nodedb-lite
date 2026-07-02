//! DDL handlers for HTAP materialized views.
//!
//! CREATE MATERIALIZED VIEW <target> FROM <source> WITH storage = 'columnar'
//! DROP MATERIALIZED VIEW <target>

use nodedb_types::columnar::{ColumnarProfile, ColumnarSchema};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: CREATE MATERIALIZED VIEW <target> FROM <source> [WITH storage = 'columnar']
    pub(in crate::query) async fn handle_create_materialized_view(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (target, source) = parse_materialized_view_sql(sql)?;

        // Get the source strict schema.
        let source_schema = self.strict.schema(&source).ok_or_else(|| {
            LiteError::Query(format!(
                "source collection '{source}' not found (must be a strict collection)"
            ))
        })?;

        // Create the target columnar collection with the same schema.
        let columnar_schema = ColumnarSchema::new(source_schema.columns)
            .map_err(|e| LiteError::Query(e.to_string()))?;

        self.columnar
            .create_collection(&target, columnar_schema, ColumnarProfile::Plain, false)
            .await?;

        // Register the CDC bridge.
        self.htap.register_view(&source, &target);

        // Register the target as a queryable table.
        self.register_columnar_collection(&target);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "materialized view '{target}' created from '{source}'"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: DROP MATERIALIZED VIEW <target>
    pub(in crate::query) async fn handle_drop_materialized_view(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let target = parts
            .get(3)
            .ok_or(LiteError::Query("expected view name".into()))?
            .to_lowercase();

        // Remove the CDC bridge.
        self.htap.remove_view(&target);

        // Drop the underlying columnar collection.
        self.columnar.drop_collection(&target).await?;

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "materialized view '{target}' dropped"
            ))]],
            rows_affected: 0,
        })
    }
}

/// Parse: CREATE MATERIALIZED VIEW <target> FROM <source> [WITH storage = 'columnar']
fn parse_materialized_view_sql(sql: &str) -> Result<(String, String), LiteError> {
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // Expected: CREATE MATERIALIZED VIEW <target> FROM <source> ...
    let view_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("VIEW"))
        .ok_or(LiteError::Query("expected VIEW keyword".into()))?;

    let target = parts
        .get(view_idx + 1)
        .ok_or(LiteError::Query("expected view name after VIEW".into()))?
        .to_lowercase();

    let from_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .ok_or(LiteError::Query("expected FROM keyword".into()))?;

    let source = parts
        .get(from_idx + 1)
        .ok_or(LiteError::Query(
            "expected source collection after FROM".into(),
        ))?
        .to_lowercase();

    if target.is_empty() || source.is_empty() {
        return Err(LiteError::Query("view and source names required".into()));
    }

    Ok((target, source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let sql =
            "CREATE MATERIALIZED VIEW customer_analytics FROM customers WITH storage = 'columnar'";
        let (target, source) = parse_materialized_view_sql(sql).expect("parse");
        assert_eq!(target, "customer_analytics");
        assert_eq!(source, "customers");
    }

    #[test]
    fn parse_without_with_clause() {
        let sql = "CREATE MATERIALIZED VIEW analytics FROM orders";
        let (target, source) = parse_materialized_view_sql(sql).expect("parse");
        assert_eq!(target, "analytics");
        assert_eq!(source, "orders");
    }

    #[test]
    fn parse_missing_from() {
        let sql = "CREATE MATERIALIZED VIEW analytics";
        assert!(parse_materialized_view_sql(sql).is_err());
    }
}
