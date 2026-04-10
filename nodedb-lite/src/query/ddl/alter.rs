//! DDL handler for ALTER TABLE operations.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::parser::parse_column_def;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: ALTER TABLE <name> ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT ...]
    pub(in crate::query) async fn handle_alter_add_column(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let upper = sql.to_uppercase();

        // Extract table name: word after ALTER TABLE.
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let table_name = parts
            .get(2)
            .ok_or(LiteError::Query("ALTER TABLE requires a table name".into()))?
            .to_lowercase();

        // Find the column definition after ADD [COLUMN].
        let add_pos = upper
            .find("ADD COLUMN ")
            .map(|p| p + 11)
            .or_else(|| upper.find("ADD ").map(|p| p + 4))
            .ok_or(LiteError::Query("expected ADD [COLUMN]".into()))?;

        let col_def_str = sql[add_pos..].trim();
        let column = parse_column_def(col_def_str)?;

        // Try strict first, then columnar.
        let is_strict = {
            let strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            strict.schema(&table_name).is_some()
        };

        if is_strict {
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(strict.alter_add_column(&table_name, column))
            })?;
            return Ok(QueryResult {
                columns: vec!["result".into()],
                rows: vec![vec![Value::String(format!(
                    "column added to strict collection '{table_name}'"
                ))]],
                rows_affected: 0,
            });
        }

        let is_columnar = {
            let columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            columnar.schema(&table_name).is_some()
        };

        if is_columnar {
            let mut columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(columnar.alter_add_column(&table_name, column))
            })?;
            return Ok(QueryResult {
                columns: vec!["result".into()],
                rows: vec![vec![Value::String(format!(
                    "column added to columnar collection '{table_name}'"
                ))]],
                rows_affected: 0,
            });
        }

        Err(LiteError::Query(format!(
            "collection '{table_name}' not found (ALTER TABLE only works on strict/columnar collections)"
        )))
    }
}
