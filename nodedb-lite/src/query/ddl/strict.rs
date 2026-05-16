//! DDL handlers for strict document collection operations.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::parser::parse_strict_create_sql;

impl<S: StorageEngine + StorageEngineSync> LiteQueryEngine<S> {
    /// Handle: CREATE COLLECTION <name> (<col_defs>) WITH storage = 'strict'
    pub(in crate::query) async fn handle_create_strict(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (name, schema) = parse_strict_create_sql(sql)?;

        self.strict.create_collection(&name, schema).await?;

        // Register the new collection in the query engine.
        self.register_strict_collection(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "strict collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: DROP COLLECTION <name> (for strict collections).
    pub(in crate::query) async fn handle_drop_strict(
        &self,
        name: &str,
    ) -> Result<QueryResult, LiteError> {
        self.strict.drop_collection(name).await?;

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "strict collection '{name}' dropped"
            ))]],
            rows_affected: 0,
        })
    }
}
