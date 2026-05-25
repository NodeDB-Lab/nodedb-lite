// SPDX-License-Identifier: Apache-2.0

//! DDL handler for bitemporal schemaless document collections.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::document::history::ops::set_bitemporal;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: `CREATE COLLECTION <name> WITH (bitemporal=true)`
    ///
    /// Persists the bitemporal flag for the collection so that subsequent
    /// `document_put`, `document_get`, and `document_delete` operations route
    /// through the history table.  The underlying schemaless document engine
    /// (CRDT) needs no special setup — the flag alone governs the routing.
    pub(in crate::query) async fn handle_create_bitemporal_document(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let name = extract_collection_name(sql)?;

        set_bitemporal(&*self.storage, &name, true)
            .await
            .map_err(|e| LiteError::Query(e.to_string()))?;

        // Register the collection name in the CRDT engine so that the SQL
        // catalog can resolve it immediately for SELECT queries, even before
        // any document has been inserted (Loro's root map has no entry yet).
        self.crdt
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .register_collection(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "bitemporal document collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }
}

/// Extract the collection name from `CREATE COLLECTION <name> ...`.
fn extract_collection_name(sql: &str) -> Result<String, LiteError> {
    let upper = sql.to_uppercase();
    let after_keyword = sql
        .get(
            upper
                .find("COLLECTION")
                .ok_or(LiteError::Query("expected COLLECTION keyword".into()))?
                + 10..,
        )
        .ok_or(LiteError::Query("unexpected end after COLLECTION".into()))?
        .trim();

    let name_end = after_keyword
        .find(|c: char| c == '(' || c == ';' || c.is_whitespace())
        .unwrap_or(after_keyword.len());

    let name = after_keyword[..name_end].trim().to_lowercase();

    if name.is_empty() {
        return Err(LiteError::Query("missing collection name".into()));
    }

    Ok(name)
}
