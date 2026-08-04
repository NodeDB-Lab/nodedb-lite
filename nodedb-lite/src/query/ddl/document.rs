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
        name: &str,
    ) -> Result<QueryResult, LiteError> {
        set_bitemporal(&*self.storage, name, true)
            .await
            .map_err(|e| LiteError::Query(e.to_string()))?;

        self.register_document_collection(name, true).await?;

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "bitemporal document collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: `CREATE COLLECTION <name>` with no engine and no flags.
    ///
    /// The plainest form of the statement, and the one a caller writes when
    /// they want the default schemaless document engine. It needs no storage
    /// setup — registering the collection is the whole of it — but it does need
    /// to be registered, or the collection is invisible to the SQL catalog and
    /// to the sync announce until the first write happens to create it
    /// implicitly.
    pub(in crate::query) async fn handle_create_document(
        &self,
        name: &str,
    ) -> Result<QueryResult, LiteError> {
        self.register_document_collection(name, false).await?;

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "document collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: `DROP COLLECTION <name>` for a schemaless document collection.
    ///
    /// Clears the collection's CRDT state, drops its text index and removes the
    /// persisted metadata — the same three steps the programmatic
    /// `drop_collection` performs, so the SQL and API paths leave the store in
    /// the same state.
    pub(in crate::query) async fn handle_drop_document(
        &self,
        name: &str,
    ) -> Result<QueryResult, LiteError> {
        self.crdt
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .clear_collection(name)
            .map_err(|e| LiteError::Query(e.to_string()))?;

        self.fts_state
            .manager
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .drop_collection(name);

        let key = format!("collection:{name}");
        self.storage
            .delete(nodedb_types::Namespace::Meta, key.as_bytes())
            .await
            .map_err(|e| LiteError::Query(format!("storage: {e}")))?;

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!("collection '{name}' dropped"))]],
            rows_affected: 0,
        })
    }

    /// Persist collection metadata and register the name with the CRDT engine.
    async fn register_document_collection(
        &self,
        name: &str,
        bitemporal: bool,
    ) -> Result<(), LiteError> {
        // Persist `CollectionMeta` under `collection:{name}` in `Namespace::Meta`,
        // symmetric with `create_collection` and the KV DDL path. Without this the
        // collection is invisible to two consumers that both read that key:
        //   - `LiteCatalog` (via `load_persisted_collection_metas`), so the SQL
        //     planner resolves `SELECT ... FROM <name>` with the REAL bitemporal
        //     flag instead of "table not found" / a hardcoded non-bitemporal
        //     fallback.
        //   - the sync outbound announce (`get_collection_meta`), so a
        //     `CollectionSchema` frame is emitted before the first delta and the
        //     collection registers on Origin.
        let meta = crate::nodedb::collection::ddl::CollectionMeta {
            name: name.to_string(),
            collection_type: "document".to_string(),
            created_at_ms: crate::runtime::now_millis(),
            fields: Vec::new(),
            config_json: None,
            descriptor_json: None,
            bitemporal,
            crdt: false,
        };
        let key = format!("collection:{name}");
        let bytes =
            sonic_rs::to_vec(&meta).map_err(|e| LiteError::Query(format!("serialize: {e}")))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await
            .map_err(|e| LiteError::Query(format!("storage: {e}")))?;

        // Also register the collection name in the CRDT engine so the SQL
        // catalog can resolve it immediately for SELECT queries, even before
        // any document has been inserted (Loro's root map has no entry yet).
        self.crdt
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .register_collection(name);

        Ok(())
    }
}
