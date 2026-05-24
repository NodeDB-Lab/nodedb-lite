// SPDX-License-Identifier: Apache-2.0

//! Document engine helpers for `NodeDbLite`.
//!
//! Read-path strategy for bitemporal collections (mirrors Origin's choice in
//! `nodedb/src/engine/document/store/engine/get.rs:10-28`):
//!
//! **Option A — switch the read path entirely.**  When a collection is
//! bitemporal, `document_get` reads from `versioned_get_current` (the history
//! table) rather than the CRDT store.  The CRDT store still receives the write
//! via `document_put` so that sync and current-state access both work, but for
//! bitemporal collections the history table is authoritative for reads and
//! `document_delete` appends a tombstone rather than performing a hard delete.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::document::history::ops::{
    is_bitemporal, versioned_get_as_of, versioned_get_current, versioned_put, versioned_tombstone,
};
// Note: versioned_get_current is used only for the non-as_of path of document_get.
use crate::engine::document::history::value::DecodedVersion;
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::{loro_value_to_document, value_to_loro};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Read a single document by id.
    ///
    /// For bitemporal collections, delegates to `versioned_get_current` so the
    /// history table is the source of truth (mirrors Origin get.rs:10-28).
    /// For plain collections, reads directly from the CRDT store.
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        if is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let version = versioned_get_current(&*self.storage, collection, id)
                .await
                .map_err(NodeDbError::storage)?;
            return Ok(version.map(|v| decoded_version_to_document(id, &v)));
        }

        let crdt = self.crdt.lock_or_recover();
        let Some(value) = crdt.read(collection, id) else {
            return Ok(None);
        };
        Ok(Some(loro_value_to_document(id, &value)))
    }

    /// Upsert a document.
    ///
    /// For bitemporal collections: writes to the CRDT store first (so sync and
    /// current-state CRDT reads continue to work), then appends a versioned
    /// `LIVE` record to the history table with `system_from_ms = now`.
    ///
    /// For plain collections: unchanged CRDT put + FTS indexing.
    pub(super) async fn document_put_impl(
        &self,
        collection: &str,
        doc: Document,
    ) -> NodeDbResult<()> {
        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        // Always write to the CRDT store (current-state + sync).
        {
            let mut crdt = self.crdt.lock_or_recover();
            let fields: Vec<(&str, loro::LoroValue)> = doc
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            crdt.upsert(collection, &doc_id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        // For bitemporal collections, also record the versioned history entry.
        if is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let now_ms = system_now_ms();
            let body = document_to_msgpack(&doc);
            versioned_put(
                &*self.storage,
                collection,
                &doc_id,
                &body,
                now_ms,
                None,
                None,
            )
            .await
            .map_err(NodeDbError::storage)?;
        }

        self.index_document_text(collection, &doc_id, &doc.fields);

        Ok(())
    }

    /// Delete a document.
    ///
    /// For bitemporal collections: appends a Tombstone version to the history
    /// table (preserves history for AS-OF queries) but does NOT hard-delete from
    /// the CRDT store — the LIVE history entry takes precedence for reads via
    /// `document_get` which now routes through `versioned_get_current`.
    ///
    /// For plain collections: hard-delete from CRDT + FTS removal (unchanged).
    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        if is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let now_ms = system_now_ms();
            versioned_tombstone(&*self.storage, collection, id, now_ms)
                .await
                .map_err(NodeDbError::storage)?;
            // FTS removal still applies — the document is logically gone now.
            self.remove_document_text(collection, id);
            return Ok(());
        }

        let mut crdt = self.crdt.lock_or_recover();
        crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        drop(crdt);

        self.remove_document_text(collection, id);

        Ok(())
    }

    /// Read a document as-of a system time, optionally filtered by valid_time.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`. Returns an
    /// error when called on a plain document collection.
    ///
    /// When `as_of_ms` is `None`, delegates to `versioned_get_current` (same
    /// result as `document_get` for bitemporal collections). When `as_of_ms`
    /// is `Some(t)`, returns the version visible at system time `t`.
    pub(super) async fn document_get_as_of_impl(
        &self,
        collection: &str,
        id: &str,
        as_of_ms: Option<i64>,
        valid_time_ms: Option<i64>,
    ) -> NodeDbResult<Option<Document>> {
        if !is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            return Err(NodeDbError::storage(
                "document_get_as_of requires a collection created WITH (bitemporal=true)",
            ));
        }

        // When as_of_ms is None, use i64::MAX as the system time so we
        // always see the most-recent version — but still apply the
        // valid_time_ms filter via versioned_get_as_of.  Using
        // versioned_get_current would skip the valid_time filter.
        let sys_as_of = as_of_ms.unwrap_or(i64::MAX);
        let version = versioned_get_as_of(&*self.storage, collection, id, sys_as_of, valid_time_ms)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(version.map(|v| decoded_version_to_document(id, &v)))
    }

    /// Put a document with explicit valid-time bounds into a bitemporal collection.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`. Returns an
    /// error when called on a plain document collection.
    pub(super) async fn document_put_with_valid_time_impl(
        &self,
        collection: &str,
        doc: Document,
        valid_from_ms: Option<i64>,
        valid_until_ms: Option<i64>,
    ) -> NodeDbResult<()> {
        if !is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            return Err(NodeDbError::storage(
                "document_put_with_valid_time requires a collection created WITH (bitemporal=true)",
            ));
        }

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        // Write to CRDT store for current-state access + sync.
        {
            let mut crdt = self.crdt.lock_or_recover();
            let fields: Vec<(&str, loro::LoroValue)> = doc
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            crdt.upsert(collection, &doc_id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        let now_ms = system_now_ms();
        let body = document_to_msgpack(&doc);
        versioned_put(
            &*self.storage,
            collection,
            &doc_id,
            &body,
            now_ms,
            valid_from_ms,
            valid_until_ms,
        )
        .await
        .map_err(NodeDbError::storage)?;

        self.index_document_text(collection, &doc_id, &doc.fields);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Decode a `DecodedVersion` body (msgpack bytes) into a `Document`.
///
/// Uses `nodedb_types::json_msgpack::value_from_msgpack` for decoding,
/// falling back to an empty document on any parse error.
fn decoded_version_to_document(id: &str, version: &DecodedVersion) -> Document {
    use nodedb_types::value::Value;

    let mut doc = Document::new(id);
    if version.body.is_empty() {
        return doc;
    }

    if let Ok(Value::Object(fields)) = nodedb_types::json_msgpack::value_from_msgpack(&version.body)
    {
        for (k, v) in fields {
            doc.set(k, v);
        }
    }

    doc
}

/// Serialize a `Document`'s fields to msgpack for storage in the history table.
fn document_to_msgpack(doc: &Document) -> Vec<u8> {
    // Encode fields as a msgpack map via the JSON bridge (same path as bulk.rs).
    let json = serde_json::to_value(&doc.fields).unwrap_or_default();
    nodedb_types::json_msgpack::json_to_msgpack_or_empty(&json)
}

/// Current wall-clock time in milliseconds since Unix epoch (i64).
fn system_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
