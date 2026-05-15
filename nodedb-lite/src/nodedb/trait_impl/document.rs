// SPDX-License-Identifier: Apache-2.0

//! Document engine helpers for `NodeDbLite`.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::{loro_value_to_document, value_to_loro};
use crate::storage::engine::{StorageEngine, StorageEngineSync};

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
    /// Read a single document by id from the CRDT store and decode it into the
    /// public `Document` type. Returns `Ok(None)` if the key is absent or has
    /// been tombstoned.
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        let crdt = self.crdt.lock_or_recover();

        let Some(value) = crdt.read(collection, id) else {
            return Ok(None);
        };

        Ok(Some(loro_value_to_document(id, &value)))
    }

    /// Upsert a document into the CRDT store and re-index its text fields for FTS.
    ///
    /// When `doc.id` is empty a fresh UUIDv7 is generated. The CRDT lock is
    /// released before text indexing to avoid holding it across the FTS write.
    pub(super) async fn document_put_impl(
        &self,
        collection: &str,
        doc: Document,
    ) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        let fields: Vec<(&str, loro::LoroValue)> = doc
            .fields
            .iter()
            .map(|(k, v)| (k.as_str(), value_to_loro(v)))
            .collect();

        crdt.upsert(collection, &doc_id, &fields)
            .map_err(NodeDbError::storage)?;
        drop(crdt);

        self.index_document_text(collection, &doc_id, &doc.fields);

        Ok(())
    }

    /// Delete a document from the CRDT store and remove its entries from the
    /// FTS index. The CRDT lock is released before FTS removal so the two
    /// operations do not contend.
    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();

        crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        drop(crdt);

        self.remove_document_text(collection, id);

        Ok(())
    }
}
