// SPDX-License-Identifier: Apache-2.0

//! Inverted text index maintenance for document writes.

use crate::nodedb::core::types::NodeDbLite;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Update the inverted text index after a document write.
    ///
    /// Called by `document_put` to keep the text index in sync.
    /// Concatenates all string fields for full-text indexing.
    pub(crate) fn index_document_text(
        &self,
        collection: &str,
        doc_id: &str,
        fields: &std::collections::HashMap<String, nodedb_types::Value>,
    ) {
        let text: String = fields
            .values()
            .filter_map(|v| match v {
                nodedb_types::Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        // Always index locally so local search works.
        self.fts_state
            .manager
            .lock_or_recover()
            .index_document(collection, doc_id, &text);

        // Propagate to Origin via sync outbound queue — unless the sync gate
        // keeps this document local-only.
        #[cfg(not(target_arch = "wasm32"))]
        if self.should_sync_doc(collection, fields)
            && let Some(q) = &self.fts_outbound
        {
            q.stage_index(collection, doc_id, text);
        }
        #[cfg(target_arch = "wasm32")]
        let _ = text;
    }

    /// Remove a document from the text index.
    pub(crate) fn remove_document_text(&self, collection: &str, doc_id: &str) {
        self.fts_state
            .manager
            .lock_or_recover()
            .remove_document(collection, doc_id);

        // Propagate deletion to Origin via sync outbound queue.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.fts_outbound {
            q.stage_delete(collection, doc_id);
        }
    }
}
