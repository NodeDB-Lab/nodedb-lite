// SPDX-License-Identifier: Apache-2.0

//! Cold recovery separates ordinary text from always-authoritative durable rows.

use crate::engine::document::history::ops::is_bitemporal;
use crate::engine::fts::rebuild::build_collection_replacement;
use crate::{
    error::LiteError,
    nodedb::{core::types::NodeDbLite, lock_ext::LockExt},
    storage::engine::StorageEngine,
};
use nodedb_types::Namespace;
use std::collections::BTreeSet;

const META_DOCUMENT_BITEMPORAL_PREFIX: &str = "document_bitemporal:";
#[cfg(test)]
const CRDT_REBUILD_ID_COUNT: usize = 256;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Rebuild ordinary document text and legacy columnar text after an incompatible checkpoint.
    pub(crate) async fn rebuild_text_indices(&self) -> Result<(), LiteError> {
        let permit = self.fts_state.admit_exclusive().await;
        let mut collections: BTreeSet<String> = self
            .crdt
            .lock_or_recover()
            .collection_names()
            .into_iter()
            .collect();
        collections.extend(
            self.fts_state
                .manager
                .lock_or_recover()
                .declaration_revisions()
                .into_keys(),
        );
        for collection in collections {
            if collection.starts_with("__")
                || self.columnar.schema(&collection).is_some()
                || self.is_authoritative_text_collection(&collection).await?
            {
                continue;
            }
            let record = self
                .fts_state
                .manager
                .lock_or_recover()
                .current_declaration_record(&collection);
            self.fts_state
                .manager
                .lock_or_recover()
                .release_for_rebuild(&collection);
            let candidate = build_collection_replacement(
                &*self.storage,
                &self.crdt,
                &self.strict,
                &self.fts_state,
                &collection,
                record,
                &permit,
            )
            .await?;
            self.fts_state
                .manager
                .lock_or_recover()
                .publish_replacement(candidate);
        }
        self.rebuild_sparse_documents().await?;
        self.rebuild_columnar_text().await?;
        Ok(())
    }

    /// Strict and valid-time text replaces checkpoint postings on every open.
    pub(crate) async fn rebuild_authoritative_text_indices(&self) -> Result<(), LiteError> {
        let permit = self.fts_state.admit_exclusive().await;
        let mut collections: BTreeSet<String> =
            self.strict.collection_names().into_iter().collect();
        collections.extend(self.list_bitemporal_collections().await?);
        for collection in collections {
            let record = self
                .fts_state
                .manager
                .lock_or_recover()
                .current_declaration_record(&collection);
            self.fts_state
                .manager
                .lock_or_recover()
                .release_for_rebuild(&collection);
            let candidate = build_collection_replacement(
                &*self.storage,
                &self.crdt,
                &self.strict,
                &self.fts_state,
                &collection,
                record,
                &permit,
            )
            .await?;
            self.fts_state
                .manager
                .lock_or_recover()
                .publish_replacement(candidate);
        }
        Ok(())
    }

    pub(crate) async fn is_authoritative_text_collection(
        &self,
        collection: &str,
    ) -> Result<bool, LiteError> {
        if self.strict.schema(collection).is_some() {
            return Ok(true);
        }
        is_bitemporal(&*self.storage, collection).await
    }

    /// Collection flags are small catalog metadata and retain the legacy scan contract.
    pub(crate) async fn list_bitemporal_collections(&self) -> Result<Vec<String>, LiteError> {
        let entries = self
            .storage
            .scan_prefix(Namespace::Meta, META_DOCUMENT_BITEMPORAL_PREFIX.as_bytes())
            .await?;
        let mut collections = Vec::new();
        for (key, value) in entries {
            if value.first().copied() != Some(1) {
                continue;
            }
            let name = std::str::from_utf8(&key).map_err(|error| LiteError::Serialization {
                detail: format!("valid-time collection flag is not UTF-8: {error}"),
            })?;
            if let Some(collection) = name.strip_prefix(META_DOCUMENT_BITEMPORAL_PREFIX) {
                collections.push(collection.to_owned());
            }
        }
        Ok(collections)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use loro::LoroValue;
    use nodedb_types::text_search::TextSearchParams;

    use super::{CRDT_REBUILD_ID_COUNT, NodeDbLite};
    use crate::nodedb::lock_ext::LockExt;
    use crate::{LiteConfig, PagedbStorageMem};

    #[tokio::test]
    async fn text_rebuild_indexes_every_crdt_id_page() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let config = LiteConfig {
            auto_flush_ms: 0,
            sync_enabled: false,
            ..LiteConfig::default()
        };
        let db = NodeDbLite::open_with_config(storage, config).await.unwrap();
        let count = CRDT_REBUILD_ID_COUNT + 17;
        let mut expected = HashSet::new();
        {
            let mut crdt = db.crdt.lock_or_recover();
            for position in 0..count {
                let id = format!("doc{position:04}");
                crdt.upsert(
                    "articles",
                    &id,
                    &[("body", LoroValue::String("pagecoverage".into()))],
                )
                .unwrap();
                expected.insert(id);
            }
        }
        {
            let fts = db.fts_state.manager.lock_or_recover();
            assert!(
                fts.search(
                    "articles",
                    "body",
                    "pagecoverage",
                    count,
                    &TextSearchParams::default()
                )
                .unwrap()
                .is_empty()
            );
        }

        db.rebuild_text_indices().await.unwrap();

        let fts = db.fts_state.manager.lock_or_recover();
        for field in ["body", ""] {
            let results = fts
                .search(
                    "articles",
                    field,
                    "pagecoverage",
                    count,
                    &TextSearchParams::default(),
                )
                .unwrap();
            let actual: HashSet<String> = results.into_iter().map(|result| result.doc_id).collect();
            assert_eq!(actual, expected, "field '{field}' omits rebuilt documents");
        }
    }
}
