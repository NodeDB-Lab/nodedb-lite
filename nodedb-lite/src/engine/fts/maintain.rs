// SPDX-License-Identifier: Apache-2.0

//! The one place a document write reaches the text index.
//!
//! Every write path — the `NodeDb` trait, SQL DML, rows pushed from Origin,
//! and imported CRDT deltas — keeps the text index current through these
//! functions. `outbound` is where the per-field text is staged for
//! Origin. A write that came from Origin passes `None`, so nothing echoes
//! back to it.

use std::collections::HashMap;
use std::sync::Mutex;

use nodedb_types::Value;

use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::FtsState;
use crate::error::LiteError;
use crate::nodedb::convert::loro_value_to_document;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

/// Where whole-document text is staged for Origin.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use crate::sync::FtsOutbound;

/// Targets without sync have no outbound queue. No value of this type
/// exists, so every caller there passes `None`.
#[cfg(target_arch = "wasm32")]
pub(crate) struct FtsOutbound<S>(std::convert::Infallible, std::marker::PhantomData<S>);

#[cfg(target_arch = "wasm32")]
impl<S> FtsOutbound<S> {
    fn stage_index(
        &self,
        _collection: &str,
        _doc_id: &str,
        _fields: Vec<(String, String)>,
    ) -> Result<(), LiteError> {
        match self.0 {}
    }

    fn stage_delete(&self, _collection: &str, _doc_id: &str) -> Result<(), LiteError> {
        match self.0 {}
    }
}

/// Index a schemaless document whole and per string field, replacing what
/// it held before, and stage its per-field text on `outbound`. A document
/// with no indexable string field stages a removal.
/// Source and local index changes precede outbound Backpressure. Errors do not roll them back.
pub(crate) fn index_document<S: StorageEngine>(
    fts: &FtsState,
    outbound: Option<&FtsOutbound<S>>,
    collection: &str,
    doc_id: &str,
    fields: &HashMap<String, Value>,
) -> Result<(), LiteError> {
    let pairs = fts
        .manager
        .lock_or_recover()
        .index_document_fields(collection, doc_id, fields)?;
    if let Some(q) = outbound {
        if pairs.is_empty() {
            q.stage_delete(collection, doc_id)?;
        } else {
            q.stage_index(collection, doc_id, pairs)?;
        }
    }
    Ok(())
}

/// Remove a document from every text index of its collection, and stage the
/// removal on `outbound`.
/// Source and local index changes precede outbound Backpressure. Errors do not roll them back.
pub(crate) fn remove_document<S: StorageEngine>(
    fts: &FtsState,
    outbound: Option<&FtsOutbound<S>>,
    collection: &str,
    doc_id: &str,
) -> Result<(), LiteError> {
    fts.manager
        .lock_or_recover()
        .remove_document_fields(collection, doc_id)?;
    if let Some(q) = outbound {
        q.stage_delete(collection, doc_id)?;
    }
    Ok(())
}

/// Bring the text entries of CRDT documents in line with their current
/// state: a document that exists is re-indexed, one that does not is
/// removed.
///
/// System collections (`__` prefix) carry no text index, as in the
/// cold-start rebuild.
pub(crate) fn reindex_crdt_documents<'a, S: StorageEngine>(
    fts: &FtsState,
    crdt: &Mutex<CrdtEngine>,
    outbound: Option<&FtsOutbound<S>>,
    collection: &str,
    doc_ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), LiteError> {
    if collection.starts_with("__") {
        return Ok(());
    }
    for doc_id in doc_ids {
        let current = crdt
            .lock_or_recover()
            .read(collection, doc_id)
            .map(|loro_val| loro_value_to_document(doc_id, &loro_val).fields);
        match current {
            Some(fields) => index_document(fts, outbound, collection, doc_id, &fields)?,
            None => remove_document(fts, outbound, collection, doc_id)?,
        }
    }
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;
    use std::sync::Arc;

    async fn outbound() -> FtsOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        FtsOutbound::open(storage).await.unwrap()
    }

    fn state() -> FtsState {
        FtsState::new(crate::engine::fts::manager::test_governor())
    }

    #[tokio::test]
    async fn document_stages_its_string_fields_sorted_by_name() {
        let fts = state();
        let q = outbound().await;
        let fields = HashMap::from([
            ("title".to_owned(), Value::String("gamma".into())),
            ("body".to_owned(), Value::String("alpha beta".into())),
            ("rank".to_owned(), Value::Integer(3)),
        ]);
        index_document(&fts, Some(&q), "docs", "d1", &fields).unwrap();
        q.flush_staging().await.unwrap();

        let indexes = q.drain_indexes(10).await.unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(
            indexes[0].1.fields,
            vec![
                ("body".to_string(), "alpha beta".to_string()),
                ("title".to_string(), "gamma".to_string()),
            ]
        );
        assert_eq!(q.pending_delete_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn document_without_string_fields_stages_a_delete() {
        let fts = state();
        let q = outbound().await;
        let fields = HashMap::from([("rank".to_owned(), Value::Integer(3))]);
        index_document(&fts, Some(&q), "docs", "d1", &fields).unwrap();
        q.flush_staging().await.unwrap();

        assert_eq!(q.pending_index_count().await.unwrap(), 0);
        let deletes = q.drain_deletes(10).await.unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].1.doc_id, "d1");
    }
}
