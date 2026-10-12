// SPDX-License-Identifier: Apache-2.0

//! `FtsIndexDoc` / `FtsDeleteDoc`: index frames keyed by an Origin surrogate.

use std::sync::Arc;

use nodedb_types::Surrogate;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;

fn affected(rows_affected: u64) -> QueryResult {
    QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected,
        command: None,
    }
}

/// `TextOp::FtsIndexDoc`: index the document's text into the
/// whole-document index. The text is every field's text joined in
/// field-name order. Empty `fields`, or fields with no text, remove the
/// document. The write is Origin-sourced and is never staged for sync.
pub(super) fn fts_index_doc<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    surrogate: Surrogate,
    fields: &[(String, String)],
) -> LitePhysicalFut<'a> {
    let document = nodedb_fts::DocumentText::from_fields(fields.iter().cloned());
    if document.is_empty() {
        return fts_delete_doc(engine, permit, collection, Some(surrogate));
    }
    let collection = collection.to_owned();
    let text = document.whole();
    let fts_state = Arc::clone(&engine.fts_state);
    Box::pin(async move {
        let guard = if permit.is_none() {
            Some(fts_state.admit_mutation().await)
        } else {
            None
        };
        let result = async {
        // On Lite the surrogate space is internal to FtsCollectionManager.
        // We use `text` as the string doc_id (stable across frames for the
        // same document). We also register the Origin surrogate → Lite doc_id
        // mapping so FtsDeleteDoc can resolve it precisely.
        let mut mgr = fts_state
            .manager
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;
        if mgr.declaration_for(&collection).is_some() {
            return Err(LiteError::Unsupported {
                detail: format!("unstructured text mutation on '{collection}' retains an active SEARCH INDEX: use source document writes or DROP SEARCH INDEX first"),
            });
        }

        mgr.index_document(&collection, &text, &text)?;
        mgr.register_origin_surrogate(surrogate, &text);
        drop(mgr);
        // The frame came from Origin: nothing is staged, so nothing echoes back.
        Ok(affected(1))

        }.await;
        match guard {
            Some(guard) => guard.finish(result),
            None => result,
        }
    })
}

/// `TextOp::FtsDeleteDoc`: remove the document an Origin surrogate names.
/// No surrogate removes nothing.
pub(super) fn fts_delete_doc<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    collection: &str,
    surrogate: Option<Surrogate>,
) -> LitePhysicalFut<'a> {
    let Some(surrogate) = surrogate else {
        return Box::pin(async { Ok(affected(0)) });
    };
    let collection = collection.to_owned();
    let fts_state = Arc::clone(&engine.fts_state);
    Box::pin(async move {
        let guard = if permit.is_none() {
            Some(fts_state.admit_mutation().await)
        } else {
            None
        };
        let result = async {
        let mut mgr = fts_state
            .manager
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;
        if mgr.declaration_for(&collection).is_some() {
            return Err(LiteError::Unsupported {
                detail: format!("unstructured text mutation on '{collection}' retains an active SEARCH INDEX: use source document writes or DROP SEARCH INDEX first"),
            });
        }

        let removed_doc_id = mgr.remove_by_origin_surrogate(&collection, surrogate)?;
        drop(mgr);
        // The frame came from Origin: nothing is staged, so nothing echoes back.
        Ok(affected(u64::from(removed_doc_id.is_some())))

        }.await;
        match guard {
            Some(guard) => guard.finish(result),
            None => result,
        }
    })
}
