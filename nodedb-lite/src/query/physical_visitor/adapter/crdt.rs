// SPDX-License-Identifier: Apache-2.0
//! CrdtOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::CrdtOp;

use crate::error::LiteError;
use crate::query::crdt_ops;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &CrdtOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        CrdtOp::Read {
            collection,
            document_id,
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            Ok(Box::pin(async move {
                crdt_ops::read::handle_read(engine, &col, &doc_id).await
            }))
        }

        CrdtOp::Apply {
            delta, mutation_id, ..
        } => {
            let delta_bytes = delta.clone();
            let mid = *mutation_id;
            Ok(Box::pin(async move {
                crdt_ops::write::handle_apply(engine, &delta_bytes, mid).await
            }))
        }

        CrdtOp::ImportSnapshot { bytes, .. } => {
            let bytes = bytes.clone();
            Ok(Box::pin(async move {
                crdt_ops::write::handle_import_snapshot(engine, &bytes).await
            }))
        }

        // Constraint install/read/drop require the `nodedb_crdt::validator`
        // Constraint-checking subsystem (UNIQUE/FK/NOT NULL validation
        // against a per-collection constraint set). Lite has no local
        // validator wiring today — it only uses `nodedb_crdt::validator`'s
        // `Violation`/`Constraint` *types* to interpret Origin-issued
        // rejections in `reject_delta_with_policy`, not to validate writes
        // itself. Building constraint enforcement locally is a real feature,
        // not a compile fix, so these are unsupported until that lands.
        CrdtOp::SetConstraints { .. } => Err(LiteError::Unsupported {
            detail: "SetConstraints requires the CRDT validator constraint subsystem, \
                     which Lite does not implement locally"
                .into(),
        }),

        CrdtOp::DropConstraints { .. } => Err(LiteError::Unsupported {
            detail: "DropConstraints requires the CRDT validator constraint subsystem, \
                     which Lite does not implement locally"
                .into(),
        }),

        CrdtOp::ReadConstraints { .. } => Err(LiteError::Unsupported {
            detail: "ReadConstraints requires the CRDT validator constraint subsystem, \
                     which Lite does not implement locally"
                .into(),
        }),

        CrdtOp::SetPolicy {
            collection,
            policy_json,
        } => {
            let col = collection.clone();
            let json = policy_json.clone();
            Ok(Box::pin(async move {
                crdt_ops::write::handle_set_policy(engine, &col, &json).await
            }))
        }

        CrdtOp::GetPolicy { collection } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                crdt_ops::read::handle_get_policy(engine, &col).await
            }))
        }

        CrdtOp::ReadAtVersion {
            collection,
            document_id,
            version_vector_json,
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let vv_json = version_vector_json.clone();
            Ok(Box::pin(async move {
                crdt_ops::version::handle_read_at_version(engine, &col, &doc_id, &vv_json).await
            }))
        }

        CrdtOp::GetVersionVector { .. } => Ok(Box::pin(async move {
            crdt_ops::version::handle_get_version_vector(engine).await
        })),

        CrdtOp::ExportDelta {
            from_version_json, ..
        } => {
            let from_json = from_version_json.clone();
            Ok(Box::pin(async move {
                crdt_ops::version::handle_export_delta(engine, &from_json).await
            }))
        }

        CrdtOp::RestoreToVersion {
            collection,
            document_id,
            target_version_json,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let target_json = target_version_json.clone();
            Ok(Box::pin(async move {
                crdt_ops::version::handle_restore_to_version(engine, &col, &doc_id, &target_json)
                    .await
            }))
        }

        CrdtOp::CompactAtVersion {
            target_version_json,
            ..
        } => {
            let target_json = target_version_json.clone();
            Ok(Box::pin(async move {
                crdt_ops::version::handle_compact_at_version(engine, &target_json).await
            }))
        }

        CrdtOp::ListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let path = list_path.clone();
            let idx = *index;
            let fields = fields_json.clone();
            Ok(Box::pin(async move {
                crdt_ops::list::handle_list_insert(engine, &col, &doc_id, &path, idx, &fields).await
            }))
        }

        CrdtOp::ListDelete {
            collection,
            document_id,
            list_path,
            index,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let path = list_path.clone();
            let idx = *index;
            Ok(Box::pin(async move {
                crdt_ops::list::handle_list_delete(engine, &col, &doc_id, &path, idx).await
            }))
        }

        CrdtOp::ListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let path = list_path.clone();
            let from = *from_index;
            let to = *to_index;
            Ok(Box::pin(async move {
                crdt_ops::list::handle_list_move(engine, &col, &doc_id, &path, from, to).await
            }))
        }

        CrdtOp::DocUpsert {
            collection,
            document_id,
            fields_json,
            partial,
            returning,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let fields = fields_json.clone();
            let partial = *partial;
            let returning = returning.clone();
            Ok(Box::pin(async move {
                crdt_ops::doc_row::handle_doc_upsert(
                    engine,
                    &col,
                    &doc_id,
                    &fields,
                    partial,
                    returning.as_ref(),
                )
                .await
            }))
        }

        CrdtOp::DocDelete {
            collection,
            document_id,
            returning,
            ..
        } => {
            let col = collection.clone();
            let doc_id = document_id.clone();
            let returning = returning.clone();
            Ok(Box::pin(async move {
                crdt_ops::doc_row::handle_doc_delete(engine, &col, &doc_id, returning.as_ref())
                    .await
            }))
        }

        // Speculative preview-apply (apply a delta to a fork for preview,
        // committed or discarded later) is an Origin-plane concept: it needs
        // the fork/pending-check machinery Lite does not run locally. Applying
        // it as an ordinary delta would silently commit a write meant to stay
        // speculative, so reject it explicitly rather than misapply it.
        CrdtOp::PreviewApply { .. } => Err(LiteError::Unsupported {
            detail: "PreviewApply requires the CRDT preview/fork subsystem, \
                     which Lite does not implement locally"
                .into(),
        }),
    }
}
