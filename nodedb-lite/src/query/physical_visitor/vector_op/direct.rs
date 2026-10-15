// SPDX-License-Identifier: Apache-2.0

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::adapter::LitePhysicalFut;
use crate::query::physical_visitor::adapter::policy::deny_policy;
use crate::query::physical_visitor::vector_direct::{
    DirectUpdateArgs, DirectWriteArgs, vector_direct_delete, vector_direct_delete_coordinated,
    vector_direct_truncate, vector_direct_truncate_coordinated, vector_direct_update,
    vector_direct_update_coordinated, vector_direct_write, vector_direct_write_coordinated,
};
use crate::storage::engine::StorageEngine;
use nodedb_physical::physical_plan::{VectorDirectWriteIntent, VectorOp};
use nodedb_types::RlsWriteCheck;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    op: &VectorOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        VectorOp::DirectUpsert {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes: _,
            returning,
            rls_filters,
            on_conflict_updates,
            rls_write_check,
        } => {
            deny_policy(
                "VectorOp::DirectUpsert",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            route_write(
                engine,
                permit,
                DirectWriteArgs {
                    collection: collection.as_str().to_string(),
                    field: field.clone(),
                    surrogate: *surrogate,
                    pk_bytes: pk_bytes.clone(),
                    vector: vector.clone(),
                    payload: payload.clone(),
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    intent: VectorDirectWriteIntent::Upsert,
                    on_conflict_updates: on_conflict_updates.clone(),
                },
            )
        }

        VectorOp::DirectInsert {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes: _,
            returning,
            rls_filters,
        } => {
            deny_policy(
                "VectorOp::DirectInsert",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            route_write(
                engine,
                permit,
                DirectWriteArgs {
                    collection: collection.as_str().to_string(),
                    field: field.clone(),
                    surrogate: *surrogate,
                    pk_bytes: pk_bytes.clone(),
                    vector: vector.clone(),
                    payload: payload.clone(),
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    intent: VectorDirectWriteIntent::Insert,
                    on_conflict_updates: Vec::new(),
                },
            )
        }

        VectorOp::DirectInsertIfAbsent {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes: _,
            returning,
            rls_filters,
        } => {
            deny_policy(
                "VectorOp::DirectInsertIfAbsent",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            route_write(
                engine,
                permit,
                DirectWriteArgs {
                    collection: collection.as_str().to_string(),
                    field: field.clone(),
                    surrogate: *surrogate,
                    pk_bytes: pk_bytes.clone(),
                    vector: vector.clone(),
                    payload: payload.clone(),
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    intent: VectorDirectWriteIntent::InsertIfAbsent,
                    on_conflict_updates: Vec::new(),
                },
            )
        }

        VectorOp::DirectDelete {
            collection,
            field,
            targets,
            returning,
            rls_filters,
            rls_write_check,
        } => {
            deny_policy(
                "VectorOp::DirectDelete",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            Ok(route_delete(
                engine,
                permit,
                collection.as_str().to_string(),
                field.clone(),
                targets.clone(),
            ))
        }

        VectorOp::DirectUpdate {
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization: _,
            storage_dtype: _,
            payload_indexes: _,
            returning,
            rls_filters,
            rls_write_check,
        } => {
            deny_policy(
                "VectorOp::DirectUpdate",
                returning.as_ref(),
                &[rls_filters.as_slice()],
                rls_write_check,
            )?;
            route_update(
                engine,
                permit,
                DirectUpdateArgs {
                    collection: collection.as_str().to_string(),
                    field: field.clone(),
                    targets: targets.clone(),
                    new_vector: new_vector.clone(),
                    payload_patch: payload_patch.clone(),
                },
            )
        }

        VectorOp::DirectTruncate {
            collection,
            field,
            restart_identity,
        } => {
            let col = collection.as_str().to_string();
            let restart = *restart_identity;
            let fut = route_truncate(engine, permit, col.clone(), field.clone());
            Ok(Box::pin(async move {
                let result = fut.await?;
                crate::query::truncate::restart_identity(engine, &col, restart);
                Ok(result)
            }))
        }
        // The parent dispatcher routes only the direct writes above here.
        VectorOp::Search { .. }
        | VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. } => Err(LiteError::BadRequest {
            detail: "vector direct dispatch received a non-direct operation".into(),
        }),
    }
}

fn route_write<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: DirectWriteArgs,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match permit {
        Some(permit) => vector_direct_write_coordinated(engine, Some(permit), args),
        None => vector_direct_write(engine, args),
    }
}

fn route_update<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: DirectUpdateArgs,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match permit {
        Some(permit) => vector_direct_update_coordinated(engine, Some(permit), args),
        None => vector_direct_update(engine, args),
    }
}

fn route_delete<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    collection: String,
    field: String,
    targets: nodedb_physical::physical_plan::VectorWriteTargets,
) -> LitePhysicalFut<'a> {
    match permit {
        Some(permit) => {
            vector_direct_delete_coordinated(engine, Some(permit), collection, field, targets)
        }
        None => vector_direct_delete(engine, collection, field, targets),
    }
}

fn route_truncate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    collection: String,
    field: String,
) -> LitePhysicalFut<'a> {
    match permit {
        Some(permit) => vector_direct_truncate_coordinated(engine, Some(permit), collection, field),
        None => vector_direct_truncate(engine, collection, field),
    }
}
