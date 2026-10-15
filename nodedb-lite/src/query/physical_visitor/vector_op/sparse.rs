// SPDX-License-Identifier: Apache-2.0
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::adapter::LitePhysicalFut;
use crate::query::physical_visitor::vector_sparse::{sparse_delete, sparse_insert, sparse_search};
use crate::storage::engine::StorageEngine;
use nodedb_physical::physical_plan::VectorOp;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &VectorOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => sparse_insert(
            engine,
            collection.as_str().to_string(),
            field_name.clone(),
            doc_id.clone(),
            entries.clone(),
        ),

        VectorOp::SparseSearch {
            collection,
            field_name,
            query_entries,
            top_k,
        } => sparse_search(
            engine,
            collection.as_str().to_string(),
            field_name.clone(),
            query_entries.clone(),
            *top_k,
        ),

        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => Ok(sparse_delete(
            engine,
            collection.as_str().to_string(),
            field_name.clone(),
            doc_id.clone(),
        )),

        // The parent dispatcher routes only the sparse operations above here.
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
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. } => Err(LiteError::BadRequest {
            detail: "vector sparse dispatch received a non-sparse operation".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::super::dispatch::execute_vector_op;
    use super::super::dispatch::tests::{make_engine, qc, run_op, sparse_insert};
    use super::*;
    use nodedb_types::value::Value;
    #[tokio::test]
    async fn vector_op_sparse_insert_then_search_ranks_by_dot_product() {
        let engine = make_engine().await;
        assert_eq!(
            run_op(&engine, sparse_insert("low", vec![(1, 0.5)]))
                .await
                .rows_affected,
            1
        );
        run_op(&engine, sparse_insert("high", vec![(1, 4.0)])).await;
        run_op(&engine, sparse_insert("disjoint", vec![(99, 9.0)])).await;

        let result = run_op(
            &engine,
            VectorOp::SparseSearch {
                collection: qc("col"),
                field_name: "sparse".to_string(),
                query_entries: vec![(1, 1.0)],
                top_k: 10,
            },
        )
        .await;

        assert_eq!(result.columns, vec!["id".to_string(), "score".to_string()]);
        assert_eq!(result.rows.len(), 2, "disjoint document must be excluded");
        assert_eq!(result.rows[0][0], Value::String("high".to_string()));
        assert_eq!(result.rows[1][0], Value::String("low".to_string()));
    }

    #[tokio::test]
    async fn vector_op_sparse_delete_removes_document() {
        let engine = make_engine().await;
        run_op(&engine, sparse_insert("d1", vec![(1, 1.0)])).await;

        let delete = VectorOp::SparseDelete {
            collection: qc("col"),
            field_name: "sparse".to_string(),
            doc_id: "d1".to_string(),
        };
        assert_eq!(run_op(&engine, delete.clone()).await.rows_affected, 1);
        assert_eq!(
            run_op(&engine, delete).await.rows_affected,
            0,
            "deleting an absent document affects no rows"
        );

        let result = run_op(
            &engine,
            VectorOp::SparseSearch {
                collection: qc("col"),
                field_name: "sparse".to_string(),
                query_entries: vec![(1, 1.0)],
                top_k: 10,
            },
        )
        .await;
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn vector_op_sparse_search_on_missing_index_is_empty_not_error() {
        let engine = make_engine().await;
        let result = run_op(
            &engine,
            VectorOp::SparseSearch {
                collection: qc("never_written"),
                field_name: "sparse".to_string(),
                query_entries: vec![(1, 1.0)],
                top_k: 10,
            },
        )
        .await;
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn vector_op_sparse_insert_rejects_non_finite_weight() {
        let engine = make_engine().await;
        match execute_vector_op(&engine, &sparse_insert("d1", vec![(1, f32::NAN)])) {
            Err(LiteError::BadRequest { detail }) => {
                assert!(detail.contains("SparseInsert"), "got: {detail}");
            }
            Err(other) => panic!("expected BadRequest, got Err({other})"),
            Ok(_) => panic!("expected BadRequest, got Ok"),
        }
    }
}
