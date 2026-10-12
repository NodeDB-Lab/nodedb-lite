// SPDX-License-Identifier: Apache-2.0
use super::super::writes::point_update_admitted;
use super::DocumentJoin;
use super::UpdateValue;
use super::source::{build_join_map, collect_ids, extract_field_str, fetch_document_value};
use crate::error::LiteError;
use crate::query::assignments::{joined_row, literal_updates, resolve_updates};
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
/// UpdateFromJoin: update target rows matched by an equi-join with a source collection.
///
/// Execution within one Lite (single-node) transaction:
/// 1. Scan source collection and build a hash map keyed by `source_join_col`.
/// 2. Scan target collection.
/// 3. For each target row whose `target_join_col` value exists in the hash map,
///    apply the `updates` assignments (merged document: target fields + source
///    fields qualified as `<source_alias>.<field>`).
/// 4. All writes go through `point_update` so CRDT vs strict routing is preserved.
pub async fn update_from_join<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    target_collection: &str,
    source_collection: &str,
    source_alias: &str,
    target_join_col: &str,
    source_join_col: &str,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    update_from_join_coordinated(
        engine,
        None,
        DocumentJoin {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
        },
        updates,
    )
    .await
}

pub(crate) async fn update_from_join_coordinated<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: Option<&crate::engine::fts::coordinator::TextMutationPermit>,
    join: DocumentJoin<'_>,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    if let Some(permit) = permit {
        return update_from_join_admitted(engine, permit, join, updates).await;
    }
    let guard = engine.fts_state.admit_mutation().await;
    let result = update_from_join_admitted(engine, guard.permit(), join, updates).await;
    guard.finish(result)
}

pub(crate) async fn update_from_join_admitted<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    permit: &crate::engine::fts::coordinator::TextMutationPermit,
    join: DocumentJoin<'_>,
    updates: &[(String, UpdateValue)],
) -> Result<QueryResult, LiteError> {
    let DocumentJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
    } = join;
    // Step 1: build join map from source collection.
    let source_map = build_join_map(engine, source_collection, source_join_col).await?;

    // Step 2: scan target collection to find matching rows. Every post-image
    // is resolved before the first write, so an assignment that fails leaves
    // every row unchanged.
    let target_ids = collect_ids(engine, target_collection).await?;

    let mut planned: Vec<(&str, Vec<(String, UpdateValue)>)> = Vec::new();
    for doc_id in &target_ids {
        let target_val = fetch_document_value(engine, target_collection, doc_id).await?;
        let Some(join_key) = extract_field_str(&target_val, target_join_col) else {
            continue;
        };
        let Some(source_val) = source_map.get(&join_key) else {
            continue;
        };
        let row = Value::Object(joined_row(&target_val, source_val, source_alias));
        planned.push((doc_id, literal_updates(resolve_updates(updates, &row)?)?));
    }

    for (doc_id, resolved) in &planned {
        point_update_admitted(engine, permit, target_collection, doc_id, resolved).await?;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: planned.len() as u64,
        command: Some("UPDATE".into()),
    })
}

#[cfg(test)]
mod tests {
    use crate::NodeDbLite;
    use crate::PagedbStorageMem;

    async fn make_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        NodeDbLite::open(storage).await.unwrap()
    }

    /// update_from_join on two empty collections returns 0 rows_affected without error.
    #[tokio::test]
    async fn update_from_join_empty_collections() {
        let db = make_db().await;
        let result = super::update_from_join(
            &db.query_engine,
            "target_ufj",
            "source_ufj",
            "s",
            "tid",
            "sid",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_affected, 0);
    }

    #[tokio::test]
    async fn update_from_join_borrows_existing_admission() -> Result<(), Box<dyn std::error::Error>>
    {
        use nodedb_types::value::Value;
        let db = make_db().await;
        for collection in ["source", "target"] {
            let bytes =
                zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([
                    ("id".into(), Value::String("one".into())),
                    ("body".into(), Value::String("original".into())),
                ])))?;
            crate::query::document_ops::writes::point_put(
                &db.query_engine,
                collection,
                "one",
                &bytes,
            )
            .await?;
        }
        let updates = vec![(
            "body".into(),
            super::super::UpdateValue::Literal(zerompk::to_msgpack_vec(&Value::String(
                "joined".into(),
            ))?),
        )];
        let guard = db.query_engine.fts_state.admit_mutation().await;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::update_from_join_coordinated(
                &db.query_engine,
                Some(guard.permit()),
                super::DocumentJoin {
                    target_collection: "target",
                    source_collection: "source",
                    source_alias: "s",
                    target_join_col: "id",
                    source_join_col: "id",
                },
                &updates,
            ),
        )
        .await?;
        let result = guard.finish(result)?;
        assert_eq!(result.rows_affected, 1);
        let fields =
            super::super::source::fetch_document_value(&db.query_engine, "target", "one").await?;
        assert_eq!(fields.get("body"), Some(&Value::String("joined".into())));
        Ok(())
    }

    /// An assignment reads target fields by name and source fields as
    /// `"<alias>.<field>"`, the shape Origin's plan conversion qualifies to.
    #[tokio::test]
    async fn update_from_join_evaluates_target_and_source_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        use nodedb_query::expr::SqlExpr as QExpr;
        use nodedb_query::expr::types::BinaryOp;
        use nodedb_types::value::Value;
        let db = make_db().await;
        for (collection, qty) in [("stock_src", 5), ("stock_dst", 2)] {
            let bytes =
                zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([
                    ("id".into(), Value::String("one".into())),
                    ("qty".into(), Value::Integer(qty)),
                ])))?;
            crate::query::document_ops::writes::point_put(
                &db.query_engine,
                collection,
                "one",
                &bytes,
            )
            .await?;
        }
        let updates = vec![(
            "qty".into(),
            super::super::UpdateValue::Expr(QExpr::BinaryOp {
                left: Box::new(QExpr::Column("qty".into())),
                op: BinaryOp::Add,
                right: Box::new(QExpr::BinaryOp {
                    left: Box::new(QExpr::Column("s.qty".into())),
                    op: BinaryOp::Mul,
                    right: Box::new(QExpr::Literal(Value::Integer(2))),
                }),
            }),
        )];
        let result = super::update_from_join(
            &db.query_engine,
            "stock_dst",
            "stock_src",
            "s",
            "id",
            "id",
            &updates,
        )
        .await?;
        assert_eq!(result.rows_affected, 1);
        let fields =
            super::super::source::fetch_document_value(&db.query_engine, "stock_dst", "one")
                .await?;
        assert_eq!(fields.get("qty"), Some(&Value::Integer(12)));
        Ok(())
    }
}
