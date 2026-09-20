// SPDX-License-Identifier: Apache-2.0
//! `TRUNCATE` of a columnar-family collection.
//!
//! Lite keeps plain, timeseries, and spatial collections in the columnar
//! engine under a profile, so one clear serves all three: the columnar
//! rows and segments, every R-tree entry the collection owns, and every
//! sample the timeseries metric engine holds under the same name.

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::truncate::{clear_spatial, truncated};
use crate::storage::engine::StorageEngine;

/// Clear a columnar, timeseries, or spatial collection while keeping it
/// registered with its schema. Errors when the columnar engine has no such
/// collection.
pub async fn truncate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    engine.columnar.truncate(collection).await?;
    clear_overlays(engine, collection)?;
    Ok(truncated())
}

/// Empty the R-tree entries and the metric-engine samples keyed by
/// `collection`.
pub(crate) fn clear_overlays<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<(), LiteError> {
    clear_spatial(engine, collection)?;
    engine
        .timeseries
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?
        .truncate_collection(collection);
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::BoundingBox;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarProfile, ColumnarSchema};
    use nodedb_types::geometry::Geometry;
    use nodedb_types::timeseries::{MetricSample, TimeRange};
    use nodedb_types::value::Value;

    use super::*;
    use crate::query::engine::test_engine;

    async fn seed(engine: &LiteQueryEngine<crate::PagedbStorageMem>, profile: ColumnarProfile) {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Int64),
        ])
        .expect("schema");
        engine
            .columnar
            .create_collection("fam", schema, profile, false)
            .await
            .expect("create");
        for i in 1..=3 {
            engine
                .columnar
                .insert("fam", &[Value::Integer(i), Value::Integer(i * 10)])
                .expect("insert");
        }
    }

    #[tokio::test]
    async fn truncate_clears_rows_rtree_and_samples() {
        let engine = test_engine().await;
        seed(&engine, ColumnarProfile::Plain).await;
        engine.spatial.lock().expect("spatial").index_document(
            "fam",
            "loc",
            "1",
            &Geometry::point(1.0, 1.0),
        );
        engine.timeseries.lock().expect("ts").ingest_metric(
            "fam",
            "cpu",
            Vec::new(),
            MetricSample {
                timestamp_ms: 1,
                value: 1.0,
            },
        );

        let r = truncate(&engine, "fam").await.expect("truncate");
        assert_eq!(r.rows_affected, 0);
        assert_eq!(r.command.as_deref(), Some("TRUNCATE"));
        assert_eq!(engine.columnar.row_count("fam"), 0);
        assert!(engine.columnar.schema("fam").is_some());
        let bbox = BoundingBox::new(0.0, 0.0, 2.0, 2.0);
        assert!(
            engine
                .spatial
                .lock()
                .expect("spatial")
                .search("fam", "loc", &bbox)
                .is_empty()
        );
        assert!(
            engine
                .timeseries
                .lock()
                .expect("ts")
                .scan("fam", &TimeRange::new(0, i64::MAX))
                .is_empty()
        );
    }

    #[tokio::test]
    async fn truncate_unknown_collection_is_an_error() {
        let engine = test_engine().await;
        assert!(truncate(&engine, "nope").await.is_err());
    }
}
