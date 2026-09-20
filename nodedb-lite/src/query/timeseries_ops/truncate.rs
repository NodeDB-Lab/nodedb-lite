// SPDX-License-Identifier: Apache-2.0
//! `TRUNCATE` of a timeseries collection.

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::columnar_ops;
use crate::query::engine::LiteQueryEngine;
use crate::query::truncate::truncated;
use crate::storage::engine::StorageEngine;

/// Clear every sample of `collection`: the metric engine's memtable and
/// partitions, and, when the collection carries a columnar schema, its
/// columnar rows and segments. A collection only the metric engine has
/// seen (opaque ingest with no DDL) is cleared without error.
pub async fn truncate<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<QueryResult, LiteError> {
    if engine.columnar.schema(collection).is_some() {
        engine.columnar.truncate(collection).await?;
    }
    columnar_ops::writes::clear_overlays(engine, collection)?;
    Ok(truncated())
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{MetricSample, TimeRange};

    use super::*;
    use crate::query::engine::test_engine;

    #[tokio::test]
    async fn truncate_without_schema_clears_metric_engine() {
        let engine = test_engine().await;
        for i in 0..3 {
            engine.timeseries.lock().expect("ts").ingest_metric(
                "raw",
                "cpu",
                Vec::new(),
                MetricSample {
                    timestamp_ms: i,
                    value: 1.0,
                },
            );
        }
        let r = truncate(&engine, "raw").await.expect("truncate");
        assert_eq!(r.command.as_deref(), Some("TRUNCATE"));
        assert_eq!(r.rows_affected, 0);
        assert!(
            engine
                .timeseries
                .lock()
                .expect("ts")
                .scan("raw", &TimeRange::new(0, i64::MAX))
                .is_empty()
        );
    }
}
