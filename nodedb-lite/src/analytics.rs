// SPDX-License-Identifier: Apache-2.0

//! Production analytics and weighted edge-list import APIs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::error::NodeDbResult;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::NodeDbLite;
use crate::config::LiteConfig;
use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::graph_diagnostics::GraphImportMeasurements;
use crate::graph_import;
pub use crate::graph_import::WeightedEdgeListSpec;
use crate::query::graph_ops::algorithms::{
    materialize_dense_raw, run_dense_raw, run_dense_raw_prevalidated_sssp,
    validate_dense_sssp_weights,
};
pub use crate::query::graph_ops::analytics_results::AnalyticsRawValues as DenseAnalyticsValues;
use crate::storage::encryption::Encryption;
use crate::storage::engine::StorageEngine;
use crate::storage::pagedb_storage::PagedbStorageDefault;

/// Dense algorithm output whose values retain CSR node order.
pub struct DenseAnalyticsResult(DenseAnalyticsValues);

struct WeightedEdgeListOpen<'a> {
    path: &'a Path,
    encryption: Encryption,
    config: LiteConfig,
    page_size: usize,
    vertices: &'a Path,
    edges: &'a Path,
    spec: &'a WeightedEdgeListSpec,
}

impl DenseAnalyticsResult {
    /// Return the algorithm kind carried by this dense result.
    #[must_use]
    pub fn algorithm(&self) -> GraphAlgorithm {
        match &self.0 {
            DenseAnalyticsValues::PageRank(_) => GraphAlgorithm::PageRank,
            DenseAnalyticsValues::Wcc(_) => GraphAlgorithm::Wcc,
            DenseAnalyticsValues::Lcc(_) => GraphAlgorithm::Lcc,
            DenseAnalyticsValues::Sssp(_) => GraphAlgorithm::Sssp,
            DenseAnalyticsValues::LabelPropagation(_) => GraphAlgorithm::LabelPropagation,
        }
    }

    /// Return the number of values in CSR node order.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.0 {
            DenseAnalyticsValues::PageRank(values)
            | DenseAnalyticsValues::Lcc(values)
            | DenseAnalyticsValues::Sssp(values) => values.len(),
            DenseAnalyticsValues::Wcc(values) | DenseAnalyticsValues::LabelPropagation(values) => {
                values.len()
            }
        }
    }

    /// Whether this result contains no dense values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the typed dense values in CSR node order.
    #[must_use]
    pub fn values(&self) -> &DenseAnalyticsValues {
        &self.0
    }

    /// Consume the result and return its typed dense values.
    #[must_use]
    pub fn into_values(self) -> DenseAnalyticsValues {
        self.0
    }
}

/// Proof that a collection's weighted adjacency passed SSSP validation.
pub struct ValidatedSssp {
    collection: String,
}

#[derive(Debug)]
pub struct GraphImportMetrics {
    pub vertices: usize,
    pub edges: usize,
    pub load_seconds: f64,
    pub prepare_seconds: f64,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Import a weighted edge list into an open graph using bounded ordinary batches.
    pub async fn import_weighted_edge_list(
        &self,
        vertices: &Path,
        edges: &Path,
        spec: &WeightedEdgeListSpec,
    ) -> Result<GraphImportMetrics, LiteError> {
        graph_import::import(
            &*self.storage,
            &self.csr,
            vertices,
            edges,
            spec,
            false,
            None,
        )
        .await
    }

    /// Import while collecting generic stage measurements.
    pub async fn import_weighted_edge_list_measured(
        &self,
        vertices: &Path,
        edges: &Path,
        spec: &WeightedEdgeListSpec,
    ) -> Result<(GraphImportMetrics, GraphImportMeasurements), LiteError> {
        let mut measurements = GraphImportMeasurements::default();
        let metrics = graph_import::import(
            &*self.storage,
            &self.csr,
            vertices,
            edges,
            spec,
            false,
            Some(&mut measurements),
        )
        .await?;
        Ok((metrics, measurements))
    }

    /// Run an algorithm and return dense values without presentation conversion.
    pub fn run_dense_analytics(
        &self,
        algorithm: GraphAlgorithm,
        params: &AlgoParams,
    ) -> Result<DenseAnalyticsResult, LiteError> {
        Ok(DenseAnalyticsResult(run_dense_raw(
            &self.csr, algorithm, params,
        )?))
    }

    /// Validate all weights for a collection before a timed SSSP invocation.
    pub fn validate_sssp_weights(
        &self,
        collection: impl Into<String>,
    ) -> Result<ValidatedSssp, LiteError> {
        let collection = collection.into();
        validate_dense_sssp_weights(&self.csr, &collection)?;
        Ok(ValidatedSssp { collection })
    }

    /// Run SSSP using a collection-bound validation token.
    pub fn run_validated_sssp(
        &self,
        params: &AlgoParams,
        validated: ValidatedSssp,
    ) -> Result<DenseAnalyticsResult, LiteError> {
        if params.collection != validated.collection {
            return Err(LiteError::BadRequest {
                detail: "SSSP validation token belongs to another collection".into(),
            });
        }
        Ok(DenseAnalyticsResult(run_dense_raw_prevalidated_sssp(
            &self.csr,
            GraphAlgorithm::Sssp,
            params,
        )?))
    }

    /// Convert dense output to the normal public query result.
    pub fn materialize_dense_analytics(
        &self,
        collection: &str,
        algorithm: GraphAlgorithm,
        result: DenseAnalyticsResult,
    ) -> Result<QueryResult, LiteError> {
        let DenseAnalyticsResult(raw) = result;
        let mut query = materialize_dense_raw(&self.csr, collection, algorithm, raw)?;
        if algorithm == GraphAlgorithm::LabelPropagation {
            let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = collection_csr(&map, collection)?;
            for row in &mut query.rows {
                if let Some(Value::Integer(label)) = row.get(1) {
                    let label = u32::try_from(*label).map_err(|_| LiteError::Storage {
                        detail: format!("invalid label propagation result {label}"),
                    })?;
                    row[1] = Value::String(csr.node_name_raw(label).to_string());
                }
            }
        }
        Ok(query)
    }

    /// Compute BFS distances in CSR node order.
    pub fn bfs_distances(&self, collection: &str, source: &str) -> Result<Vec<i64>, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = collection_csr(&map, collection)?;
        let source_id = csr.node_id_raw(source).ok_or_else(|| LiteError::Storage {
            detail: format!("source vertex '{source}' is absent"),
        })?;
        Ok(csr.bfs_both_distances_raw(source_id))
    }

    /// Materialize BFS distances after timing-sensitive traversal completes.
    pub fn materialize_bfs_distances(
        &self,
        collection: &str,
        distances: Vec<i64>,
    ) -> Result<QueryResult, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = collection_csr(&map, collection)?;
        if distances.len() != csr.node_count() {
            return Err(LiteError::Storage {
                detail: "BFS distance count does not match the collection CSR".into(),
            });
        }
        Ok(QueryResult {
            columns: vec!["node_id".into(), "distance".into()],
            rows: distances
                .into_iter()
                .enumerate()
                .map(|(node, distance)| {
                    vec![
                        Value::String(csr.node_name_raw(node as u32).to_string()),
                        Value::Integer(distance),
                    ]
                })
                .collect(),
            rows_affected: 0,
        })
    }
}

impl NodeDbLite<PagedbStorageDefault> {
    /// Create a durable PageDB graph from bounded, sorted weighted edge-list input.
    pub async fn open_and_import_weighted_edge_list_at_path(
        path: &Path,
        encryption: Encryption,
        config: LiteConfig,
        page_size: usize,
        vertices: &Path,
        edges: &Path,
        spec: &WeightedEdgeListSpec,
    ) -> NodeDbResult<(Arc<Self>, GraphImportMetrics)> {
        let request = WeightedEdgeListOpen {
            path,
            encryption,
            config,
            page_size,
            vertices,
            edges,
            spec,
        };
        let (database, metrics, _) =
            Self::open_and_import_weighted_edge_list_inner(request, false).await?;
        Ok((database, metrics))
    }

    /// Create a durable graph while collecting generic import measurements.
    pub async fn open_and_import_weighted_edge_list_measured_at_path(
        path: &Path,
        encryption: Encryption,
        config: LiteConfig,
        page_size: usize,
        vertices: &Path,
        edges: &Path,
        spec: &WeightedEdgeListSpec,
    ) -> NodeDbResult<(Arc<Self>, GraphImportMetrics, GraphImportMeasurements)> {
        let request = WeightedEdgeListOpen {
            path,
            encryption,
            config,
            page_size,
            vertices,
            edges,
            spec,
        };
        let (database, metrics, measurements) =
            Self::open_and_import_weighted_edge_list_inner(request, true).await?;
        Ok((
            database,
            metrics,
            measurements.expect("measurement requested"),
        ))
    }

    async fn open_and_import_weighted_edge_list_inner(
        request: WeightedEdgeListOpen<'_>,
        measured: bool,
    ) -> NodeDbResult<(
        Arc<Self>,
        GraphImportMetrics,
        Option<GraphImportMeasurements>,
    )> {
        request.config.validate()?;
        let storage = PagedbStorageDefault::open_with_policy_and_page_size(
            request.path,
            request.encryption,
            request.config.corruption_policy,
            request.page_size,
        )
        .await?;
        let local_csr = Arc::new(Mutex::new(HashMap::<String, CsrIndex>::new()));
        let mut measurements = measured.then(GraphImportMeasurements::default);
        let metrics = graph_import::import(
            &storage,
            &local_csr,
            request.vertices,
            request.edges,
            request.spec,
            true,
            measurements.as_mut(),
        )
        .await?;
        let prepared_csr =
            std::mem::take(&mut *local_csr.lock().map_err(|_| LiteError::LockPoisoned)?);
        let database =
            Self::open_with_config_and_csr(storage, request.config, prepared_csr).await?;
        Ok((database, metrics, measurements))
    }
}

fn collection_csr<'a>(
    map: &'a HashMap<String, CsrIndex>,
    collection: &str,
) -> Result<&'a CsrIndex, LiteError> {
    map.get(collection).ok_or_else(|| LiteError::Storage {
        detail: format!("graph collection '{collection}' is not loaded"),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nodedb_types::Namespace;

    use super::*;
    use crate::graph_storage::WeightProperties;
    use crate::query::graph_ops::edges::{durable_vertex_store_key, edge_store_key, edge_to_value};
    use crate::storage::pagedb_storage::{PagedbStorageDefault, PagedbStorageMem};

    const COLLECTION: &str = "weighted";
    const EDGE_LABEL: &str = "LINK";

    fn graph_files(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let vertices = dir.join("tiny.v");
        let edges = dir.join("tiny.e");
        fs::write(&vertices, "a\nb\nc\n").unwrap();
        fs::write(&edges, "a b 1\na b 2\n").unwrap();
        (vertices, edges)
    }

    fn spec() -> WeightedEdgeListSpec {
        WeightedEdgeListSpec::new(COLLECTION, EDGE_LABEL)
    }

    fn expected_edge_value() -> Value {
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.0)).unwrap();
        let encoded = edge_to_value(COLLECTION, "a", EDGE_LABEL, "b", &properties).unwrap();
        zerompk::from_msgpack(&encoded).unwrap()
    }

    fn decode_stored(value: Option<Vec<u8>>) -> Option<Value> {
        value.map(|encoded| zerompk::from_msgpack(&encoded).unwrap())
    }

    fn test_config() -> LiteConfig {
        LiteConfig {
            auto_flush_ms: 0,
            ..LiteConfig::default()
        }
    }

    fn assert_prepared_csr(db: &NodeDbLite<impl StorageEngine>) {
        let map = db.csr.lock().unwrap();
        let csr = map.get(COLLECTION).unwrap();
        assert!(csr.node_id_raw("c").is_some(), "isolated vertex was lost");
        assert_eq!(csr.edge_weight("a", EDGE_LABEL, "b"), Some(2.0));
    }

    #[tokio::test]
    async fn opened_store_import_uses_bounded_normal_batches() {
        let dir = tempfile::tempdir().unwrap();
        let (vertices, edges) = graph_files(dir.path());
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();
        let metrics = db
            .import_weighted_edge_list(&vertices, &edges, &spec())
            .await
            .unwrap();
        assert_eq!((metrics.vertices, metrics.edges), (3, 2));
        assert_eq!(db.storage.count(Namespace::Graph).await.unwrap(), 2);
        assert_prepared_csr(&db);
        assert_eq!(
            decode_stored(
                db.storage
                    .get(
                        Namespace::Graph,
                        &edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"),
                    )
                    .await
                    .unwrap(),
            ),
            Some(expected_edge_value()),
        );
    }

    #[tokio::test]
    async fn preopen_bulk_import_is_atomic_and_reopens_with_isolates() {
        let dir = tempfile::tempdir().unwrap();
        let (vertices, edges) = graph_files(dir.path());
        let path = dir.path().join("graph.pagedb");
        let (db, _, measurements) =
            NodeDbLite::open_and_import_weighted_edge_list_measured_at_path(
                &path,
                Encryption::Plaintext,
                test_config(),
                16 * 1024,
                &vertices,
                &edges,
                &spec(),
            )
            .await
            .unwrap();
        assert_eq!(measurements.storage.operations, 2);
        assert_eq!(db.storage.count(Namespace::Graph).await.unwrap(), 2);
        assert_prepared_csr(&db);
        assert!(
            db.storage
                .get(Namespace::Meta, b"meta:lite_id")
                .await
                .unwrap()
                .is_some(),
        );
        assert_eq!(
            decode_stored(
                db.storage
                    .get(
                        Namespace::Graph,
                        &edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"),
                    )
                    .await
                    .unwrap(),
            ),
            Some(expected_edge_value()),
        );
        assert_eq!(
            db.storage
                .get(Namespace::Graph, &durable_vertex_store_key(COLLECTION, "c"),)
                .await
                .unwrap(),
            Some(Vec::new()),
        );
        drop(db);
        let reopened = NodeDbLite::open_at_path_with_config_and_page_size(
            &path,
            Encryption::Plaintext,
            test_config(),
            16 * 1024,
        )
        .await
        .unwrap();
        assert_prepared_csr(&reopened);
        assert_eq!(
            decode_stored(
                reopened
                    .storage
                    .get(
                        Namespace::Graph,
                        &edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"),
                    )
                    .await
                    .unwrap(),
            ),
            Some(expected_edge_value()),
        );
    }

    #[tokio::test]
    async fn preopen_parse_error_publishes_neither_graph_nor_identity() {
        let dir = tempfile::tempdir().unwrap();
        let vertices = dir.path().join("bad.v");
        let edges = dir.path().join("bad.e");
        let path = dir.path().join("bad.pagedb");
        fs::write(&vertices, "a\nb\n").unwrap();
        fs::write(&edges, "a b 1\na b not-a-weight\n").unwrap();

        let error = match NodeDbLite::open_and_import_weighted_edge_list_at_path(
            &path,
            Encryption::Plaintext,
            LiteConfig::default(),
            16 * 1024,
            &vertices,
            &edges,
            &spec(),
        )
        .await
        {
            Ok(_) => panic!("malformed input unexpectedly imported"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("malformed weighted edge-list record")
        );
        let storage = PagedbStorageDefault::open_with_policy_and_page_size(
            &path,
            Encryption::Plaintext,
            LiteConfig::default().corruption_policy,
            16 * 1024,
        )
        .await
        .unwrap();
        assert_eq!(storage.count(Namespace::Graph).await.unwrap(), 0);
        assert_eq!(storage.count(Namespace::Meta).await.unwrap(), 0);
    }
}
