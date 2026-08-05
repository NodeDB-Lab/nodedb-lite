// SPDX-License-Identifier: Apache-2.0

//! Feature-gated embedded runner support for LDBC Graphalytics.

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
use crate::graphalytics_diagnostics::GraphalyticsLoadDiagnostics;
use crate::graphalytics_import::{self, COLLECTION};
use crate::query::graph_ops::algorithms::{
    materialize_graphalytics_raw, run_algo, run_graphalytics_raw,
    run_graphalytics_raw_prevalidated_sssp, validate_graphalytics_sssp_weights,
};
use crate::query::graph_ops::graphalytics_results::GraphalyticsRawValues;
use crate::storage::encryption::Encryption;
use crate::storage::engine::StorageEngine;
use crate::storage::pagedb_storage::PagedbStorageDefault;

/// Opaque dense primitive output passed from timed computation to untimed materialization.
#[doc(hidden)]
pub struct GraphalyticsRawResult(GraphalyticsRawValues);

/// Proof that the current Graphalytics CSR passed dataset-wide SSSP weight validation.
#[doc(hidden)]
pub struct GraphalyticsValidatedSssp(());

#[derive(Debug)]
pub struct ImportMetrics {
    pub vertices: usize,
    pub edges: usize,
    pub load_seconds: f64,
    pub prepare_seconds: f64,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Import a weighted Graphalytics edge-list into an already-open normal
    /// durable graph table, using bounded ordinary storage batches.
    pub async fn graphalytics_import(
        &self,
        vertex_file: &Path,
        edge_file: &Path,
    ) -> Result<ImportMetrics, LiteError> {
        graphalytics_import::import(
            &*self.storage,
            &self.csr,
            vertex_file,
            edge_file,
            false,
            None,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn graphalytics_import_with_diagnostics(
        &self,
        vertex_file: &Path,
        edge_file: &Path,
        enabled: bool,
    ) -> Result<(ImportMetrics, Option<GraphalyticsLoadDiagnostics>), LiteError> {
        let mut diagnostics = enabled.then(GraphalyticsLoadDiagnostics::new);
        let metrics = graphalytics_import::import(
            &*self.storage,
            &self.csr,
            vertex_file,
            edge_file,
            false,
            diagnostics.as_mut(),
        )
        .await?;
        Ok((metrics, diagnostics))
    }

    /// Run one Graphalytics algorithm, returning the full in-memory result.
    pub fn graphalytics_run(
        &self,
        algorithm: GraphAlgorithm,
        source: &str,
    ) -> Result<QueryResult, LiteError> {
        if matches!(
            algorithm,
            GraphAlgorithm::PageRank
                | GraphAlgorithm::Wcc
                | GraphAlgorithm::Lcc
                | GraphAlgorithm::Sssp
                | GraphAlgorithm::LabelPropagation
        ) {
            let raw = self.graphalytics_raw_run(algorithm, source)?;
            return self.graphalytics_raw_result(algorithm, raw);
        }
        if algorithm == GraphAlgorithm::Diameter {
            return Err(LiteError::Storage {
                detail: "diameter is not part of the Graphalytics runner".to_string(),
            });
        }
        run_algo(&self.csr, algorithm, &graphalytics_params(source))
    }

    #[doc(hidden)]
    pub fn graphalytics_validate_sssp_weights(
        &self,
    ) -> Result<GraphalyticsValidatedSssp, LiteError> {
        validate_graphalytics_sssp_weights(&self.csr, COLLECTION)?;
        Ok(GraphalyticsValidatedSssp(()))
    }

    #[doc(hidden)]
    pub fn graphalytics_raw_run(
        &self,
        algorithm: GraphAlgorithm,
        source: &str,
    ) -> Result<GraphalyticsRawResult, LiteError> {
        if algorithm == GraphAlgorithm::Diameter {
            return Err(LiteError::Storage {
                detail: "diameter is not part of the Graphalytics runner".to_string(),
            });
        }
        Ok(GraphalyticsRawResult(run_graphalytics_raw(
            &self.csr,
            algorithm,
            &graphalytics_params(source),
        )?))
    }

    #[doc(hidden)]
    pub fn graphalytics_sssp_raw_prevalidated(
        &self,
        source: &str,
        _validated: GraphalyticsValidatedSssp,
    ) -> Result<GraphalyticsRawResult, LiteError> {
        Ok(GraphalyticsRawResult(
            run_graphalytics_raw_prevalidated_sssp(
                &self.csr,
                GraphAlgorithm::Sssp,
                &graphalytics_params(source),
            )?,
        ))
    }

    #[doc(hidden)]
    pub fn graphalytics_raw_result(
        &self,
        algorithm: GraphAlgorithm,
        raw: GraphalyticsRawResult,
    ) -> Result<QueryResult, LiteError> {
        let GraphalyticsRawResult(raw) = raw;
        let mut result = materialize_graphalytics_raw(&self.csr, COLLECTION, algorithm, raw)?;
        if algorithm == GraphAlgorithm::LabelPropagation {
            let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
                detail: "graphalytics CSR is not loaded".to_string(),
            })?;
            for row in &mut result.rows {
                if let Some(Value::Integer(label)) = row.get(1) {
                    let label = u32::try_from(*label).map_err(|_| LiteError::Storage {
                        detail: format!("invalid label propagation result {label}"),
                    })?;
                    row[1] = Value::String(csr.node_name_raw(label).to_string());
                }
            }
        }
        Ok(result)
    }

    pub fn graphalytics_bfs_distances(&self, source: &str) -> Result<Vec<i64>, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
            detail: "graphalytics CSR is not loaded".to_string(),
        })?;
        let source_id = csr.node_id_raw(source).ok_or_else(|| LiteError::Storage {
            detail: format!("source vertex '{source}' is absent"),
        })?;
        Ok(csr.bfs_both_distances_raw(source_id))
    }

    pub fn graphalytics_bfs_result(&self, distance: Vec<i64>) -> Result<QueryResult, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
            detail: "graphalytics CSR is not loaded".to_string(),
        })?;
        if distance.len() != csr.node_count() {
            return Err(LiteError::Storage {
                detail: "Graphalytics BFS distance count does not match the CSR".to_string(),
            });
        }
        Ok(QueryResult {
            columns: vec!["node_id".to_string(), "distance".to_string()],
            rows: distance
                .into_iter()
                .enumerate()
                .map(|(node, value)| {
                    vec![
                        Value::String(csr.node_name_raw(node as u32).to_string()),
                        Value::Integer(value),
                    ]
                })
                .collect(),
            rows_affected: 0,
        })
    }

    pub fn graphalytics_bfs(&self, source: &str) -> Result<QueryResult, LiteError> {
        self.graphalytics_bfs_result(self.graphalytics_bfs_distances(source)?)
    }
}

impl NodeDbLite<PagedbStorageDefault> {
    /// Create an empty native PageDB, bulk-import the Graphalytics edge table,
    /// then initialize the normal Lite runtime around that durable store.
    ///
    /// Configuration is validated before publication. If later runtime
    /// initialization fails, the committed graph remains a valid PageDB and
    /// can be recovered through the normal path-based opener, which rebuilds
    /// CSR state from the durable Graph namespace.
    pub async fn graphalytics_open_and_import_at_path(
        path: &Path,
        encryption: Encryption,
        config: LiteConfig,
        page_size: usize,
        vertex_file: &Path,
        edge_file: &Path,
        diagnostics_enabled: bool,
    ) -> NodeDbResult<(
        Arc<Self>,
        ImportMetrics,
        Option<GraphalyticsLoadDiagnostics>,
    )> {
        // Validate before durable construction: an invalid configuration must
        // not leave a published graph that cannot be initialized by Lite.
        config.validate()?;
        let storage = PagedbStorageDefault::open_with_policy_and_page_size(
            path,
            encryption,
            config.corruption_policy,
            page_size,
        )
        .await?;
        let local_csr = Arc::new(Mutex::new(HashMap::<String, CsrIndex>::new()));
        let mut diagnostics = diagnostics_enabled.then(GraphalyticsLoadDiagnostics::new);
        let metrics = graphalytics_import::import(
            &storage,
            &local_csr,
            vertex_file,
            edge_file,
            true,
            diagnostics.as_mut(),
        )
        .await?;
        let prepared_csr = {
            let mut local = local_csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            std::mem::take(&mut *local)
        };
        let db = Self::open_with_config_and_csr(storage, config, prepared_csr).await?;
        Ok((db, metrics, diagnostics))
    }
}

fn graphalytics_params(source: &str) -> AlgoParams {
    AlgoParams {
        collection: COLLECTION.to_string(),
        damping: Some(0.85),
        max_iterations: Some(10),
        tolerance: Some(f64::MIN_POSITIVE),
        source_node: Some(source.to_string()),
        direction: Some("both".to_string()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nodedb_types::Namespace;

    use super::*;
    use crate::graphalytics_storage::WeightProperties;
    use crate::query::graph_ops::edges::{durable_vertex_store_key, edge_store_key, edge_to_value};
    use crate::storage::pagedb_storage::{PagedbStorageDefault, PagedbStorageMem};

    fn graph_files(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let vertices = dir.join("tiny.v");
        let edges = dir.join("tiny.e");
        fs::write(&vertices, "a\nb\nc\n").unwrap();
        fs::write(&edges, "a b 1\na b 2\n").unwrap();
        (vertices, edges)
    }

    fn expected_edge_value() -> Value {
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.0)).unwrap();
        let encoded = edge_to_value(COLLECTION, "a", "EDGE", "b", &properties).unwrap();
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
        assert_eq!(csr.edge_weight("a", "EDGE", "b"), Some(2.0));
    }

    #[tokio::test]
    async fn opened_store_import_uses_bounded_normal_batches() {
        let dir = tempfile::tempdir().unwrap();
        let (vertices, edges) = graph_files(dir.path());
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();
        let metrics = db.graphalytics_import(&vertices, &edges).await.unwrap();
        assert_eq!((metrics.vertices, metrics.edges), (3, 2));
        assert_eq!(db.storage.count(Namespace::Graph).await.unwrap(), 2);
        assert_prepared_csr(&db);
        assert_eq!(
            decode_stored(
                db.storage
                    .get(
                        Namespace::Graph,
                        &edge_store_key(COLLECTION, "a", "EDGE", "b"),
                    )
                    .await
                    .unwrap(),
            ),
            Some(expected_edge_value()),
        );
    }

    #[tokio::test]
    async fn preopen_bulk_import_persists_and_reports_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (vertices, edges) = graph_files(dir.path());
        let path = dir.path().join("graph.pagedb");
        let (db, _, diagnostics) = NodeDbLite::graphalytics_open_and_import_at_path(
            &path,
            Encryption::Plaintext,
            test_config(),
            16 * 1024,
            &vertices,
            &edges,
            true,
        )
        .await
        .unwrap();
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
                        &edge_store_key(COLLECTION, "a", "EDGE", "b"),
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
        let diagnostics = String::from_utf8(diagnostics.unwrap().to_json("tiny").unwrap()).unwrap();
        assert!(diagnostics.contains("\"storage_batch_commits\":1"));
        assert!(diagnostics.contains("\"storage_batch_operations\":2"));
        // Do not flush a CSR checkpoint: normal reopen must recover the
        // explicit isolated vertex from the atomically built Graph tree.
        drop(db);
        let reopened = NodeDbLite::open_at_path_with_config_and_page_size(
            &path,
            Encryption::Plaintext,
            test_config(),
            16 * 1024,
        )
        .await
        .unwrap();
        let edge_key = edge_store_key(COLLECTION, "a", "EDGE", "b");
        assert_eq!(
            reopened
                .storage
                .scan_prefix(Namespace::Graph, &edge_key)
                .await
                .unwrap()
                .len(),
            1,
        );
        assert_prepared_csr(&reopened);
        assert!(
            reopened
                .storage
                .get(Namespace::Meta, b"meta:lite_id")
                .await
                .unwrap()
                .is_some(),
        );
        assert_eq!(
            decode_stored(
                reopened
                    .storage
                    .get(
                        Namespace::Graph,
                        &edge_store_key(COLLECTION, "a", "EDGE", "b"),
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

        let error = match NodeDbLite::graphalytics_open_and_import_at_path(
            &path,
            Encryption::Plaintext,
            LiteConfig::default(),
            16 * 1024,
            &vertices,
            &edges,
            false,
        )
        .await
        {
            Ok(_) => panic!("malformed input unexpectedly imported"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("malformed Graphalytics edge"));

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
