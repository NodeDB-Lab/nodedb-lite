// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

use std::fs;

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_lite::analytics::{DenseAnalyticsValues, WeightedEdgeListSpec};
use nodedb_lite::{Encryption, LiteConfig, NodeDbLite, StorageEngine};
use nodedb_types::Namespace;

#[tokio::test]
async fn external_consumer_can_build_and_reopen_weighted_graph() {
    let directory = tempfile::tempdir().unwrap();
    let vertices = directory.path().join("vertices.txt");
    let edges = directory.path().join("edges.txt");
    let database = directory.path().join("weighted.pagedb");
    fs::write(&vertices, "one\ntwo\nisolated\n").unwrap();
    fs::write(&edges, "one two 3.5\n").unwrap();

    let spec = WeightedEdgeListSpec::new("research", "RELATES_TO");
    let (opened, metrics) = NodeDbLite::open_and_import_weighted_edge_list_at_path(
        &database,
        Encryption::Plaintext,
        LiteConfig::default(),
        16 * 1024,
        &vertices,
        &edges,
        &spec,
    )
    .await
    .unwrap();
    assert_eq!((metrics.vertices, metrics.edges), (3, 1));
    assert_eq!(opened.storage().count(Namespace::Graph).await.unwrap(), 2);
    let dense = opened
        .run_dense_analytics(
            GraphAlgorithm::Wcc,
            &AlgoParams {
                collection: "research".to_string(),
                ..AlgoParams::default()
            },
        )
        .unwrap();
    let DenseAnalyticsValues::Wcc(components) = dense.values() else {
        panic!("WCC returned the wrong dense value type");
    };
    assert_eq!(components.len(), 3);
    drop(opened);

    let reopened = NodeDbLite::open_at_path_with_config_and_page_size(
        &database,
        Encryption::Plaintext,
        LiteConfig::default(),
        16 * 1024,
    )
    .await
    .unwrap();
    assert_eq!(reopened.storage().count(Namespace::Graph).await.unwrap(), 2);
}
