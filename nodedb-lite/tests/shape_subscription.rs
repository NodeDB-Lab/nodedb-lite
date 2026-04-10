//! Shape subscription simulation tests — no Origin required.
//!
//! Tests the edge-side shape manager + sync client protocol logic
//! using in-process message construction and dispatch.

use std::sync::Arc;

use nodedb_lite::sync::*;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::*;

fn make_client() -> Arc<SyncClient> {
    Arc::new(SyncClient::new(
        SyncConfig::new("wss://localhost:6433/sync", "test.jwt"),
        1,
    ))
}

#[tokio::test]
async fn subscribe_shape_and_receive_snapshot() {
    let client = make_client();

    // Subscribe to a document shape.
    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "orders-shape".into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "all orders".into(),
            field_filter: vec![],
        });
    }

    // Verify subscription is tracked.
    {
        let shapes = client.shapes().lock().await;
        assert!(shapes.is_subscribed("orders-shape"));
        assert_eq!(shapes.count(), 1);
    }

    // Simulate receiving a snapshot from Origin.
    client
        .handle_shape_snapshot(&ShapeSnapshotMsg {
            shape_id: "orders-shape".into(),
            data: vec![1, 2, 3], // Simulated snapshot bytes.
            snapshot_lsn: 100,
            doc_count: 50,
        })
        .await;

    // Snapshot should be marked as loaded with correct LSN.
    let shapes = client.shapes().lock().await;
    let sub = shapes.get("orders-shape").unwrap();
    assert!(sub.snapshot_loaded);
    assert_eq!(sub.last_lsn, 100);
}

#[tokio::test]
async fn incremental_delta_advances_lsn() {
    let client = make_client();

    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Vector {
                collection: "embeddings".into(),
                field_name: None,
            },
            description: "all embeddings".into(),
            field_filter: vec![],
        });
        shapes.mark_snapshot_loaded("s1", 50);
    }

    // Simulate incremental deltas.
    client
        .handle_shape_delta(&ShapeDeltaMsg {
            shape_id: "s1".into(),
            collection: "embeddings".into(),
            document_id: "e1".into(),
            operation: "INSERT".into(),
            delta: vec![10, 20],
            lsn: 75,
        })
        .await;

    client
        .handle_shape_delta(&ShapeDeltaMsg {
            shape_id: "s1".into(),
            collection: "embeddings".into(),
            document_id: "e2".into(),
            operation: "INSERT".into(),
            delta: vec![30, 40],
            lsn: 80,
        })
        .await;

    let shapes = client.shapes().lock().await;
    assert_eq!(shapes.get("s1").unwrap().last_lsn, 80);
}

#[tokio::test]
async fn unsubscribe_removes_shape() {
    let client = make_client();

    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "temp".into(),
            tenant_id: 1,
            shape_type: ShapeType::Graph {
                root_nodes: vec!["alice".into()],
                max_depth: 2,
                edge_label: None,
            },
            description: "temp graph".into(),
            field_filter: vec![],
        });
    }

    assert_eq!(client.shapes().lock().await.count(), 1);

    {
        let mut shapes = client.shapes().lock().await;
        shapes.unsubscribe("temp");
    }

    assert_eq!(client.shapes().lock().await.count(), 0);
}

#[tokio::test]
async fn handshake_includes_active_shapes() {
    let client = make_client();

    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "orders".into(),
            field_filter: vec![],
        });
        shapes.subscribe(ShapeDefinition {
            shape_id: "s2".into(),
            tenant_id: 1,
            shape_type: ShapeType::Vector {
                collection: "vecs".into(),
                field_name: None,
            },
            description: "vecs".into(),
            field_filter: vec![],
        });
    }

    let handshake = client.build_handshake().await;
    assert_eq!(handshake.subscribed_shapes.len(), 2);
    assert!(handshake.subscribed_shapes.contains(&"s1".to_string()));
    assert!(handshake.subscribed_shapes.contains(&"s2".to_string()));
}

#[tokio::test]
async fn non_matching_shape_receives_nothing() {
    let client = make_client();

    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "orders-only".into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "orders only".into(),
            field_filter: vec![],
        });
    }

    // Delta for a different collection — shape should not advance.
    client
        .handle_shape_delta(&ShapeDeltaMsg {
            shape_id: "users-shape".into(), // Different shape.
            collection: "users".into(),
            document_id: "u1".into(),
            operation: "INSERT".into(),
            delta: vec![1, 2, 3],
            lsn: 999,
        })
        .await;

    let shapes = client.shapes().lock().await;
    let sub = shapes.get("orders-only").unwrap();
    assert_eq!(sub.last_lsn, 0, "non-matching shape should not advance LSN");
}
