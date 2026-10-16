// SPDX-License-Identifier: Apache-2.0

use nodedb_client::NodeDb;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

// ─── Persistence: Flush and Reopen ───────────────────────────────────

#[tokio::test]
async fn flush_and_reopen_persists_all() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persist.db");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();

        db.batch_vector_insert("vecs", &[("v1", &[1.0, 2.0, 3.0][..])])
            .await
            .unwrap();
        db.batch_graph_insert_edges(
            "vecs",
            &[(
                NodeId::from_validated("a".to_string()),
                NodeId::from_validated("b".to_string()),
                "KNOWS",
                None,
            )],
        )
        .await
        .unwrap();
        let mut doc = Document::new("d1");
        doc.set("key", Value::String("persistent".into()));
        db.document_put("docs", doc).await.unwrap();

        db.flush().await.unwrap();
        db.shutdown().await;
    }

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .unwrap();
        let db = NodeDbLite::open(storage).await.unwrap();

        let doc = db.document_get("docs", "d1").await.unwrap();
        assert!(doc.is_some(), "document should persist across restart");

        let results = db
            .vector_search("vecs", &[1.0, 2.0, 3.0], 1, None, None)
            .await
            .unwrap();
        assert!(!results.is_empty(), "vector should persist across restart");

        let sg = db
            .graph_traverse(
                "vecs",
                &NodeId::from_validated("a".to_string()),
                1,
                nodedb_types::graph::Direction::Out,
                None,
            )
            .await
            .unwrap();
        assert!(sg.node_count() >= 2, "graph should persist across restart");
    }
}
