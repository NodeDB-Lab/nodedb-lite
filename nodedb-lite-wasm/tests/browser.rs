//! Browser integration tests for NodeDB-Lite WASM.
//!
//! Run with: `wasm-pack test --headless --chrome nodedb-lite-wasm`

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use nodedb_lite_wasm::NodeDbLiteWasm;

#[wasm_bindgen_test]
async fn open_in_memory() {
    let db = NodeDbLiteWasm::open(1).await.unwrap();
    db.flush().await.unwrap();
}

#[wasm_bindgen_test]
async fn vector_insert_and_search() {
    let db = NodeDbLiteWasm::open(1).await.unwrap();

    db.vector_insert("vecs", "v1", &[1.0f32, 0.0, 0.0])
        .await
        .unwrap();
    db.vector_insert("vecs", "v2", &[0.0, 1.0, 0.0])
        .await
        .unwrap();

    let results = db.vector_search("vecs", &[1.0, 0.0, 0.0], 2).await.unwrap();

    // Results should be a JS array.
    assert!(results.is_array());
}

#[wasm_bindgen_test]
async fn graph_insert_and_traverse() {
    let db = NodeDbLiteWasm::open(2).await.unwrap();

    let edge_id = db.graph_insert_edge("alice", "bob", "KNOWS").await.unwrap();
    assert!(edge_id.contains("alice"));

    let subgraph = db.graph_traverse("alice", 2).await.unwrap();
    assert!(subgraph.is_object());
}

#[wasm_bindgen_test]
async fn document_crud() {
    let db = NodeDbLiteWasm::open(3).await.unwrap();

    // Put.
    db.document_put("notes", "n1", r#"{"title":{"String":"Hello"}}"#)
        .await
        .unwrap();

    // Get.
    let doc = db.document_get("notes", "n1").await.unwrap();
    assert!(!doc.is_null());

    // Delete.
    db.document_delete("notes", "n1").await.unwrap();
    let doc = db.document_get("notes", "n1").await.unwrap();
    assert!(doc.is_null());
}

#[wasm_bindgen_test]
async fn multi_modal() {
    let db = NodeDbLiteWasm::open(4).await.unwrap();

    // Vector + graph + document in one session.
    db.vector_insert("kb", "concept-ai", &[1.0, 0.0])
        .await
        .unwrap();
    db.graph_insert_edge("concept-ai", "concept-ml", "RELATES_TO")
        .await
        .unwrap();
    db.document_put("notes", "n1", r#"{"body":{"String":"AI is great"}}"#)
        .await
        .unwrap();

    let results = db.vector_search("kb", &[1.0, 0.0], 1).await.unwrap();
    assert!(results.is_array());

    let subgraph = db.graph_traverse("concept-ai", 1).await.unwrap();
    assert!(subgraph.is_object());

    let doc = db.document_get("notes", "n1").await.unwrap();
    assert!(!doc.is_null());
}
