//! Smoke tests for NodeDB-Lite WASM — runs in Node.js via `wasm-pack test --node`.
//!
//! These tests only cover the in-memory path (`PagedbStorage<MemVfs>`).
//! Persistent OPFS tests require a real browser with Web Worker support and
//! are covered by `tests/browser.rs`.

use wasm_bindgen_test::*;

use nodedb_lite_wasm::NodeDbLiteWasm;

#[wasm_bindgen_test]
async fn open_in_memory_smoke() {
    // open_in_memory is the canonical Rust name; the JS binding is openInMemory.
    let db = NodeDbLiteWasm::open_in_memory(1).await.unwrap();
    db.flush().await.unwrap();
}

#[wasm_bindgen_test]
async fn document_put_get_roundtrip() {
    let db = NodeDbLiteWasm::open_in_memory(2).await.unwrap();

    let id = db
        .document_put("col", "", r#"{"name":{"String":"Alice"}}"#)
        .await
        .unwrap();
    assert!(!id.is_empty());

    let doc = db.document_get("col", &id).await.unwrap();
    assert!(!doc.is_null());
}

#[wasm_bindgen_test]
async fn open_alias_still_works() {
    // `open` is the backward-compat alias for `open_in_memory`.
    let db = NodeDbLiteWasm::open(3).await.unwrap();
    db.flush().await.unwrap();
}
