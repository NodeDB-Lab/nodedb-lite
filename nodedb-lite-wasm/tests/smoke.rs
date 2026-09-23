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
    let db = NodeDbLiteWasm::open_in_memory().await.unwrap();
    db.flush().await.unwrap();
}

#[wasm_bindgen_test]
async fn document_put_get_roundtrip() {
    let db = NodeDbLiteWasm::open_in_memory().await.unwrap();

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
    let db = NodeDbLiteWasm::open().await.unwrap();
    db.flush().await.unwrap();
}

#[wasm_bindgen_test]
async fn statement_aliases_share_the_execute_path() {
    // `sql`, `exec`, and `query` are backward-compat names for `executeSql`.
    // The assertion is deliberately about the plumbing, not SQL semantics:
    // each alias must answer exactly as the canonical call does.
    let db = NodeDbLiteWasm::open_in_memory().await.unwrap();
    let statement = "SELECT 1";

    let canonical = db.execute_sql(statement).await;
    let aliases = [
        ("sql", db.sql(statement).await),
        ("exec", db.exec(statement).await),
        ("query", db.query(statement).await),
    ];
    for (name, result) in aliases {
        assert_eq!(
            result.is_ok(),
            canonical.is_ok(),
            "`{name}` diverged from executeSql"
        );
    }
}
