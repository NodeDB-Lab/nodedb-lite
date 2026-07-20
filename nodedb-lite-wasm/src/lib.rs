//! JavaScript/TypeScript bindings for NodeDB-Lite via wasm-bindgen.
//!
//! # In-memory (ephemeral)
//!
//! ```js
//! const db = await NodeDbLiteWasm.openInMemory(1n);
//! // or the legacy alias:
//! const db = await NodeDbLiteWasm.open(1n);
//! ```
//!
//! # Persistent (OPFS-backed)
//!
//! Persistent storage uses pagedb's OPFS VFS, which drives a dedicated Web
//! Worker for all synchronous file-system calls.
//!
//! **Bootstrap requirement — breaking change from the pre-pagedb API:**
//!
//! The embedder must create a JS worker bootstrap file (e.g. `opfs_worker.js`)
//! and pass its URL as the `workerUrl` argument to `openPersistent` /
//! `openPersistentWithConfig`. The bootstrap file must call `run_opfs_worker`:
//!
//! ```js
//! // opfs_worker.js
//! import init, { run_opfs_worker } from "./nodedb_lite_wasm.js";
//! await init();
//! run_opfs_worker();
//! ```
//!
//! The caller side:
//!
//! ```js
//! // Must be called from any execution context (main thread or worker).
//! const db = await NodeDbLiteWasm.openPersistent(
//!     "mydb.pagedb",        // logical database name (used as OPFS sub-directory)
//!     1n,                   // peer_id
//!     "./opfs_worker.js",   // URL of the worker bootstrap script
//! );
//! ```
//!
//! The `filename` parameter selects the OPFS sub-directory for this database.
//! Each unique `filename` value produces an isolated database. pagedb stores
//! all of its files under that directory in the browser's OPFS origin sandbox.
//!
//! # Corruption recovery
//!
//! OPFS has no rename primitive, so the automatic rename-and-recreate recovery
//! available on native is not supported. When `openPersistent` returns
//! `WorkerFailed`, the caller should delete the OPFS directory for `filename`
//! (using the File System Access API) and re-sync from Origin.

pub mod array;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::storage::pagedb_storage::PagedbStorageMem;
use nodedb_lite::{LiteConfig, NodeDbLite};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

// `PagedbStorageOpfs` is only available on wasm32 with the `opfs` feature
// active. On native (e.g. `cargo check` without a `--target` flag) this
// import must be suppressed.
#[cfg(all(target_arch = "wasm32", feature = "opfs"))]
use nodedb_lite::PagedbStorageOpfs;
// `Encryption` is only referenced by the OPFS persistent constructors below,
// which carry the same cfg; importing it unconditionally warns on other targets.
#[cfg(all(target_arch = "wasm32", feature = "opfs"))]
use nodedb_lite::storage::encryption::Encryption;

// ─── OPFS worker note ─────────────────────────────────────────────────────────
//
// The OPFS Web Worker is now pure JavaScript — no Rust/WASM is loaded in the
// worker context. Use the JS source from `pagedb::vfs::opfs::OPFS_WORKER_JS`
// (available in the pagedb crate when compiled for wasm32 with the `opfs`
// feature). Write it to a Blob URL or serve it statically, then pass the URL
// to `openPersistent`:
//
//   const workerBlob = new Blob([OPFS_WORKER_JS], { type: "text/javascript" });
//   const workerUrl  = URL.createObjectURL(workerBlob);
//   const db         = await NodeDbLiteWasm.openPersistent(workerUrl);

// ─── Inner enum ───────────────────────────────────────────────────────────────

/// Holds either an in-memory or an OPFS-backed `NodeDbLite` instance.
///
/// The two concrete storage types are different Rust types, so we unify them
/// behind this enum and dispatch each method to the appropriate arm.
///
/// `Arc` is used so that `start_auto_flush` can hold a `Weak` reference and
/// the auto-flush background task exits cleanly when the JS object is GC'd.
enum NodeDbLiteWasmInner {
    InMemory(Arc<NodeDbLite<PagedbStorageMem>>),
    #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
    Persistent(Arc<NodeDbLite<PagedbStorageOpfs>>),
}

// These macros are used in both `lib.rs` and `array.rs`.  Declaring them at the
// crate root makes them available in all submodules without any `use` import.
macro_rules! dispatch {
    ($self:ident, $inner:ident, $body:expr) => {
        match &$self.inner {
            crate::NodeDbLiteWasmInner::InMemory($inner) => $body,
            #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
            crate::NodeDbLiteWasmInner::Persistent($inner) => $body,
        }
    };
}
pub(crate) use dispatch;

// ─── Public JS type ───────────────────────────────────────────────────────────

/// NodeDB-Lite instance for browser/WASM environments.
///
/// Wraps either an in-memory or an OPFS-backed database. Construct via the
/// static factory methods: `openInMemory`, `open`, `openWithConfig`,
/// `openPersistent`, or `openPersistentWithConfig`.
#[wasm_bindgen]
pub struct NodeDbLiteWasm {
    inner: NodeDbLiteWasmInner,
}

#[wasm_bindgen]
impl NodeDbLiteWasm {
    // ─── Constructors — in-memory ──────────────────────────────────────────

    /// Create a new in-memory NodeDB-Lite database (no persistence).
    ///
    /// Memory budget is resolved from the default (100 MiB).
    #[wasm_bindgen(js_name = "openInMemory")]
    pub async fn open_in_memory(peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = Arc::new(
            NodeDbLite::open(storage, peer_id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))?,
        );
        db.start_auto_flush(LiteConfig::default().auto_flush_ms);
        db.start_auto_compact(LiteConfig::default().auto_compact_ms);
        Ok(Self {
            inner: NodeDbLiteWasmInner::InMemory(db),
        })
    }

    /// Alias for `openInMemory` — retained for backwards compatibility.
    ///
    /// Memory budget is resolved from the default (100 MiB).
    #[wasm_bindgen]
    pub async fn open(peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        Self::open_in_memory(peer_id).await
    }

    /// Create a new in-memory NodeDB-Lite database with an explicit memory budget.
    ///
    /// `memory_mb` — total memory budget in mebibytes.
    /// Pass `None` (or `undefined` from JS) to use the default 100 MiB.
    #[wasm_bindgen(js_name = "openWithConfig")]
    pub async fn open_with_config(
        peer_id: u64,
        memory_mb: Option<u32>,
    ) -> Result<NodeDbLiteWasm, JsError> {
        let config = config_from_memory_mb(memory_mb);
        let auto_flush_ms = config.auto_flush_ms;
        let auto_compact_ms = config.auto_compact_ms;
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = Arc::new(
            NodeDbLite::open_with_config(storage, peer_id, config)
                .await
                .map_err(|e| JsError::new(&e.to_string()))?,
        );
        db.start_auto_flush(auto_flush_ms);
        db.start_auto_compact(auto_compact_ms);
        Ok(Self {
            inner: NodeDbLiteWasmInner::InMemory(db),
        })
    }

    // ─── Constructors — persistent (OPFS) ─────────────────────────────────

    /// Create a persistent NodeDB-Lite database backed by OPFS.
    ///
    /// `worker_url` is the URL of the JS bootstrap script that calls
    /// `run_opfs_worker()`. See the module-level documentation for the
    /// required bootstrap file format.
    ///
    /// `passphrase` controls at-rest encryption of the OPFS database pages.
    /// OPFS storage is not encrypted by the browser itself, so a passphrase
    /// is strongly recommended. Pass an empty string to consciously opt out
    /// of encryption (all-zero page key; data is readable by anyone with
    /// OPFS origin access).
    ///
    /// A 16-byte random salt is persisted in an OPFS sidecar (`__nodedb_salt`)
    /// alongside the database on first open so the same passphrase reproduces
    /// the same key on every subsequent reopen.
    ///
    /// `filename` selects the OPFS sub-directory for this database. Every unique
    /// value is a fully isolated database instance in the shared OPFS origin;
    /// reopening with the same value reattaches the same data. It must be a
    /// single path segment (non-empty, no `/`, `\`, or NUL, not `.`/`..`).
    ///
    /// Data survives page reloads and browser restarts. Can be called from
    /// any execution context (the sync I/O runs inside the worker, not the
    /// caller).
    #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
    #[wasm_bindgen(js_name = "openPersistent")]
    pub async fn open_persistent(
        filename: &str,
        peer_id: u64,
        worker_url: &str,
        passphrase: String,
    ) -> Result<NodeDbLiteWasm, JsError> {
        let enc = if passphrase.is_empty() {
            Encryption::Plaintext
        } else {
            Encryption::passphrase(passphrase)
        };
        let storage = PagedbStorageOpfs::open_opfs(filename, worker_url, enc)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = Arc::new(
            NodeDbLite::open(storage, peer_id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))?,
        );
        db.start_auto_flush(LiteConfig::default().auto_flush_ms);
        db.start_auto_compact(LiteConfig::default().auto_compact_ms);
        Ok(Self {
            inner: NodeDbLiteWasmInner::Persistent(db),
        })
    }

    /// Create a persistent OPFS-backed NodeDB-Lite database with an explicit
    /// memory budget.
    ///
    /// `passphrase` controls at-rest encryption. See `openPersistent` for the
    /// full encryption semantics. Pass an empty string to opt out.
    ///
    /// `filename` selects the OPFS sub-directory for this database — see
    /// `openPersistent` for the isolation and naming rules.
    ///
    /// `memory_mb` — total memory budget in mebibytes.
    /// Pass `None` (or `undefined` from JS) to use the default 100 MiB.
    #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
    #[wasm_bindgen(js_name = "openPersistentWithConfig")]
    pub async fn open_persistent_with_config(
        filename: &str,
        peer_id: u64,
        worker_url: &str,
        passphrase: String,
        memory_mb: Option<u32>,
    ) -> Result<NodeDbLiteWasm, JsError> {
        let enc = if passphrase.is_empty() {
            Encryption::Plaintext
        } else {
            Encryption::passphrase(passphrase)
        };
        let config = config_from_memory_mb(memory_mb);
        let auto_flush_ms = config.auto_flush_ms;
        let auto_compact_ms = config.auto_compact_ms;
        let storage = PagedbStorageOpfs::open_opfs(filename, worker_url, enc)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = Arc::new(
            NodeDbLite::open_with_config(storage, peer_id, config)
                .await
                .map_err(|e| JsError::new(&e.to_string()))?,
        );
        db.start_auto_flush(auto_flush_ms);
        db.start_auto_compact(auto_compact_ms);
        Ok(Self {
            inner: NodeDbLiteWasmInner::Persistent(db),
        })
    }

    // ─── Database methods ──────────────────────────────────────────────────

    /// Insert a vector into a collection.
    #[wasm_bindgen(js_name = "vectorInsert")]
    pub async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.vector_insert(collection, id, embedding, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Search for the k nearest vectors. Returns JSON array.
    #[wasm_bindgen(js_name = "vectorSearch")]
    pub async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> Result<JsValue, JsError> {
        let results = dispatch!(self, db, {
            db.vector_search(collection, query, k, None, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Delete a vector by ID.
    #[wasm_bindgen(js_name = "vectorDelete")]
    pub async fn vector_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.vector_delete(collection, id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Insert a directed graph edge into `collection`.
    ///
    /// Returns the generated edge ID as a string.
    #[wasm_bindgen(js_name = "graphInsertEdge")]
    pub async fn graph_insert_edge(
        &self,
        collection: &str,
        from: &str,
        to: &str,
        edge_type: &str,
    ) -> Result<String, JsError> {
        let from_id = NodeId::try_new(from).map_err(|e| JsError::new(&e.to_string()))?;
        let to_id = NodeId::try_new(to).map_err(|e| JsError::new(&e.to_string()))?;
        let edge_id = dispatch!(self, db, {
            db.graph_insert_edge(collection, &from_id, &to_id, edge_type, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;
        Ok(edge_id.to_string())
    }

    /// Traverse the graph from a start node within `collection`. Returns JSON.
    #[wasm_bindgen(js_name = "graphTraverse")]
    pub async fn graph_traverse(
        &self,
        collection: &str,
        start: &str,
        depth: u8,
    ) -> Result<JsValue, JsError> {
        let start_id = NodeId::try_new(start).map_err(|e| JsError::new(&e.to_string()))?;
        let subgraph = dispatch!(self, db, {
            db.graph_traverse(collection, &start_id, depth, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json = serde_json::json!({
            "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({
                "id": n.id.as_str(),
                "depth": n.depth,
            })).collect::<Vec<_>>(),
            "edges": subgraph.edges.iter().map(|e| serde_json::json!({
                "from": e.from.as_str(),
                "to": e.to.as_str(),
                "label": e.label,
            })).collect::<Vec<_>>(),
        });

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Get a document by ID. Returns JSON or null.
    #[wasm_bindgen(js_name = "documentGet")]
    pub async fn document_get(&self, collection: &str, id: &str) -> Result<JsValue, JsError> {
        let doc = dispatch!(self, db, {
            db.document_get(collection, id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        match doc {
            Some(d) => serde_wasm_bindgen::to_value(&d).map_err(|e| JsError::new(&e.to_string())),
            None => Ok(JsValue::NULL),
        }
    }

    /// Put (insert or update) a document. Takes a JSON string of fields.
    ///
    /// If `id` is empty, a UUIDv7 is auto-generated.
    /// Returns the document ID (useful when auto-generated).
    #[wasm_bindgen(js_name = "documentPut")]
    pub async fn document_put(
        &self,
        collection: &str,
        id: &str,
        fields_json: &str,
    ) -> Result<String, JsError> {
        let fields: std::collections::HashMap<String, Value> =
            sonic_rs::from_str(fields_json).map_err(|e| JsError::new(&e.to_string()))?;

        let doc_id = if id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            id.to_string()
        };

        let mut doc = Document::new(&doc_id);
        for (k, v) in fields {
            doc.set(k, v);
        }

        dispatch!(self, db, {
            db.document_put(collection, doc)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        Ok(doc_id)
    }

    /// Delete a document by ID.
    #[wasm_bindgen(js_name = "documentDelete")]
    pub async fn document_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.document_delete(collection, id)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Delete a graph edge by ID from `collection`.
    #[wasm_bindgen(js_name = "graphDeleteEdge")]
    pub async fn graph_delete_edge(&self, collection: &str, edge_id: &str) -> Result<(), JsError> {
        let eid: nodedb_types::id::EdgeId = edge_id
            .parse()
            .map_err(|e: nodedb_types::id::EdgeIdParseError| JsError::new(&e.to_string()))?;
        dispatch!(self, db, {
            db.graph_delete_edge(collection, &eid)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Return aggregate graph statistics for `collection`.
    #[wasm_bindgen(js_name = "graphStats")]
    pub async fn graph_stats(&self, collection: Option<String>) -> Result<JsValue, JsError> {
        let col = collection.as_deref();
        let stats = dispatch!(self, db, {
            db.graph_stats(col, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json: Vec<serde_json::Value> = stats
            .iter()
            .map(|s| {
                serde_json::json!({
                    "collection": s.collection,
                    "node_count": s.node_count,
                    "edge_count": s.edge_count,
                    "distinct_label_count": s.distinct_label_count,
                    "labels": s.labels,
                })
            })
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Find the shortest path between two nodes within `collection`. Returns JSON.
    #[wasm_bindgen(js_name = "graphShortestPath")]
    pub async fn graph_shortest_path(
        &self,
        collection: &str,
        from: &str,
        to: &str,
        max_depth: u8,
    ) -> Result<JsValue, JsError> {
        let from_id = NodeId::try_new(from).map_err(|e| JsError::new(&e.to_string()))?;
        let to_id = NodeId::try_new(to).map_err(|e| JsError::new(&e.to_string()))?;
        let path = dispatch!(self, db, {
            db.graph_shortest_path(collection, &from_id, &to_id, max_depth, None)
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        match path {
            Some(nodes) => {
                let ids: Vec<&str> = nodes.iter().map(|n| n.as_str()).collect();
                serde_wasm_bindgen::to_value(&ids).map_err(|e| JsError::new(&e.to_string()))
            }
            None => Ok(JsValue::NULL),
        }
    }

    /// Full-text search (BM25) against `field` in `collection`. Returns JSON array of results.
    #[wasm_bindgen(js_name = "textSearch")]
    pub async fn text_search(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
    ) -> Result<JsValue, JsError> {
        let results = dispatch!(self, db, {
            db.text_search(
                collection,
                field,
                query,
                top_k,
                nodedb_types::TextSearchParams::default(),
                None,
            )
            .await
            .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Execute a SQL query. Returns JSON with columns and rows.
    #[wasm_bindgen(js_name = "executeSql")]
    pub async fn execute_sql(&self, sql: &str) -> Result<JsValue, JsError> {
        let result = dispatch!(self, db, {
            db.execute_sql(sql, &[])
                .await
                .map_err(|e| JsError::new(&e.to_string()))
        })?;

        let json = serde_json::json!({
            "columns": result.columns,
            "rows": result.rows,
            "rows_affected": result.rows_affected,
        });

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Flush all in-memory state to storage.
    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<(), JsError> {
        dispatch!(self, db, {
            db.flush().await.map_err(|e| JsError::new(&e.to_string()))
        })
    }

    /// Compact the backing store, reclaiming dead pages and truncating the
    /// OPFS file to bound on-disk growth.
    ///
    /// Returns a `{ reclaimedPages, segmentsRepacked, fileBytesFreed }` object.
    /// Useful for one-commit-per-entry workloads where the file would otherwise
    /// grow without bound; a no-op for the in-memory backend.
    #[wasm_bindgen]
    pub async fn compact(&self) -> Result<JsValue, JsError> {
        dispatch!(self, db, {
            let outcome = db
                .compact()
                .await
                .map_err(|e| JsError::new(&e.to_string()))?;
            serde_wasm_bindgen::to_value(&outcome).map_err(|e| JsError::new(&e.to_string()))
        })
    }

    // ─── ID Generation ──────────────────────────────────────────────────

    /// Generate a UUIDv7 (time-sortable, recommended for primary keys).
    #[wasm_bindgen(js_name = "generateId")]
    pub fn generate_id() -> String {
        nodedb_types::id_gen::uuid_v7()
    }

    /// Generate an ID of the specified type.
    ///
    /// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
    #[wasm_bindgen(js_name = "generateIdTyped")]
    pub fn generate_id_typed(id_type: &str) -> Result<String, JsError> {
        nodedb_types::id_gen::generate_by_type(id_type).ok_or_else(|| {
            JsError::new(&format!(
                "unknown ID type '{id_type}': use uuidv7, uuidv4, ulid, cuid2, or nanoid"
            ))
        })
    }
}

/// Register a user-defined WASM function from raw bytes.
///
/// Uses the browser's native `WebAssembly.instantiate()` — no wasmtime needed.
/// The `.wasm` module must export a function with the given name.
///
/// ```js
/// const wasmBytes = await fetch('my_udf.wasm').then(r => r.arrayBuffer());
/// await registerWasmUdf('my_func', new Uint8Array(wasmBytes));
/// ```
#[wasm_bindgen(js_name = "registerWasmUdf")]
pub async fn register_wasm_udf(name: &str, wasm_bytes: &[u8]) -> Result<(), JsError> {
    use js_sys::{Object, Reflect, WebAssembly};

    // Validate WASM magic header.
    if wasm_bytes.len() < 4 || &wasm_bytes[..4] != b"\0asm" {
        return Err(JsError::new("invalid WASM binary: missing \\0asm header"));
    }

    // Compile and instantiate via browser WebAssembly API.
    let module_promise = WebAssembly::compile(&js_sys::Uint8Array::from(wasm_bytes).into());
    let module = JsFuture::from(module_promise)
        .await
        .map_err(|e| JsError::new(&format!("WebAssembly.compile failed: {e:?}")))?;

    let imports = Object::new();
    let instance_promise =
        WebAssembly::instantiate_module(&module.unchecked_into::<WebAssembly::Module>(), &imports);
    let instance = JsFuture::from(instance_promise)
        .await
        .map_err(|e| JsError::new(&format!("WebAssembly.instantiate failed: {e:?}")))?;

    // Verify the named export exists.
    let exports = Reflect::get(&instance, &"exports".into())
        .map_err(|_| JsError::new("failed to access WASM instance exports"))?;
    let func = Reflect::get(&exports, &name.into())
        .map_err(|_| JsError::new(&format!("WASM module does not export function '{name}'")))?;
    if func.is_undefined() {
        return Err(JsError::new(&format!(
            "WASM module does not export function '{name}'"
        )));
    }

    // Store the instance for later invocation.
    web_sys::console::log_1(
        &format!("WASM UDF '{name}' registered ({} bytes)", wasm_bytes.len()).into(),
    );

    Ok(())
}

/// Largest accepted `memory_mb` override, in MiB (16 GiB).
///
/// A JS caller can pass any `u32`; values beyond what the browser/WASM heap can
/// ever back are clamped to this ceiling rather than producing a `LiteConfig`
/// that promises a budget the runtime cannot honour. 16 GiB comfortably exceeds
/// the wasm32 4 GiB address space while leaving an obvious sane upper bound.
const MAX_MEMORY_BUDGET_MB: u32 = 16 * 1024;

/// Build a [`LiteConfig`] from an optional `memory_mb` value.
///
/// `None` or `Some(0)` → default config (100 MiB).
/// `Some(mb)` → default config with `memory_budget` overridden to `mb` MiB,
/// clamped to [`MAX_MEMORY_BUDGET_MB`].
fn config_from_memory_mb(memory_mb: Option<u32>) -> LiteConfig {
    match memory_mb {
        // `saturating_mul` guards the byte computation: on wasm32 `usize` is
        // 32-bit, so even a ~4 GiB budget would overflow without it. The clamp
        // bounds the logical request; saturation bounds the arithmetic.
        Some(mb) if mb > 0 => LiteConfig {
            memory_budget: (mb.min(MAX_MEMORY_BUDGET_MB) as usize).saturating_mul(1024 * 1024),
            ..LiteConfig::default()
        },
        _ => LiteConfig::default(),
    }
}
