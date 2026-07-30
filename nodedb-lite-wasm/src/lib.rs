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
//! # Durability
//!
//! Every constructor starts the background flush task from the database's
//! configuration, so writes reach storage within `auto_flush_ms` (one second by
//! default). `flush()` forces it when a specific write must be durable before
//! the caller continues.
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
//!
//! # OPFS worker note
//!
//! The OPFS Web Worker is pure JavaScript — no Rust/WASM is loaded in the
//! worker context. Use the JS source from `pagedb::vfs::opfs::OPFS_WORKER_JS`
//! (available in the pagedb crate when compiled for wasm32 with the `opfs`
//! feature). Write it to a Blob URL or serve it statically, then pass the URL
//! to `openPersistent`:
//!
//! ```js
//! const workerBlob = new Blob([OPFS_WORKER_JS], { type: "text/javascript" });
//! const workerUrl  = URL.createObjectURL(workerBlob);
//! const db         = await NodeDbLiteWasm.openPersistent(workerUrl);
//! ```

pub mod array;
pub mod document;
pub mod graph;
pub mod maintenance;
pub mod open;
pub mod query;
pub mod types;
pub mod udf;
pub mod vector;

pub use types::NodeDbLiteWasm;
pub use udf::register_wasm_udf;

pub(crate) use types::dispatch;
