//! # NodeDB-Lite
//!
//! Embedded, offline-first build of NodeDB for phones, browsers (WASM), and
//! desktops. A single in-process library exposing the same [`NodeDb`] trait as
//! the Origin server — document, key-value, vector, graph, full-text, spatial,
//! columnar, timeseries, and array engines over one storage core — with CRDT
//! sync to an Origin server over WebSocket.
//!
//! ## Quick start
//!
//! ```no_run
//! use nodedb_lite::{NodeDbLite, PagedbStorageMem};
//! use nodedb_client::NodeDb;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = PagedbStorageMem::open_in_memory().await?;
//! let db = NodeDbLite::open(storage).await?;
//! db.execute_sql("CREATE COLLECTION notes", &[]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Durability
//!
//! Writes are buffered for batching; `await` returning `Ok` does **not** by
//! itself guarantee on-disk durability. Durability is bounded by the
//! [`config::LiteConfig::auto_flush_ms`] background flush interval — one second
//! by default — or forced by an explicit [`NodeDbLite::flush`].
//!
//! The `open*` constructors return `Arc<NodeDbLite>` and start that flush task
//! themselves, so the bound holds identically for a direct Rust embedder, the
//! FFI bindings, and the WASM bindings. The task holds a `Weak` handle and
//! stops when the last `Arc` is dropped. Set `auto_flush_ms` to 0 to take full
//! manual control of when data reaches disk.
//!
//! For at-rest encryption see [`Encryption`]. [`NodeDb`]: nodedb_client::NodeDb

pub mod config;
pub mod engine;
pub mod error;
pub mod event;
#[cfg(all(feature = "graphalytics-runner", not(target_arch = "wasm32")))]
pub mod graphalytics;
#[doc(hidden)]
#[cfg(all(feature = "graphalytics-runner", not(target_arch = "wasm32")))]
pub mod graphalytics_diagnostics;
#[cfg(all(feature = "graphalytics-runner", not(target_arch = "wasm32")))]
mod graphalytics_external_sort;
#[cfg(all(feature = "graphalytics-runner", not(target_arch = "wasm32")))]
mod graphalytics_storage;
pub mod identity;
pub mod memory;
pub mod nodedb;
pub mod query;
pub mod runtime;
pub mod sequence;
pub mod storage;
#[cfg(not(target_arch = "wasm32"))]
pub mod sync;

pub use config::LiteConfig;
pub use error::LiteError;
pub use memory::MemoryGovernor;
pub use nodedb::{BatchItem, NodeDbLite, SyncGate};
pub use nodedb_query;
pub use nodedb_types::id_gen;
pub use storage::corruption::CorruptionPolicy;
pub use storage::encryption::Encryption;
pub use storage::engine::{StorageEngine, WriteOp};
#[cfg(not(target_arch = "wasm32"))]
pub use storage::pagedb_storage::PagedbStorageDefault;
#[cfg(target_arch = "wasm32")]
pub use storage::pagedb_storage::PagedbStorageOpfs;
pub use storage::pagedb_storage::{PagedbStorage, PagedbStorageMem};
