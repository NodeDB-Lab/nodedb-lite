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
//! let db = NodeDbLite::open(storage, 1u64).await?;
//! db.execute_sql("CREATE COLLECTION notes", &[]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Durability
//!
//! Writes are buffered for batching; `await` returning `Ok` does **not** by
//! itself guarantee on-disk durability. Durability is bounded by the
//! [`config::LiteConfig::auto_flush_ms`] background flush interval, or forced
//! by an explicit [`NodeDbLite::flush`]. For at-rest encryption see
//! [`Encryption`]. [`NodeDb`]: nodedb_client::NodeDb

pub mod config;
pub mod engine;
pub mod error;
pub mod event;
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
pub use storage::encryption::Encryption;
pub use storage::engine::{StorageEngine, WriteOp};
#[cfg(not(target_arch = "wasm32"))]
pub use storage::pagedb_storage::PagedbStorageDefault;
#[cfg(target_arch = "wasm32")]
pub use storage::pagedb_storage::PagedbStorageOpfs;
pub use storage::pagedb_storage::{PagedbStorage, PagedbStorageMem};
