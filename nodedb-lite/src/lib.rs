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
//! Strict row writes commit before returning success. Native filesystem pagedb commits survive reopening without an explicit flush.
//! Generic storage backends provide their own commit durability guarantees.
//!
//! Ordinary document and CRDT writes remain buffered. Unindexed `kv_put` and all `kv_delete` calls also remain buffered.
//! Indexed KV puts and SQL KV writes commit directly. Vector rows persist during insertion.
//! [`NodeDbLite::flush`] applies buffered document, CRDT, and KV state and persists derived engine checkpoints.
//!
//! Derived full-text, vector, and spatial checkpoints remain separate from strict row commits.
//! Later maintenance errors do not roll back committed strict rows.
//!
//! The `open*` constructors return `Arc<NodeDbLite>` and schedule automatic flushes through [`config::LiteConfig::auto_flush_ms`].
//! The default interval is one second. Scheduling delays and storage errors prevent a hard wall-clock durability bound.
//! The task holds a `Weak` handle and stops when the last `Arc` disappears.
//! Set `auto_flush_ms` to 0 for manual flushing. Call [`NodeDbLite::flush`] before dropping buffered state.
//!
//! For at-rest encryption see [`Encryption`].
//!
//! ## Cargo features
//!
//! - `sync` (default): the WebSocket sync client and transport. It gates `sync::client`,
//!   `sync::transport`, `SyncClient`, `run_sync_loop`, `NodeDbLite::start_sync`, and
//!   `NodeDbLite::stop_sync`. It pulls in `tokio-tungstenite`, `futures`, and tokio's `net` feature.
//!
//! A native build with `--no-default-features` contains no network code. It links no WebSocket
//! client and enables no tokio networking. Local change capture still compiles: outbound queues,
//! pending-entry types, and the `sync::SyncDelegate` trait. An embedder can drive its own
//! transport through `SyncDelegate`. On wasm32 the `sync` module is absent and the feature has no
//! effect.
//!
//! [`NodeDb`]: nodedb_client::NodeDb

pub mod config;
pub mod engine;
pub mod error;
pub mod event;
pub mod identity;
pub mod index;
pub mod nodedb;
pub mod query;
pub mod runtime;
pub mod sequence;
pub mod storage;
#[cfg(not(target_arch = "wasm32"))]
pub mod sync;
pub mod tasks;

pub use config::LiteConfig;
pub use error::LiteError;
pub use nodedb::{BatchItem, GraphRagParams, HybridSearchParams, NodeDbLite, SyncGate};
pub use nodedb_mem::{EngineId, MemoryGovernor, PressureLevel};
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
