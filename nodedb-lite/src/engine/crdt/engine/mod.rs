// SPDX-License-Identifier: BUSL-1.1

//! CRDT engine for NodeDB-Lite.
//!
//! Wraps one `nodedb-crdt::CrdtState` (Loro-backed) per collection with:
//! - Delta accumulation: tracks unsent mutations for sync
//! - State persistence: save/load Loro snapshots to `StorageEngine`
//! - Delta persistence: save/load unsent deltas to `StorageEngine`
//! - Vector clock: local version tracking for sync handshake
//! - History compaction: periodic Loro GC to prevent unbounded growth
//!
//! Every mutation on Lite (vector insert, graph edge, document put) flows
//! through this engine. It wraps each as a Loro operation, generating a
//! delta that will eventually sync to Origin.

mod checkpoint;
mod lifecycle;
mod list_ops;
mod mutate;
mod pending;
mod persist;
mod read;
mod rotate;
pub mod types;

#[cfg(test)]
mod flush_ack_tests;
#[cfg(test)]
mod tests;

pub use checkpoint::{CrdtPersisted, CrdtWrite, CrdtWriteKind};
pub use types::{CrdtBatchOp, CrdtEngine, CrdtField, PendingDelta};
