// SPDX-License-Identifier: Apache-2.0

//! Sync outbound queue wiring: per-engine durable queues and array CRDT sync state.
//!
//! Compiled out on wasm32 — Lite's sync path (and therefore these outbound
//! queues) is native-only.

use std::sync::Arc;

use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::columnar::ColumnarEngine;
use crate::nodedb::core::types::NodeDbLite;
use crate::storage::engine::StorageEngine;

/// Per-engine sync outbound queues wired up when sync is enabled.
///
/// Grouped into a struct (rather than a tuple) so [`NodeDbLite::build_outbound_queues`]
/// avoids an unwieldy multi-`Option<Arc<..>>` return signature.
#[cfg(not(target_arch = "wasm32"))]
pub(super) struct OutboundQueues<S: StorageEngine> {
    pub(super) columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound<S>>>,
    pub(super) vector_outbound: Option<Arc<crate::sync::VectorOutbound<S>>>,
    pub(super) fts_outbound: Option<Arc<crate::sync::FtsOutbound<S>>>,
    pub(super) spatial_outbound: Option<Arc<crate::sync::SpatialOutbound<S>>>,
    pub(super) timeseries_outbound: Option<Arc<crate::sync::TimeseriesOutbound<S>>>,
}

/// Array CRDT sync state: send path (outbound), receive path (inbound), and
/// the outbound stream sequence frontier.
#[cfg(not(target_arch = "wasm32"))]
pub(super) struct ArraySyncState<S: StorageEngine> {
    pub(super) array_replica: Arc<crate::sync::array::ReplicaState>,
    pub(super) array_schemas: Arc<crate::sync::array::SchemaRegistry<S>>,
    pub(super) array_outbound: Arc<crate::sync::array::ArrayOutbound<S>>,
    pub(super) array_inbound: Arc<crate::sync::array::ArrayInbound<S>>,
    pub(super) array_catchup: Arc<crate::sync::array::CatchupTracker<S>>,
    pub(super) stream_seq: Arc<crate::sync::StreamSeqTracker<S>>,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Wire per-engine sync outbound queues when sync is enabled (native only).
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn build_outbound_queues(
        storage: &Arc<S>,
        sync_enabled: bool,
        outbound_queue_cap: usize,
        columnar: &mut ColumnarEngine<S>,
    ) -> NodeDbResult<OutboundQueues<S>> {
        let columnar_outbound: Option<Arc<crate::sync::ColumnarOutbound<S>>> = if sync_enabled {
            let q = Arc::new(
                crate::sync::ColumnarOutbound::open_with_cap(
                    Arc::clone(storage),
                    outbound_queue_cap,
                )
                .await
                .map_err(|e| NodeDbError::storage(format!("columnar outbound open: {e}")))?,
            );
            columnar.set_outbound(Arc::clone(&q));
            Some(q)
        } else {
            None
        };

        let vector_outbound: Option<Arc<crate::sync::VectorOutbound<S>>> = if sync_enabled {
            let q = Arc::new(
                crate::sync::VectorOutbound::open_with_cap(Arc::clone(storage), outbound_queue_cap)
                    .await
                    .map_err(|e| NodeDbError::storage(format!("vector outbound open: {e}")))?,
            );
            Some(q)
        } else {
            None
        };

        let fts_outbound_init: Option<Arc<crate::sync::FtsOutbound<S>>> = if sync_enabled {
            let q = Arc::new(
                crate::sync::FtsOutbound::open_with_cap(Arc::clone(storage), outbound_queue_cap)
                    .await
                    .map_err(|e| NodeDbError::storage(format!("fts outbound open: {e}")))?,
            );
            Some(q)
        } else {
            None
        };

        let spatial_outbound_init: Option<Arc<crate::sync::SpatialOutbound<S>>> = if sync_enabled {
            let q = Arc::new(
                crate::sync::SpatialOutbound::open_with_cap(
                    Arc::clone(storage),
                    outbound_queue_cap,
                )
                .await
                .map_err(|e| NodeDbError::storage(format!("spatial outbound open: {e}")))?,
            );
            Some(q)
        } else {
            None
        };

        let timeseries_outbound_init: Option<Arc<crate::sync::TimeseriesOutbound<S>>> =
            if sync_enabled {
                let q = Arc::new(
                    crate::sync::TimeseriesOutbound::open_with_cap(
                        Arc::clone(storage),
                        outbound_queue_cap,
                    )
                    .await
                    .map_err(|e| NodeDbError::storage(format!("timeseries outbound open: {e}")))?,
                );
                columnar.set_timeseries_outbound(Arc::clone(&q));
                Some(q)
            } else {
                None
            };

        Ok(OutboundQueues {
            columnar_outbound,
            vector_outbound,
            fts_outbound: fts_outbound_init,
            spatial_outbound: spatial_outbound_init,
            timeseries_outbound: timeseries_outbound_init,
        })
    }

    /// Build array CRDT sync state (send path, receive path, and the outbound
    /// stream sequence frontier) — native only.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn build_array_sync_state(
        storage: &Arc<S>,
        array_state: &Arc<tokio::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    ) -> NodeDbResult<ArraySyncState<S>> {
        // ── Array CRDT sync state (non-wasm only) ─────────────────────────────
        let array_replica = Arc::new(
            crate::sync::array::ReplicaState::load_or_init(&**storage)
                .await
                .map_err(NodeDbError::storage)?,
        );
        let array_schemas = Arc::new(
            crate::sync::array::SchemaRegistry::load(
                Arc::clone(storage),
                Arc::clone(&array_replica),
            )
            .await
            .map_err(NodeDbError::storage)?,
        );
        let array_op_log = Arc::new(crate::sync::array::KvOpLogStore::new(Arc::clone(storage)));
        let array_pending = Arc::new(crate::sync::array::PendingQueue::new(Arc::clone(storage)));
        let array_outbound = Arc::new(crate::sync::array::ArrayOutbound::new(
            Arc::clone(&array_op_log),
            Arc::clone(&array_pending),
            Arc::clone(&array_schemas),
            Arc::clone(&array_replica),
        ));

        // ── Array CRDT inbound receive path (non-wasm only) ───────────────────
        let array_catchup = Arc::new(
            crate::sync::array::CatchupTracker::load(Arc::clone(storage))
                .await
                .map_err(NodeDbError::storage)?,
        );

        // ── Outbound stream sequence frontier ────────────────────────────────
        let stream_seq = Arc::new(
            crate::sync::StreamSeqTracker::load(Arc::clone(storage))
                .await
                .map_err(NodeDbError::storage)?,
        );
        let array_apply_engine = Arc::new(
            crate::sync::array::LiteApplyEngine::new(
                Arc::clone(storage),
                Arc::clone(array_state),
                Arc::clone(&array_schemas),
                Arc::clone(array_outbound.op_log()),
            )
            .await,
        );
        let array_inbound = Arc::new(crate::sync::array::ArrayInbound::new(
            array_apply_engine,
            Arc::clone(&array_schemas),
            Arc::clone(&array_replica),
            Arc::clone(array_outbound.pending()),
            Arc::clone(array_outbound.op_log()),
            Arc::clone(&array_catchup),
        ));

        Ok(ArraySyncState {
            array_replica,
            array_schemas,
            array_outbound,
            array_inbound,
            array_catchup,
            stream_seq,
        })
    }
}
