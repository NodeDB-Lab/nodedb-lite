//! Session catch-up — the requests a new sync session sends to resume every
//! array this replica holds, and the tracker access the apply paths use to
//! record what they applied.

use std::sync::Arc;

use nodedb_types::sync::wire::array::ArrayCatchupRequestMsg;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;
use crate::sync::array::catchup::CatchupTracker;

use super::dispatcher::ArrayInbound;

impl<S: StorageEngine> ArrayInbound<S> {
    /// The per-array high-water mark of ops applied from Origin.
    pub fn catchup_tracker(&self) -> &Arc<CatchupTracker<S>> {
        &self.catchup
    }

    /// Start the catch-up round for a new session.
    ///
    /// Drops partial snapshot assemblies left by the previous session, then
    /// returns one [`ArrayCatchupRequestMsg`] per array in the local schema
    /// registry, sorted by name. Each request starts at the tracker's
    /// last-seen HLC, or at [`nodedb_array::sync::hlc::Hlc::ZERO`] for an
    /// array a `RetentionFloor` reject flagged. Returns an empty list when
    /// the replica holds no arrays.
    pub fn begin_catchup_round(&self) -> Result<Vec<ArrayCatchupRequestMsg>, LiteError> {
        self.discard_partial_snapshots()?;
        let mut arrays = self.schemas.list_arrays();
        arrays.sort_unstable();
        Ok(arrays
            .iter()
            .map(|array| self.catchup.build_request(array))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::sync::hlc::Hlc;

    use super::super::fixtures::{hlc, make_inbound, simple_schema};

    #[tokio::test(flavor = "multi_thread")]
    async fn catchup_round_is_empty_without_arrays() {
        let (inbound, _schemas, _pending, _storage) = make_inbound().await;
        assert!(inbound.begin_catchup_round().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catchup_round_requests_each_array_from_its_last_seen_hlc() {
        let (inbound, schemas, _pending, _storage) = make_inbound().await;
        schemas.put_schema("b", &simple_schema("b")).await.unwrap();
        schemas.put_schema("a", &simple_schema("a")).await.unwrap();
        inbound
            .catchup_tracker()
            .record("b", hlc(700))
            .await
            .unwrap();

        let requests = inbound.begin_catchup_round().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].array, "a");
        assert_eq!(requests[0].from_hlc_bytes, Hlc::ZERO.to_bytes());
        assert_eq!(requests[1].array, "b");
        assert_eq!(requests[1].from_hlc_bytes, hlc(700).to_bytes());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catchup_round_requests_flagged_array_from_zero() {
        let (inbound, schemas, _pending, _storage) = make_inbound().await;
        schemas.put_schema("a", &simple_schema("a")).await.unwrap();
        let tracker = inbound.catchup_tracker();
        tracker.record("a", hlc(900)).await.unwrap();
        tracker.record_reject_retention_floor("a").await.unwrap();

        let requests = inbound.begin_catchup_round().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].from_hlc_bytes, Hlc::ZERO.to_bytes());
    }
}
