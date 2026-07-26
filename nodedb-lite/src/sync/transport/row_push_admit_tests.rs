//! Tests for the `RowPush` high-water-mark gate (`SyncClient::admit_row_push`).
//!
//! Split out from `tests.rs` to stay under the repo's per-file line limit;
//! reuses that module's `MockDelegate` / `make_client` helpers.

use std::sync::Arc;

use nodedb_types::sync::wire::{RowOp, RowPushMsg, SyncFrame, SyncMessageType};

use super::delegate::SyncDelegate;
use super::dispatch::dispatch_frame;
use super::tests::{MockDelegate, make_client};

fn row_push(collection: &str, document_id: &str, sequence: u64) -> RowPushMsg {
    RowPushMsg {
        collection: collection.to_string(),
        document_id: document_id.to_string(),
        payload: vec![0x80], // empty msgpack map
        op: RowOp::Upsert,
        lsn: 0,
        peer_id: 1,
        sequence,
    }
}

async fn dispatch(
    client: &Arc<crate::sync::client::SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    msg: &RowPushMsg,
) {
    let frame = SyncFrame::try_encode(SyncMessageType::RowPush, msg).expect("test frame encode");
    dispatch_frame(client, delegate, &frame).await;
}

/// A re-delivered frame at the same sequence must be applied once, not
/// twice — otherwise Origin re-sending an already-acked RowPush (e.g. after
/// a reconnect) would re-apply a stale post-image on top of newer local
/// state.
#[tokio::test]
async fn duplicate_sequence_is_applied_once() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    let msg = row_push("orders", "o-1", 5);
    dispatch(&client, &delegate, &msg).await;
    dispatch(&client, &delegate, &msg).await;

    assert_eq!(
        mock.applied_rows().len(),
        1,
        "duplicate sequence must be applied exactly once"
    );
}

/// A lower sequence arriving after a higher one is a stale/out-of-order
/// re-delivery and must be skipped, or it would leave an older post-image
/// as the final applied state.
#[tokio::test]
async fn lower_sequence_after_higher_is_skipped() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    dispatch(&client, &delegate, &row_push("orders", "o-1", 10)).await;
    dispatch(&client, &delegate, &row_push("orders", "o-2", 3)).await;

    let applied = mock.applied_rows();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].1, "o-1");
}

/// The watermark is keyed per `(peer_id, collection)`, not globally — two
/// different collections reusing the same sequence number (each has its
/// own monotonic counter on Origin) must both be applied.
#[tokio::test]
async fn same_sequence_different_collections_both_applied() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    dispatch(&client, &delegate, &row_push("orders", "o-1", 1)).await;
    dispatch(&client, &delegate, &row_push("invoices", "i-1", 1)).await;

    let applied = mock.applied_rows();
    assert_eq!(
        applied.len(),
        2,
        "a shared sequence number across distinct collections must not collide in the gate"
    );
}

/// `sequence == 0` is the unsequenced sentinel for DDL-managed system rows.
/// It carries no ordering information, so it must always be applied and
/// must never move the watermark for its collection.
#[tokio::test]
async fn unsequenced_frames_always_apply_and_never_gate() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    dispatch(&client, &delegate, &row_push("system_alerts", "a-1", 0)).await;
    dispatch(&client, &delegate, &row_push("system_alerts", "a-2", 0)).await;
    // A later, legitimately-sequenced frame must still apply even though
    // two sequence-0 frames for the same collection came before it.
    dispatch(&client, &delegate, &row_push("system_alerts", "a-3", 1)).await;

    let applied = mock.applied_rows();
    assert_eq!(
        applied.len(),
        3,
        "sequence-0 frames must always apply and must not block a later sequenced frame"
    );
}
