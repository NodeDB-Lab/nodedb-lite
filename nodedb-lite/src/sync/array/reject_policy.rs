//! What Lite does with a locally-originated op Origin refuses.
//!
//! Origin's inbound array path returns one `ArrayRejectMsg` per refused
//! frame. Its reason decides the op's fate:
//!
//! | Reason | Origin's meaning | Disposition |
//! |---|---|---|
//! | `ArrayUnknown` | No schema for the array on Origin | [`RejectDisposition::ResendSchema`] |
//! | `SchemaTooNew` | Op schema HLC is ahead of Origin's | [`RejectDisposition::ResendSchema`] |
//! | `RetentionFloor` | This replica's catch-up cursor is below Origin's GC floor | [`RejectDisposition::CatchUp`] |
//! | `ShapeInvalid` | The op failed decode or shape validation | [`RejectDisposition::Drop`] |
//! | `EngineRejected`, `QuotaExceeded`, `WireVersionMismatch`, any other | Refused for a reason Lite cannot repair in this session | [`RejectDisposition::Park`] |
//!
//! `EngineRejected` covers both transient errors (Raft timeout, proposer
//! unavailable) and lasting ones (permission denied, producer fenced). Origin
//! sends no code that tells them apart, so the op stays durable and is
//! retried in the next session. A dropped op is a lost write that a later
//! session can deliver.

use nodedb_types::sync::wire::array::ArrayRejectReason;

/// The fate of a locally-originated op Origin refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectDisposition {
    /// Keep the op. Announce the array's schema again, then re-send the op.
    ResendSchema,
    /// The reject names this replica's catch-up cursor, not a local op. Ask
    /// Origin for a snapshot. The pending queue is untouched.
    CatchUp,
    /// Origin can never accept the op. Remove it from the pending queue.
    Drop,
    /// Keep the op durable but stop sending it until the next session.
    Park,
}

/// Map a reject reason to its disposition.
pub fn reject_disposition(reason: ArrayRejectReason) -> RejectDisposition {
    match reason {
        ArrayRejectReason::ArrayUnknown | ArrayRejectReason::SchemaTooNew => {
            RejectDisposition::ResendSchema
        }
        ArrayRejectReason::RetentionFloor => RejectDisposition::CatchUp,
        ArrayRejectReason::ShapeInvalid => RejectDisposition::Drop,
        _ => RejectDisposition::Park,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_rejects_resend_the_schema() {
        assert_eq!(
            reject_disposition(ArrayRejectReason::ArrayUnknown),
            RejectDisposition::ResendSchema
        );
        assert_eq!(
            reject_disposition(ArrayRejectReason::SchemaTooNew),
            RejectDisposition::ResendSchema
        );
    }

    #[test]
    fn retention_floor_requests_a_catch_up() {
        assert_eq!(
            reject_disposition(ArrayRejectReason::RetentionFloor),
            RejectDisposition::CatchUp
        );
    }

    #[test]
    fn invalid_shape_drops_the_op() {
        assert_eq!(
            reject_disposition(ArrayRejectReason::ShapeInvalid),
            RejectDisposition::Drop
        );
    }

    #[test]
    fn unrepairable_rejects_park_the_op() {
        for reason in [
            ArrayRejectReason::EngineRejected,
            ArrayRejectReason::QuotaExceeded,
            ArrayRejectReason::WireVersionMismatch,
        ] {
            assert_eq!(reject_disposition(reason), RejectDisposition::Park);
        }
    }
}
