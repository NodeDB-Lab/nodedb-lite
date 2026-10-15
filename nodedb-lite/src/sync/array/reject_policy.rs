//! What Lite does with a locally-originated op Origin refuses.
//!
//! Origin's inbound array path returns one `ArrayRejectMsg` per refused
//! frame. Each reason has one fixed `ArrayRejectClass`, and the class decides
//! the op's fate:
//!
//! | Class | Reasons | Disposition |
//! |---|---|---|
//! | `SchemaRequired` | `ArrayUnknown`, `SchemaTooNew` | [`RejectDisposition::ResendSchema`] |
//! | `CatchUp` | `RetentionFloor` | [`RejectDisposition::CatchUp`] |
//! | `Permanent` | `ShapeInvalid`, `EngineRefused`, `PermissionDenied` | [`RejectDisposition::Drop`] |
//! | `Transient` | `EngineUnavailable`, `SnapshotCapacity`, `SnapshotBufferLost` | [`RejectDisposition::Park`] |
//! | `Reauthenticate` | `Unauthenticated` | [`RejectDisposition::Park`] |
//!
//! A transient reject applied nothing, so the op stays durable and the next
//! session re-sends it. Origin deduplicates a re-sent op by its HLC. An
//! unauthenticated session ends with a fresh handshake, and the next session
//! re-sends the op.

use nodedb_types::sync::wire::array::{ArrayRejectClass, ArrayRejectReason};

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
    match reason.class() {
        ArrayRejectClass::SchemaRequired => RejectDisposition::ResendSchema,
        ArrayRejectClass::CatchUp => RejectDisposition::CatchUp,
        ArrayRejectClass::Permanent => RejectDisposition::Drop,
        ArrayRejectClass::Transient | ArrayRejectClass::Reauthenticate => RejectDisposition::Park,
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
    fn permanent_rejects_drop_the_op() {
        for reason in [
            ArrayRejectReason::ShapeInvalid,
            ArrayRejectReason::EngineRefused,
            ArrayRejectReason::PermissionDenied,
        ] {
            assert_eq!(reject_disposition(reason), RejectDisposition::Drop);
        }
    }

    #[test]
    fn transient_and_auth_rejects_park_the_op() {
        for reason in [
            ArrayRejectReason::EngineUnavailable,
            ArrayRejectReason::SnapshotCapacity,
            ArrayRejectReason::SnapshotBufferLost,
            ArrayRejectReason::Unauthenticated,
        ] {
            assert_eq!(reject_disposition(reason), RejectDisposition::Park);
        }
    }
}
