// SPDX-License-Identifier: Apache-2.0

//! The class of each CRDT engine error, as Origin classifies it.
//!
//! A client request causes most CRDT errors: a malformed delta, a version
//! that no longer exists, a write that names an absent document. Each keeps
//! a client class here. Only engine faults stay `Storage`.

use nodedb_crdt::CrdtError;

use super::LiteError;

/// Exhaustive, so a new CRDT error picks its own class rather than
/// defaulting to `Storage`.
impl From<CrdtError> for LiteError {
    fn from(e: CrdtError) -> Self {
        match e {
            CrdtError::ConstraintViolation {
                constraint,
                collection,
                detail,
            } => Self::ConstraintViolation {
                collection,
                constraint,
                detail,
            },
            // Delta or snapshot bytes that are malformed, past a limit, or of
            // a shape no collection holds, and block-list paths or indexes
            // that do not resolve. Class `22`.
            client @ (CrdtError::ImportTooLarge { .. }
            | CrdtError::ImportMalformed { .. }
            | CrdtError::ImportInvalidOperationRange
            | CrdtError::ImportOperationLimitExceeded { .. }
            | CrdtError::PreviewDeltaTooLarge { .. }
            | CrdtError::PreviewMalformed { .. }
            | CrdtError::PreviewInvalidOperationRange
            | CrdtError::PreviewOperationLimitExceeded { .. }
            | CrdtError::PreviewWriteSetLimitExceeded { .. }
            | CrdtError::PreviewTargetMismatch { .. }
            | CrdtError::PreviewPostImageTooLarge { .. }
            | CrdtError::NonMapRootContainer { .. }
            | CrdtError::NonMapRowValue { .. }
            | CrdtError::BlockListPathUnresolved { .. }
            | CrdtError::BlockListIndexOutOfBounds { .. }) => Self::DataException {
                detail: client.to_string(),
            },
            // A value of the wrong kind for the field or path it targets.
            // `42804`.
            CrdtError::ScalarFieldShadowsContainer { ref collection, .. } => Self::TypeMismatch {
                collection: collection.clone(),
                detail: e.to_string(),
            },
            client @ CrdtError::BlockListNotMovable { .. } => Self::TypeMismatch {
                collection: String::new(),
                detail: client.to_string(),
            },
            CrdtError::RowAbsentAtVersion { collection, row_id } => Self::UndefinedObject {
                object: format!(
                    "document \"{row_id}\" in collection \"{collection}\" at the target version"
                ),
            },
            CrdtError::BlockListRowAbsent { collection, row_id } => Self::UndefinedObject {
                object: format!("document \"{row_id}\" in collection \"{collection}\""),
            },
            CrdtError::UnknownCollection(collection) => Self::UndefinedObject {
                object: format!("collection \"{collection}\""),
            },
            client @ CrdtError::VersionBeforeCompactionBoundary { .. } => Self::ObjectNotReady {
                object: "CRDT version".into(),
                detail: client.to_string(),
            },
            // The same bytes succeed once the missing causal history
            // arrives, or once the dead-letter queue drains.
            retry @ (CrdtError::ImportPendingDependencies
            | CrdtError::PreviewPendingDependencies
            | CrdtError::DlqFull { .. }) => Self::RetryableRefusal {
                detail: retry.to_string(),
            },
            denied @ (CrdtError::AuthExpired { .. } | CrdtError::InvalidSignature { .. }) => {
                Self::AuthorizationDenied {
                    resource: denied.to_string(),
                }
            }
            // A replayed sequence number fails the same way every time.
            replay @ CrdtError::ReplayDetected { .. } => Self::PrevalidationRejected {
                reason: replay.to_string(),
            },
            // Engine faults: a Loro failure, a pending transaction on the
            // authoritative document, or an apply this replica could not run.
            fault @ (CrdtError::DeltaApplyFailed(_)
            | CrdtError::PreviewSourceTransactionPending { .. }
            | CrdtError::Loro(_)) => Self::Storage {
                detail: fault.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::{ErrorCode, NodeDbError};

    use super::*;

    fn public(e: CrdtError) -> NodeDbError {
        NodeDbError::from(LiteError::from(e))
    }

    #[test]
    fn a_missing_document_or_collection_is_an_undefined_object() {
        for e in [
            CrdtError::RowAbsentAtVersion {
                collection: "notes".into(),
                row_id: "doc".into(),
            },
            CrdtError::BlockListRowAbsent {
                collection: "pages".into(),
                row_id: "p".into(),
            },
            CrdtError::UnknownCollection("c".into()),
        ] {
            assert_eq!(public(e).code(), ErrorCode::UNDEFINED_OBJECT);
        }
    }

    #[test]
    fn a_version_below_the_compaction_boundary_is_not_ready() {
        let e = CrdtError::VersionBeforeCompactionBoundary {
            peer: 1,
            requested: 2,
            discarded: 3,
        };
        assert_eq!(public(e).code(), ErrorCode::OBJECT_NOT_READY);
    }

    #[test]
    fn malformed_client_input_is_a_data_exception() {
        for e in [
            CrdtError::ImportMalformed { detail: "x".into() },
            CrdtError::ImportTooLarge {
                limit: 1,
                actual: 2,
            },
            CrdtError::PreviewPostImageTooLarge {
                limit: 1,
                actual: 2,
            },
            CrdtError::NonMapRowValue {
                collection: "c".into(),
                row_id: "r".into(),
                value: "a List container".into(),
            },
            CrdtError::BlockListPathUnresolved {
                list_path: "a.blocks".into(),
                segment: "a".into(),
            },
            CrdtError::BlockListIndexOutOfBounds {
                list_path: "blocks".into(),
                index: 9,
                len: 1,
            },
        ] {
            assert_eq!(public(e).code(), ErrorCode::DATA_EXCEPTION);
        }
    }

    #[test]
    fn a_wrong_kind_of_value_is_a_type_mismatch() {
        let shadow = LiteError::from(CrdtError::ScalarFieldShadowsContainer {
            collection: "c".into(),
            row_id: "r".into(),
            field: "blocks".into(),
        });
        assert!(
            matches!(&shadow, LiteError::TypeMismatch { collection, .. } if collection == "c"),
            "{shadow:?}"
        );
        assert_eq!(NodeDbError::from(shadow).code(), ErrorCode::TYPE_MISMATCH);
        let plain = CrdtError::BlockListNotMovable {
            list_path: "blocks".into(),
        };
        assert_eq!(public(plain).code(), ErrorCode::TYPE_MISMATCH);
    }

    #[test]
    fn missing_causal_history_and_a_full_queue_are_retriable() {
        for e in [
            CrdtError::ImportPendingDependencies,
            CrdtError::PreviewPendingDependencies,
            CrdtError::DlqFull {
                capacity: 1,
                pending: 1,
            },
        ] {
            let mapped = public(e);
            assert_eq!(mapped.code(), ErrorCode::WRITE_CONFLICT);
            assert!(mapped.is_retriable(), "{mapped}");
        }
    }

    #[test]
    fn a_constraint_violation_keeps_its_constraint() {
        let mapped = LiteError::from(CrdtError::ConstraintViolation {
            constraint: "users_email_unique".into(),
            collection: "users".into(),
            detail: "duplicate".into(),
        });
        assert!(
            matches!(&mapped, LiteError::ConstraintViolation { constraint, collection, .. }
                if constraint == "users_email_unique" && collection == "users"),
            "{mapped:?}"
        );
        assert!(NodeDbError::from(mapped).is_constraint_violation());
    }

    #[test]
    fn auth_and_replay_refusals_keep_their_class() {
        let expired = public(CrdtError::AuthExpired {
            user_id: 1,
            expired_at: 2,
        });
        assert!(expired.is_auth_denied(), "{expired}");
        let replay = public(CrdtError::ReplayDetected {
            user_id: 1,
            device_id: 2,
            seq_no: 3,
            last_seen: 4,
        });
        assert_eq!(replay.code(), ErrorCode::PREVALIDATION_REJECTED);
    }

    #[test]
    fn only_engine_faults_stay_storage() {
        for e in [
            CrdtError::PreviewSourceTransactionPending { operations: 1 },
            CrdtError::Loro("boom".into()),
            CrdtError::DeltaApplyFailed("boom".into()),
        ] {
            assert!(matches!(LiteError::from(e), LiteError::Storage { .. }));
        }
    }

    #[test]
    fn a_storage_class_crdt_error_names_its_operation() {
        let e = LiteError::crdt("CRDT upsert", CrdtError::Loro("boom".into()));
        assert!(
            matches!(&e, LiteError::Storage { detail } if detail.starts_with("CRDT upsert: ")),
            "{e:?}"
        );
        let client = LiteError::crdt("CRDT upsert", CrdtError::UnknownCollection("c".into()));
        assert!(matches!(client, LiteError::UndefinedObject { .. }));
    }
}
