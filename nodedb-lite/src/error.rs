//! Error types for NodeDB-Lite.

mod from_crdt;
mod from_sql;

/// Errors specific to the Lite embedded engine.
#[derive(Debug, thiserror::Error)]
pub enum LiteError {
    #[error("storage error: {detail}")]
    Storage { detail: String },

    #[error("storage backend returned poison lock")]
    LockPoisoned,

    #[error("async task join failed: {detail}")]
    JoinError { detail: String },

    #[error("serialization error: {detail}")]
    Serialization { detail: String },

    #[error("namespace {ns} not recognized")]
    InvalidNamespace { ns: u8 },

    #[error("bad request: {detail}")]
    BadRequest { detail: String },

    /// A statement named a collection the engine it addressed does not
    /// hold. Maps to `collection_not_found` at the public API boundary.
    #[error("{engine} collection '{collection}' does not exist")]
    CollectionNotFound {
        engine: &'static str,
        collection: String,
    },

    /// A write that would duplicate a declared unique key. Maps to SQLSTATE
    /// `23505` at the SQL boundary.
    #[error("duplicate key value violates unique constraint on '{collection}': {detail}")]
    UniqueViolation { collection: String, detail: String },

    /// A write that would store NULL in a NOT NULL column. Maps to SQLSTATE
    /// `23502` at the SQL boundary.
    #[error("null value in column '{column}' of '{collection}' violates not-null constraint")]
    NotNullViolation { collection: String, column: String },

    #[error("sync error: {detail}")]
    Sync { detail: String },

    #[error("query error: {0}")]
    Query(String),

    #[error("Arrow type conversion: expected {expected}, got {got}")]
    ArrowTypeConversion { expected: String, got: String },

    #[error("backpressure: {detail}")]
    Backpressure { detail: String },

    /// Feature or SQL construct not supported in this Lite beta release.
    #[error("unsupported: {detail}")]
    Unsupported { detail: String },

    /// The OPFS worker bridge failed to start or encountered an IPC error.
    ///
    /// This variant is produced when `PagedbStorage::open_opfs` cannot spawn
    /// the dedicated Web Worker or when the worker signals a corruption-class
    /// failure that cannot be recovered automatically (OPFS has no rename).
    #[error("OPFS worker bridge failed: {detail}")]
    WorkerFailed { detail: String },

    /// An error during key derivation, salt I/O, or encryption setup.
    #[error("encryption error: {detail}")]
    Encryption { detail: String },

    /// A corruption-class failure surfaced from the storage backend.
    ///
    /// Kept as a distinct variant (rather than folded into [`LiteError::Storage`])
    /// so the corruption signal survives the error-type conversions and reaches
    /// the open-sequence recovery driver, which renames the corrupt store aside,
    /// recreates a fresh one, and retries the open exactly once.
    #[error("storage corrupted: {detail}")]
    Corrupted { detail: String },

    /// A full-text index update failed while writing a document.
    ///
    /// The write that triggered it must fail too: the row and its postings are
    /// written together, and nothing re-indexes the gap afterwards — the next
    /// write to the document only indexes its new text, so a swallowed failure
    /// leaves the document permanently missing from (or stale in) the index.
    #[error("full-text index update failed for {collection}: {detail}")]
    FtsIndex { collection: String, detail: String },

    /// A full-text read named a field no document of the collection holds as
    /// text, while other fields of the collection are text-indexed. Maps to a
    /// bad-request error at the public API boundary.
    #[error(
        "collection '{collection}' has no full-text index for field '{field}': \
         write a document with a string value in '{field}', or search with an \
         empty field to cover every string field"
    )]
    TextIndexMissing { collection: String, field: String },

    /// A full-text query the index cannot run, such as a query with only
    /// negated terms. Maps to a bad-request error at the public API boundary.
    #[error("invalid full-text query on {collection}: {detail}")]
    FtsQueryInvalid { collection: String, detail: String },

    /// A full-text index read failed. The search fails with it: an empty
    /// result would claim that no document matches.
    #[error("full-text search failed for {collection}: {detail}")]
    FtsSearch { collection: String, detail: String },

    /// A KV counter atomic read a stored value it cannot parse as a number,
    /// or computed a result out of range. Maps to SQLSTATE `22P02` or `22003`
    /// at the SQL boundary, the same as Origin.
    #[error("{fault} on {collection}")]
    CounterFault {
        collection: String,
        fault: nodedb_physical::kv_atomic::CounterFault,
    },

    /// A value of the wrong kind for its target: a KV atomic on a typed row
    /// with no column of the type it reads, a text element in a `VECTOR`
    /// literal, a scalar write onto a nested CRDT container. `collection` is
    /// empty when the fault names no collection. SQLSTATE `42804`.
    #[error("{}", type_mismatch_message(.collection, .detail))]
    TypeMismatch { collection: String, detail: String },

    /// An input the engine cannot compute on: a vector of the wrong
    /// dimension, or index input it cannot use. The same public error
    /// Origin raises for the same fault, SQLSTATE `22000`.
    #[error("{detail}")]
    DataException { detail: String },

    /// A value past the range of its numeric type: an integer wider than
    /// its column, a constant that overflows. SQLSTATE `22003`.
    #[error("{detail}")]
    NumericValueOutOfRange { detail: String },

    /// A statement or CRDT operation named an object that does not exist,
    /// such as a document absent at the requested version. SQLSTATE `42704`.
    #[error("{object} does not exist")]
    UndefinedObject { object: String },

    /// The object exists, but not in the state the request needs, such as a
    /// CRDT version below the compaction boundary. SQLSTATE `55000`.
    #[error("{detail}")]
    ObjectNotReady { object: String, detail: String },

    /// A named constraint refused a write. `constraint` names the
    /// constraint. SQLSTATE class `23`.
    #[error("constraint violation: {constraint} on '{collection}': {detail}")]
    ConstraintViolation {
        collection: String,
        constraint: String,
        detail: String,
    },

    /// The request applied nothing it reports as done. The same request
    /// succeeds once a transient condition clears: missing causal history
    /// arrives, or a full queue drains. Retriable.
    #[error("refused, retry: {detail}")]
    RetryableRefusal { detail: String },

    /// The caller is not allowed to perform the request: expired auth or a
    /// delta signature that does not check out.
    #[error("authorization denied on {resource}")]
    AuthorizationDenied { resource: String },

    /// A pre-validation gate refused the request, such as a replayed sync
    /// sequence number. The same request fails the same way every time.
    #[error("pre-validation rejected: {reason}")]
    PrevalidationRejected { reason: String },
}

/// `detail` alone when no collection is named.
fn type_mismatch_message(collection: &str, detail: &str) -> String {
    if collection.is_empty() {
        detail.to_string()
    } else {
        format!("type mismatch on {collection}: {detail}")
    }
}

impl LiteError {
    /// `collection` is not a collection of `engine` (`"strict"`,
    /// `"columnar"`).
    pub fn collection_not_found(engine: &'static str, collection: impl Into<String>) -> Self {
        Self::CollectionNotFound {
            engine,
            collection: collection.into(),
        }
    }

    /// `e` classified as `From<CrdtError>` classifies it. A storage-class
    /// error names `op`, the operation that raised it.
    pub(crate) fn crdt(op: &str, e: nodedb_crdt::CrdtError) -> Self {
        Self::from(e).within(op)
    }

    /// `self` with `op` named in a storage-class detail. Every other class
    /// keeps its variant and message.
    pub(crate) fn within(self, op: &str) -> Self {
        match self {
            Self::Storage { detail } => Self::Storage {
                detail: format!("{op}: {detail}"),
            },
            other => other,
        }
    }
}

/// A vector engine error, classified as Origin classifies it: an input
/// vector of the wrong dimension, or index input the engine cannot use, is
/// the caller's data error; a memory budget refusal is backpressure; a
/// stored-data or checkpoint failure is corruption.
impl From<nodedb_vector::error::VectorError> for LiteError {
    fn from(e: nodedb_vector::error::VectorError) -> Self {
        use nodedb_vector::error::VectorError as Ve;
        let detail = e.to_string();
        match e {
            Ve::DimensionMismatch { .. } | Ve::InvalidInput { .. } => {
                Self::DataException { detail }
            }
            Ve::BudgetExhausted(_) => Self::Backpressure { detail },
            Ve::SegmentIo(_) | Ve::InvalidFilterBitmap { .. } => Self::Storage { detail },
            // `VectorError` is `#[non_exhaustive]`; every other variant is a
            // stored-data or checkpoint failure.
            _ => Self::Corrupted { detail },
        }
    }
}

/// Returns `true` when `e` is the corruption-class variant that should drive
/// the store-recovery retry (rename the corrupt store, recreate fresh, reopen).
pub(crate) fn is_corruption(e: &LiteError) -> bool {
    matches!(e, LiteError::Corrupted { .. })
}

/// Expression evaluation failure: division or modulo by a zero divisor, a
/// call to a function no evaluator implements, or a function argument it
/// cannot compute on (vectors of different dimensions, an argument of the
/// wrong type, a malformed JSONPath). SQL requires each to fail the
/// statement rather than fold the row to `NULL`. Mapped to
/// [`LiteError::Query`] so it surfaces to the caller instead of silently
/// filtering rows out.
impl From<nodedb_query::EvalError> for LiteError {
    fn from(e: nodedb_query::EvalError) -> Self {
        Self::Query(e.to_string())
    }
}

impl From<nodedb_types::columnar::SchemaError> for LiteError {
    fn from(e: nodedb_types::columnar::SchemaError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<LiteError> for nodedb_types::error::NodeDbError {
    fn from(e: LiteError) -> Self {
        use nodedb_types::error::{ErrorCode, NodeDbError};
        match e {
            // The same public errors Origin builds for the same faults.
            LiteError::CounterFault { collection, fault } => {
                NodeDbError::kv_counter_fault(collection, fault, fault.is_out_of_range())
            }
            LiteError::TypeMismatch { collection, detail } if collection.is_empty() => {
                NodeDbError::from_wire(ErrorCode::TYPE_MISMATCH, detail)
            }
            LiteError::TypeMismatch { collection, detail } => {
                NodeDbError::type_mismatch(collection, detail)
            }
            LiteError::DataException { detail } => NodeDbError::data_exception(detail),
            LiteError::NumericValueOutOfRange { detail } => {
                NodeDbError::numeric_value_out_of_range(detail)
            }
            LiteError::UndefinedObject { object } => NodeDbError::undefined_object(object),
            LiteError::ObjectNotReady { object, detail } => {
                NodeDbError::object_not_ready(object, detail)
            }
            LiteError::CollectionNotFound { collection, .. } => {
                NodeDbError::collection_not_found(collection)
            }
            LiteError::UniqueViolation { collection, detail } => {
                NodeDbError::constraint_violation(collection, "unique", detail)
            }
            LiteError::NotNullViolation { collection, column } => {
                NodeDbError::constraint_violation(
                    collection,
                    "not_null",
                    format!("null value in column '{column}'"),
                )
            }
            LiteError::ConstraintViolation {
                collection,
                constraint,
                detail,
            } => NodeDbError::constraint_violation(collection, constraint, detail),
            // Nothing the caller asked for took effect, and the same request
            // succeeds later: the retriable class, as Origin renders it.
            e @ LiteError::RetryableRefusal { .. } => {
                NodeDbError::from_wire(ErrorCode::WRITE_CONFLICT, e.to_string())
            }
            LiteError::AuthorizationDenied { resource } => {
                NodeDbError::authorization_denied(resource)
            }
            LiteError::PrevalidationRejected { reason } => {
                NodeDbError::prevalidation_rejected("crdt", reason)
            }
            LiteError::Query(detail) => NodeDbError::plan_error(detail),
            LiteError::BadRequest { detail } => NodeDbError::bad_request(detail),
            // SQLSTATE `0A000`: `SQL_NOT_ENABLED` is the class every
            // feature-not-supported refusal carries.
            LiteError::Unsupported { detail } => {
                NodeDbError::from_wire(ErrorCode::SQL_NOT_ENABLED, detail)
            }
            e @ (LiteError::TextIndexMissing { .. } | LiteError::FtsQueryInvalid { .. }) => {
                NodeDbError::bad_request(e)
            }
            e if is_corruption(&e) => NodeDbError::segment_corrupted(e.to_string()),
            e => NodeDbError::storage(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lite_error_display() {
        let e = LiteError::Storage {
            detail: "disk full".into(),
        };
        assert!(e.to_string().contains("disk full"));
    }

    #[test]
    fn lite_error_converts_to_nodedb_error() {
        let e = LiteError::Storage {
            detail: "test".into(),
        };
        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert!(ndb.to_string().contains("test"));
    }

    #[test]
    fn a_unique_violation_is_a_constraint_violation() {
        let e = LiteError::UniqueViolation {
            collection: "users".into(),
            detail: "key ($.email)=(a@x) already exists".into(),
        };
        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert!(ndb.is_constraint_violation(), "{ndb}");
        assert!(ndb.to_string().contains("unique"), "{ndb}");
    }

    #[test]
    fn lite_error_encryption_display_and_convert() {
        let e = LiteError::Encryption {
            detail: "argon2 key derivation failed".into(),
        };
        let rendered = e.to_string();
        assert!(rendered.contains("encryption error"));
        assert!(rendered.contains("argon2 key derivation failed"));

        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert!(ndb.to_string().contains("argon2 key derivation failed"));
    }

    #[test]
    fn a_missing_collection_is_collection_not_found() {
        let e = LiteError::collection_not_found("strict", "users");
        assert_eq!(e.to_string(), "strict collection 'users' does not exist");

        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert_eq!(
            ndb.code(),
            nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND,
            "{ndb}"
        );
    }

    #[test]
    fn a_not_null_violation_is_a_constraint_violation() {
        let e = LiteError::NotNullViolation {
            collection: "users".into(),
            column: "email".into(),
        };
        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert!(ndb.is_constraint_violation(), "{ndb}");
    }

    #[test]
    fn a_bad_request_keeps_its_class() {
        let e = LiteError::BadRequest {
            detail: "invalid version vector JSON".into(),
        };
        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert_eq!(ndb.code(), nodedb_types::error::ErrorCode::BAD_REQUEST);
    }

    #[test]
    fn a_type_mismatch_without_a_collection_renders_its_detail() {
        let e = LiteError::TypeMismatch {
            collection: String::new(),
            detail: "column 'v': expected a number".into(),
        };
        assert_eq!(e.to_string(), "column 'v': expected a number");
        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert_eq!(ndb.code(), nodedb_types::error::ErrorCode::TYPE_MISMATCH);
    }

    #[test]
    fn lite_error_backpressure_display() {
        let e = LiteError::Backpressure {
            detail: "outbound queue full".into(),
        };
        assert!(e.to_string().contains("backpressure"));
        assert!(e.to_string().contains("outbound queue full"));
    }
}
