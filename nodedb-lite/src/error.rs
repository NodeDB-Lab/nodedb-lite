//! Error types for NodeDB-Lite.

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
        nodedb_types::error::NodeDbError::storage(e)
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
    fn lite_error_backpressure_display() {
        let e = LiteError::Backpressure {
            detail: "outbound queue full".into(),
        };
        assert!(e.to_string().contains("backpressure"));
        assert!(e.to_string().contains("outbound queue full"));
    }
}
