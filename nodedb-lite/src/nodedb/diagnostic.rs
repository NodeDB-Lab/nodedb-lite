//! Diagnostic dump — JSON blob for remote bug reports.
//!
//! `db.diagnostic_dump()` produces a structured report containing:
//! - Redacted sync config (tokens masked)
//! - Storage stats (redb namespace counts)
//! - Engine stats (from health API)
//! - Pending delta summary (count + oldest timestamp, not actual data)
//! - Flow control state
//! - Recent error summary
//!
//! Designed for production debugging: safe to send to support (no user data,
//! no embeddings, no document contents).

use serde::Serialize;

use crate::storage::engine::StorageEngine;

use super::core::NodeDbLite;
use super::health::HealthStatus;
use super::lock_ext::LockExt;

/// Full diagnostic dump for bug reports.
#[derive(Debug, Serialize)]
pub struct DiagnosticDump {
    /// Timestamp of this dump (epoch ms).
    pub timestamp_ms: u64,
    /// Health status snapshot.
    pub health: HealthStatus,
    /// Pending delta summary (no actual data).
    pub pending_summary: PendingSummary,
    /// Storage namespace entry counts.
    pub storage_counts: StorageCounts,
    /// Engine version and build info.
    pub build_info: BuildInfo,
}

/// Pending delta summary — safe to include in bug reports.
#[derive(Debug, Serialize)]
pub struct PendingSummary {
    /// Number of pending deltas.
    pub count: usize,
    /// Total bytes of pending deltas.
    pub total_bytes: usize,
    /// Oldest pending mutation_id (0 if no pending).
    pub oldest_mutation_id: u64,
    /// Newest pending mutation_id (0 if no pending).
    pub newest_mutation_id: u64,
    /// Collections with pending deltas.
    pub collections: Vec<String>,
}

/// Storage namespace entry counts.
#[derive(Debug, Serialize)]
pub struct StorageCounts {
    pub meta: u64,
    pub vector: u64,
    pub graph: u64,
    pub crdt: u64,
    pub loro_state: u64,
}

/// Build and version info.
#[derive(Debug, Serialize)]
pub struct BuildInfo {
    pub version: &'static str,
    pub target: &'static str,
    pub profile: &'static str,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Generate a diagnostic dump suitable for bug reports.
    ///
    /// Contains NO user data, NO document contents, NO embeddings.
    /// Safe to send to support channels.
    pub async fn diagnostic_dump(&self) -> DiagnosticDump {
        let health = self.health();

        let pending_summary = {
            let crdt = self.crdt.lock_or_recover();
            let pending = crdt.pending_deltas();
            let total_bytes: usize = pending.iter().map(|d| d.delta_bytes.len()).sum();
            let oldest = pending.iter().map(|d| d.mutation_id).min().unwrap_or(0);
            let newest = pending.iter().map(|d| d.mutation_id).max().unwrap_or(0);
            let mut collections: Vec<String> = pending
                .iter()
                .map(|d| d.collection.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            collections.sort();

            PendingSummary {
                count: pending.len(),
                total_bytes,
                oldest_mutation_id: oldest,
                newest_mutation_id: newest,
                collections,
            }
        };

        let storage_counts = StorageCounts {
            meta: self
                .storage
                .count(nodedb_types::Namespace::Meta)
                .await
                .unwrap_or(0),
            vector: self
                .storage
                .count(nodedb_types::Namespace::Vector)
                .await
                .unwrap_or(0),
            graph: self
                .storage
                .count(nodedb_types::Namespace::Graph)
                .await
                .unwrap_or(0),
            crdt: self
                .storage
                .count(nodedb_types::Namespace::Crdt)
                .await
                .unwrap_or(0),
            loro_state: self
                .storage
                .count(nodedb_types::Namespace::LoroState)
                .await
                .unwrap_or(0),
        };

        let build_info = BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            target: std::env::consts::ARCH,
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        };

        DiagnosticDump {
            timestamp_ms: crate::runtime::now_millis(),
            health,
            pending_summary,
            storage_counts,
            build_info,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RedbStorage;

    async fn make_db() -> NodeDbLite<RedbStorage> {
        let storage = RedbStorage::open_in_memory().unwrap();
        NodeDbLite::open(storage, 1).await.unwrap()
    }

    #[tokio::test]
    async fn diagnostic_dump_empty_db() {
        let db = make_db().await;
        let dump = db.diagnostic_dump().await;

        assert_eq!(dump.pending_summary.count, 0);
        assert!(!dump.build_info.version.is_empty());
        assert!(dump.timestamp_ms > 0);

        // Should serialize to JSON cleanly.
        let json = sonic_rs::to_string_pretty(&dump).unwrap();
        assert!(json.contains("\"health\""));
        assert!(json.contains("\"pending_summary\""));
        assert!(json.contains("\"storage_counts\""));
    }

    #[tokio::test]
    async fn diagnostic_dump_with_pending() {
        use nodedb_client::NodeDb;
        use nodedb_types::document::Document;

        let db = make_db().await;
        let mut doc = Document::new("d1");
        doc.set("key", nodedb_types::Value::String("value".into()));
        db.document_put("test_coll", doc).await.unwrap();

        let dump = db.diagnostic_dump().await;
        assert!(dump.pending_summary.count > 0);
        assert!(
            dump.pending_summary
                .collections
                .contains(&"test_coll".to_string())
        );
        assert!(dump.pending_summary.oldest_mutation_id > 0);
    }
}
