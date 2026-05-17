// SPDX-License-Identifier: Apache-2.0
//! Sorted index (leaderboard) operations for the KV engine physical visitor.
//!
//! Implementation is split across sub-modules in `sorted/`:
//!   keys.rs    — key encoding helpers
//!   register.rs — DDL: register and drop
//!   query.rs   — read queries with lazy window purge
//!   window.rs  — window metadata persistence and purge logic

mod keys;
mod query;
mod register;
mod window;

pub use query::{
    kv_sorted_index_count, kv_sorted_index_range, kv_sorted_index_rank, kv_sorted_index_score,
    kv_sorted_index_top_k,
};
pub use register::{kv_drop_sorted_index, kv_register_sorted_index};

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::NodeDbLite;
    use crate::RedbStorage;

    async fn make_db() -> NodeDbLite<RedbStorage> {
        let storage = RedbStorage::open_in_memory().unwrap();
        NodeDbLite::open(storage, 1).await.unwrap()
    }

    /// Register a tumbling sorted index, write entries inside and outside the
    /// window, then query and verify out-of-window entries are not visible.
    #[test]
    fn tumbling_window_purges_expired_entries() {
        use super::keys::{SCORE_TS_SEPARATOR, f64_to_sort_bytes, pk_entry_key, score_prefix};
        use super::window::{WindowDef, purge_outside_window, store_window_def};
        use crate::storage::engine::{StorageEngineSync, WriteOp};
        use nodedb_types::Namespace;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let db = rt.block_on(make_db());
        let engine = &db.query_engine;

        // Define a tumbling window: [1000, 2000) ms.
        let def = WindowDef {
            window_type: "tumbling".to_string(),
            window_timestamp_column: "ts".to_string(),
            window_start_ms: 1000,
            window_end_ms: 2000,
        };
        store_window_def(engine, "test_idx", &def).unwrap();

        // Write two score entries: one inside the window (ts=1500), one outside (ts=500).
        let inside_ts: u64 = 1500;
        let outside_ts: u64 = 500;
        let score_bytes = f64_to_sort_bytes(42.0);
        let pk_in = b"pk_in";
        let pk_out = b"pk_out";

        // Build score key for inside entry: {prefix}{score:8}{SEP}{pk}{SEP}{ts:8}
        let pfx = score_prefix("test_idx");
        let mut in_key = pfx.as_bytes().to_vec();
        in_key.extend_from_slice(&score_bytes);
        in_key.push(SCORE_TS_SEPARATOR);
        in_key.extend_from_slice(pk_in);
        in_key.push(SCORE_TS_SEPARATOR);
        in_key.extend_from_slice(&inside_ts.to_le_bytes());

        let mut out_key = pfx.as_bytes().to_vec();
        out_key.extend_from_slice(&score_bytes);
        out_key.push(SCORE_TS_SEPARATOR);
        out_key.extend_from_slice(pk_out);
        out_key.push(SCORE_TS_SEPARATOR);
        out_key.extend_from_slice(&outside_ts.to_le_bytes());

        let ops = vec![
            WriteOp::Put {
                ns: Namespace::Meta,
                key: in_key.clone(),
                value: vec![],
            },
            WriteOp::Put {
                ns: Namespace::Meta,
                key: out_key.clone(),
                value: vec![],
            },
            WriteOp::Put {
                ns: Namespace::Meta,
                key: pk_entry_key("test_idx", pk_in),
                value: score_bytes.to_vec(),
            },
            WriteOp::Put {
                ns: Namespace::Meta,
                key: pk_entry_key("test_idx", pk_out),
                value: score_bytes.to_vec(),
            },
        ];
        engine.storage.batch_write_sync(&ops).unwrap();

        // Purge at now_ms=1500 (inside the window [1000, 2000)).
        purge_outside_window(engine, "test_idx", 1500).unwrap();

        // Inside entry must still exist.
        assert!(
            engine
                .storage
                .get_sync(Namespace::Meta, &in_key)
                .unwrap()
                .is_some(),
            "inside-window entry must survive purge"
        );

        // Outside entry must be gone.
        assert!(
            engine
                .storage
                .get_sync(Namespace::Meta, &out_key)
                .unwrap()
                .is_none(),
            "outside-window entry must be purged"
        );

        // Reverse pk entry for outside must also be gone.
        assert!(
            engine
                .storage
                .get_sync(Namespace::Meta, &pk_entry_key("test_idx", pk_out))
                .unwrap()
                .is_none(),
            "pk reverse entry for outside must be purged"
        );
    }

    /// Non-windowed index: purge_outside_window must be a no-op (no window def stored).
    #[test]
    fn non_windowed_purge_is_noop() {
        use super::keys::{SCORE_TS_SEPARATOR, f64_to_sort_bytes, score_prefix};
        use super::window::purge_outside_window;
        use crate::storage::engine::{StorageEngineSync, WriteOp};
        use nodedb_types::Namespace;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let db = rt.block_on(make_db());
        let engine = &db.query_engine;

        let score_bytes = f64_to_sort_bytes(10.0);
        let pfx = score_prefix("nw_idx");
        let mut key = pfx.as_bytes().to_vec();
        key.extend_from_slice(&score_bytes);
        key.push(SCORE_TS_SEPARATOR);
        key.extend_from_slice(b"pk1");

        engine
            .storage
            .batch_write_sync(&[WriteOp::Put {
                ns: Namespace::Meta,
                key: key.clone(),
                value: vec![],
            }])
            .unwrap();

        // No window def stored → purge is a no-op.
        purge_outside_window(engine, "nw_idx", 9999999).unwrap();

        assert!(
            engine
                .storage
                .get_sync(Namespace::Meta, &key)
                .unwrap()
                .is_some(),
            "non-windowed entry must not be purged"
        );
    }

    /// kv_register_sorted_index with window_type="tumbling" succeeds and persists
    /// a window definition that can be loaded back.
    #[tokio::test]
    async fn register_tumbling_index_persists_window_def() {
        use super::window::load_window_def;

        let db = make_db().await;
        let engine = &db.query_engine;

        super::kv_register_sorted_index(
            engine,
            "leaderboard",
            "tumbling",
            "event_ts",
            1_000_000,
            2_000_000,
        )
        .unwrap();

        let def = load_window_def(engine, "leaderboard").unwrap();
        assert!(def.is_some(), "window def must be persisted");
        let def = def.unwrap();
        assert_eq!(def.window_type, "tumbling");
        assert_eq!(def.window_start_ms, 1_000_000);
        assert_eq!(def.window_end_ms, 2_000_000);
    }

    /// kv_register_sorted_index with window_type="none" succeeds without storing a def.
    #[tokio::test]
    async fn register_none_window_no_def_stored() {
        use super::window::load_window_def;

        let db = make_db().await;
        let engine = &db.query_engine;

        super::kv_register_sorted_index(engine, "plain_idx", "none", "", 0, 0).unwrap();

        let def = load_window_def(engine, "plain_idx").unwrap();
        assert!(def.is_none(), "no window def for non-windowed index");
    }
}
