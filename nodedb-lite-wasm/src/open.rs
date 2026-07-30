// SPDX-License-Identifier: Apache-2.0

//! Constructors.
//!
//! Each one returns a database whose background flush and compaction tasks are
//! already running at the intervals its `LiteConfig` specifies — the JS caller
//! never has to start them.

use wasm_bindgen::prelude::*;

use nodedb_lite::storage::pagedb_storage::PagedbStorageMem;
use nodedb_lite::{LiteConfig, NodeDbLite};

#[cfg(all(target_arch = "wasm32", feature = "opfs"))]
use nodedb_lite::PagedbStorageOpfs;
// `Encryption` is only referenced by the OPFS persistent constructors below,
// which carry the same cfg; importing it unconditionally warns on other targets.
#[cfg(all(target_arch = "wasm32", feature = "opfs"))]
use nodedb_lite::storage::encryption::Encryption;

use crate::types::{NodeDbLiteWasm, NodeDbLiteWasmInner};

/// Largest accepted `memory_mb` override, in MiB (16 GiB).
///
/// A JS caller can pass any `u32`; values beyond what the browser/WASM heap can
/// ever back are clamped to this ceiling rather than producing a `LiteConfig`
/// that promises a budget the runtime cannot honour. 16 GiB comfortably exceeds
/// the wasm32 4 GiB address space while leaving an obvious sane upper bound.
const MAX_MEMORY_BUDGET_MB: u32 = 16 * 1024;

/// Build a [`LiteConfig`] from an optional `memory_mb` value.
///
/// `None` or `Some(0)` → default config (100 MiB).
/// `Some(mb)` → default config with `memory_budget` overridden to `mb` MiB,
/// clamped to [`MAX_MEMORY_BUDGET_MB`].
fn config_from_memory_mb(memory_mb: Option<u32>) -> LiteConfig {
    match memory_mb {
        // `saturating_mul` guards the byte computation: on wasm32 `usize` is
        // 32-bit, so even a ~4 GiB budget would overflow without it. The clamp
        // bounds the logical request; saturation bounds the arithmetic.
        Some(mb) if mb > 0 => LiteConfig {
            memory_budget: (mb.min(MAX_MEMORY_BUDGET_MB) as usize).saturating_mul(1024 * 1024),
            ..LiteConfig::default()
        },
        _ => LiteConfig::default(),
    }
}

#[wasm_bindgen]
impl NodeDbLiteWasm {
    // ─── Constructors — in-memory ──────────────────────────────────────────

    /// Create a new in-memory NodeDB-Lite database (no persistence).
    ///
    /// Memory budget is resolved from the default (100 MiB).
    #[wasm_bindgen(js_name = "openInMemory")]
    pub async fn open_in_memory(peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = NodeDbLite::open(storage, peer_id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self {
            inner: NodeDbLiteWasmInner::InMemory(db),
        })
    }

    /// Alias for `openInMemory` — retained for backwards compatibility.
    ///
    /// Memory budget is resolved from the default (100 MiB).
    #[wasm_bindgen]
    pub async fn open(peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        Self::open_in_memory(peer_id).await
    }

    /// Create a new in-memory NodeDB-Lite database with an explicit memory budget.
    ///
    /// `memory_mb` — total memory budget in mebibytes.
    /// Pass `None` (or `undefined` from JS) to use the default 100 MiB.
    #[wasm_bindgen(js_name = "openWithConfig")]
    pub async fn open_with_config(
        peer_id: u64,
        memory_mb: Option<u32>,
    ) -> Result<NodeDbLiteWasm, JsError> {
        let config = config_from_memory_mb(memory_mb);
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = NodeDbLite::open_with_config(storage, peer_id, config)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self {
            inner: NodeDbLiteWasmInner::InMemory(db),
        })
    }

    // ─── Constructors — persistent (OPFS) ─────────────────────────────────

    /// Create a persistent NodeDB-Lite database backed by OPFS.
    ///
    /// `worker_url` is the URL of the JS bootstrap script that calls
    /// `run_opfs_worker()`. See the module-level documentation for the
    /// required bootstrap file format.
    ///
    /// `passphrase` controls at-rest encryption of the OPFS database pages.
    /// OPFS storage is not encrypted by the browser itself, so a passphrase
    /// is strongly recommended. Pass an empty string to consciously opt out
    /// of encryption (all-zero page key; data is readable by anyone with
    /// OPFS origin access).
    ///
    /// A 16-byte random salt is persisted in an OPFS sidecar (`__nodedb_salt`)
    /// alongside the database on first open so the same passphrase reproduces
    /// the same key on every subsequent reopen.
    ///
    /// `filename` selects the OPFS sub-directory for this database. Every unique
    /// value is a fully isolated database instance in the shared OPFS origin;
    /// reopening with the same value reattaches the same data. It must be a
    /// single path segment (non-empty, no `/`, `\`, or NUL, not `.`/`..`).
    ///
    /// Data survives page reloads and browser restarts. Can be called from
    /// any execution context (the sync I/O runs inside the worker, not the
    /// caller).
    #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
    #[wasm_bindgen(js_name = "openPersistent")]
    pub async fn open_persistent(
        filename: &str,
        peer_id: u64,
        worker_url: &str,
        passphrase: String,
    ) -> Result<NodeDbLiteWasm, JsError> {
        let enc = if passphrase.is_empty() {
            Encryption::Plaintext
        } else {
            Encryption::passphrase(passphrase)
        };
        let storage = PagedbStorageOpfs::open_opfs(filename, worker_url, enc)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = NodeDbLite::open(storage, peer_id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self {
            inner: NodeDbLiteWasmInner::Persistent(db),
        })
    }

    /// Create a persistent OPFS-backed NodeDB-Lite database with an explicit
    /// memory budget.
    ///
    /// `passphrase` controls at-rest encryption. See `openPersistent` for the
    /// full encryption semantics. Pass an empty string to opt out.
    ///
    /// `filename` selects the OPFS sub-directory for this database — see
    /// `openPersistent` for the isolation and naming rules.
    ///
    /// `memory_mb` — total memory budget in mebibytes.
    /// Pass `None` (or `undefined` from JS) to use the default 100 MiB.
    #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
    #[wasm_bindgen(js_name = "openPersistentWithConfig")]
    pub async fn open_persistent_with_config(
        filename: &str,
        peer_id: u64,
        worker_url: &str,
        passphrase: String,
        memory_mb: Option<u32>,
    ) -> Result<NodeDbLiteWasm, JsError> {
        let enc = if passphrase.is_empty() {
            Encryption::Plaintext
        } else {
            Encryption::passphrase(passphrase)
        };
        let config = config_from_memory_mb(memory_mb);
        let storage = PagedbStorageOpfs::open_opfs(filename, worker_url, enc)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        let db = NodeDbLite::open_with_config(storage, peer_id, config)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self {
            inner: NodeDbLiteWasmInner::Persistent(db),
        })
    }
}
