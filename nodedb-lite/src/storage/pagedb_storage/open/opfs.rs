// SPDX-License-Identifier: Apache-2.0

//! Browser OPFS constructor (wasm32 only).

use std::sync::Arc;

use pagedb::{Db, PagedbError, RealmId};

use crate::error::LiteError;
use crate::storage::encryption::Encryption;
use crate::storage::pagedb_storage::types::{PagedbStorage, lite_open_options};

/// Validate an OPFS database name before it is used as the VFS root directory.
///
/// The name becomes a single OPFS directory segment, so it must be non-empty,
/// free of path separators and NUL, and must not be a relative-traversal
/// segment. Rejecting here yields a clear error instead of an opaque worker
/// failure (OPFS `getDirectoryHandle` rejects `.`/`..` with a `TypeError`).
fn validate_opfs_db_name(name: &str) -> Result<(), LiteError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(LiteError::BadRequest {
            detail: format!(
                "invalid OPFS database name {name:?}: must be a non-empty single path \
                 segment without '/', '\\', or NUL and not '.' or '..'"
            ),
        });
    }
    Ok(())
}

impl PagedbStorage<pagedb::vfs::opfs::OpfsVfs> {
    /// Open or create a persistent database backed by the browser's Origin
    /// Private File System (OPFS).
    ///
    /// `db_name` selects an OPFS sub-directory that scopes every file this
    /// database touches (`main.db`, segments, locks, the salt sidecar). Distinct
    /// names are fully isolated databases in the shared OPFS origin; reopening
    /// with the same name reattaches the same database. It must be a single path
    /// segment — non-empty, no `/`, `\`, or NUL, and not `.`/`..`.
    ///
    /// `worker_url` is the URL of the JS bootstrap script that calls
    /// `run_opfs_worker()` inside a dedicated Web Worker. The embedder
    /// (nodedb-lite-wasm) must export that function and serve the bootstrap
    /// script at a URL the browser can load.
    ///
    /// `encryption` controls how the 32-byte pagedb page-encryption key is
    /// obtained:
    ///
    /// - [`Encryption::Plaintext`] — no encryption; the all-zero key is used.
    ///   Must be chosen consciously; OPFS storage is not encrypted by the
    ///   browser itself, so a passphrase is strongly recommended.
    /// - [`Encryption::Passphrase`] — derives the key via Argon2id. A random
    ///   16-byte salt is persisted in an OPFS sidecar file at
    ///   `__nodedb_salt` (in the same OPFS origin sandbox as the database)
    ///   so the same passphrase reproduces the same key on every reopen.
    /// - [`Encryption::RawKey`] — uses the supplied 32-byte key directly;
    ///   the caller is responsible for key management and no sidecar is
    ///   written.
    ///
    /// # Corruption
    ///
    /// A damaged store is reported and left alone, the same guarantee the
    /// native open gives under [`CorruptionPolicy::FailClosed`]. There is no
    /// discard-and-recreate variant here: OPFS has no `rename`, so the
    /// "renamed aside, never deleted" property that makes discarding survivable
    /// cannot be provided, and discarding without it would be unrecoverable.
    ///
    /// [`CorruptionPolicy::FailClosed`]: crate::storage::corruption::CorruptionPolicy::FailClosed
    pub async fn open_opfs(
        db_name: &str,
        worker_url: &str,
        encryption: Encryption,
    ) -> Result<Self, LiteError> {
        validate_opfs_db_name(db_name)?;

        let realm = RealmId::new([0u8; 16]);

        let vfs = pagedb::vfs::opfs::OpfsVfs::with_root(worker_url, db_name).map_err(|e| {
            LiteError::WorkerFailed {
                detail: format!("failed to spawn OPFS worker at '{worker_url}': {e}"),
            }
        })?;

        // Resolve the KEK using a clone of the VFS so the original can be
        // forwarded into Db::open below. OpfsVfs::clone is cheap (Arc clone).
        let kek = crate::storage::encryption::resolve_kek_opfs(&encryption, &vfs.clone()).await?;

        let db = Db::open(vfs, kek, 4096, realm, lite_open_options())
            .await
            .map_err(|e| match e {
                // Typed as corruption rather than a worker failure so the
                // caller matches the same way it would on native.
                PagedbError::Corruption(_) | PagedbError::ChecksumFailure => LiteError::Corrupted {
                    detail: format!(
                        "OPFS database is corrupted and has been left untouched. Recovery \
                             is the caller's decision: re-sync from Origin into a new database \
                             name, or delete the OPFS directory to start empty. Original \
                             error: {e}"
                    ),
                },
                other => LiteError::from(other),
            })?;

        Ok(Self {
            db: Arc::new(db),
            page_size: 4096,
        })
    }
}
