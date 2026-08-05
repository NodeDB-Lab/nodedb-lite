// SPDX-License-Identifier: Apache-2.0

//! Native constructors: open an on-disk store, and the opt-in discard path.

use std::path::Path;
use std::sync::Arc;

use pagedb::vfs::DefaultVfs;
use pagedb::{Db, RealmId};

use crate::error::LiteError;
use crate::storage::corruption::CorruptionPolicy;
use crate::storage::encryption::Encryption;
use crate::storage::pagedb_storage::errors::is_corruption;
use crate::storage::pagedb_storage::types::{PagedbStorage, lite_open_options};

const DEFAULT_PAGE_SIZE: usize = 4096;

impl PagedbStorage<DefaultVfs> {
    /// Open or create a database at `path` using the platform-native async VFS.
    ///
    /// A store that cannot be read is reported, not repaired: this is
    /// [`CorruptionPolicy::FailClosed`]. Use
    /// [`open_with_policy`](Self::open_with_policy) to choose otherwise.
    ///
    /// `encryption` controls how the 32-byte pagedb page-encryption key is
    /// obtained:
    ///
    /// - [`Encryption::Plaintext`] — no encryption; the all-zero key is used.
    ///   Must be chosen consciously.
    /// - [`Encryption::Passphrase`] — derives the key via Argon2id using a
    ///   random 16-byte salt. The salt is persisted in a plaintext sidecar file
    ///   at `<path>.salt` (created on first open, mode 0o600 on Unix) so that
    ///   the same passphrase reproduces the same key on every reopen.
    /// - [`Encryption::RawKey`] — uses the supplied 32-byte key directly; the
    ///   caller is responsible for key management and no sidecar is written.
    pub async fn open(path: impl AsRef<Path>, encryption: Encryption) -> Result<Self, LiteError> {
        Self::open_with_policy(path, encryption, CorruptionPolicy::FailClosed).await
    }

    /// Open or create a database at `path`, choosing what happens if the store
    /// on disk cannot be read.
    ///
    /// Under [`CorruptionPolicy::FailClosed`] the corruption is returned and
    /// the store is left untouched. Under
    /// [`CorruptionPolicy::DiscardStoreAndRecreate`] the damaged store is
    /// renamed aside and a fresh, empty database takes its place — see that
    /// variant's documentation for what the caller is agreeing to.
    pub async fn open_with_policy(
        path: impl AsRef<Path>,
        encryption: Encryption,
        policy: CorruptionPolicy,
    ) -> Result<Self, LiteError> {
        Self::open_with_policy_and_page_size(path, encryption, policy, DEFAULT_PAGE_SIZE).await
    }

    /// Open or create a database with an explicit durable page size.
    ///
    /// The supplied size is part of the persistent format and must match on
    /// reopen. It must satisfy PageDB's supported page-size constraints.
    pub async fn open_with_policy_and_page_size(
        path: impl AsRef<Path>,
        encryption: Encryption,
        policy: CorruptionPolicy,
        page_size: usize,
    ) -> Result<Self, LiteError> {
        let path = path.as_ref();
        let kek = crate::storage::encryption::resolve_kek_native(&encryption, path)?;
        let realm = RealmId::new([0u8; 16]);

        let vfs = pagedb::vfs::open_default(path).map_err(LiteError::from)?;

        match Db::open(vfs, kek, page_size, realm, lite_open_options()).await {
            Ok(db) => Ok(Self {
                db: Arc::new(db),
                page_size,
            }),
            Err(e) if is_corruption(&e) && path.exists() => {
                if !policy.may_discard() {
                    tracing::error!(
                        path = %path.display(),
                        error = %e,
                        "pagedb open detected corruption — refusing to open. The store has \
                         been left exactly as it was found."
                    );
                    return Err(LiteError::from(e));
                }
                tracing::error!(
                    path = %path.display(),
                    error = %e,
                    "pagedb open detected corruption — the caller opted into discarding the \
                     store (rename aside, recreate fresh). The new database is empty."
                );
                Self::discard_and_recreate_with_page_size(path, &encryption, page_size).await
            }
            Err(e) => Err(LiteError::from(e)),
        }
    }

    /// Rename a damaged database aside and recreate a fresh one at `path`.
    ///
    /// The damaged store is renamed to `{path}.corrupt.{unix_secs}` (never
    /// deleted, so the bytes remain available for offline forensics) and a
    /// fresh database is created at `path` using the same `encryption`.
    ///
    /// Only reachable when the caller selected
    /// [`CorruptionPolicy::DiscardStoreAndRecreate`]; the open paths call it
    /// after checking, so the destructive step has exactly one gate in front of
    /// it and exactly one implementation behind it.
    pub(crate) async fn discard_and_recreate_with_page_size(
        path: &Path,
        encryption: &Encryption,
        page_size: usize,
    ) -> Result<Self, LiteError> {
        let kek = crate::storage::encryption::resolve_kek_native(encryption, path)?;
        let realm = RealmId::new([0u8; 16]);

        let timestamp = crate::runtime::now_secs();
        let corrupt_path = path.with_extension(format!("corrupt.{timestamp}"));

        tracing::error!(
            path = %path.display(),
            corrupt_backup = %corrupt_path.display(),
            "renaming corrupted pagedb store to backup and creating a fresh database"
        );

        if let Err(rename_err) = std::fs::rename(path, &corrupt_path) {
            tracing::error!(error = %rename_err, "failed to rename corrupted pagedb directory");
            return Err(LiteError::Storage {
                detail: format!("pagedb corrupted and rename failed: rename={rename_err}"),
            });
        }

        let vfs = pagedb::vfs::open_default(path).map_err(LiteError::from)?;
        let db = Db::open(vfs, kek, page_size, realm, lite_open_options())
            .await
            .map_err(|e2| LiteError::Storage {
                detail: format!(
                    "pagedb corrupted, backup saved to {}, fresh create failed: {e2}",
                    corrupt_path.display()
                ),
            })?;
        Ok(Self {
            db: Arc::new(db),
            page_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Namespace;

    use super::*;
    use crate::storage::engine::StorageEngine;

    #[tokio::test]
    async fn explicit_page_size_reopens_and_rejects_a_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sized.pagedb");
        let storage = PagedbStorage::<DefaultVfs>::open_with_policy_and_page_size(
            &path,
            Encryption::Plaintext,
            CorruptionPolicy::FailClosed,
            16 * 1024,
        )
        .await
        .unwrap();
        storage
            .put(Namespace::Vector, b"key", b"value")
            .await
            .unwrap();
        drop(storage);

        let reopened = PagedbStorage::<DefaultVfs>::open_with_policy_and_page_size(
            &path,
            Encryption::Plaintext,
            CorruptionPolicy::FailClosed,
            16 * 1024,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.get(Namespace::Vector, b"key").await.unwrap(),
            Some(b"value".to_vec())
        );
        drop(reopened);

        let error = match PagedbStorage::<DefaultVfs>::open_with_policy_and_page_size(
            &path,
            Encryption::Plaintext,
            CorruptionPolicy::FailClosed,
            4096,
        )
        .await
        {
            Ok(_) => panic!("mismatched page size must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("page size"));
    }
}
