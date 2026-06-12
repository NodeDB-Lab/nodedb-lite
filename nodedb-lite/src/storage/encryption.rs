//! Page-level encryption key management for `PagedbStorage`.
//!
//! Callers choose an `Encryption` variant when opening a persistent database.
//! The variant determines how the 32-byte key-encryption key (KEK) that pagedb
//! uses for AES-256-GCM page encryption is obtained.
//!
//! In-memory storage (`open_in_memory`) is volatile and does not use this
//! module — no at-rest encryption is meaningful there.

use crate::error::LiteError;

// ─── Public enum ─────────────────────────────────────────────────────────────

/// How the pagedb page-encryption key is obtained when opening a persistent
/// database.
///
/// No `Default` implementation is provided — the choice must be made
/// explicitly by the caller.
#[derive(Clone)]
pub enum Encryption {
    /// Explicit opt-out: data is written unencrypted (KEK = all-zero bytes).
    /// Must be chosen consciously; plaintext databases are readable by anyone
    /// with filesystem access.
    Plaintext,

    /// Derive the 32-byte pagedb KEK from a passphrase via Argon2id.
    ///
    /// A random 16-byte salt is persisted in a plaintext sidecar file next to
    /// the database (path `<db_path>.salt`) so the same passphrase reproduces
    /// the same key on every reopen. The sidecar is created on first open with
    /// mode 0o600 on Unix.
    Passphrase {
        passphrase: String,
        /// Argon2id memory cost in KiB (OWASP minimum: 19 456).
        m_cost: u32,
        /// Argon2id iteration count (OWASP minimum: 2).
        t_cost: u32,
        /// Argon2id parallelism lanes (OWASP minimum: 1).
        p_cost: u32,
    },

    /// Use a caller-supplied 32-byte key directly as the page-encryption key.
    ///
    /// No salt is stored; the caller owns key management entirely.
    RawKey([u8; 32]),
}

impl std::fmt::Debug for Encryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Encryption::Plaintext => write!(f, "Encryption::Plaintext"),
            Encryption::Passphrase { .. } => write!(f, "Encryption::Passphrase {{ .. }}"),
            Encryption::RawKey(..) => write!(f, "Encryption::RawKey(..)"),
        }
    }
}

impl Encryption {
    /// Construct a `Passphrase` variant using the OWASP-recommended Argon2id
    /// defaults: m_cost=19_456 KiB, t_cost=2, p_cost=1.
    pub fn passphrase(passphrase: impl Into<String>) -> Self {
        Encryption::Passphrase {
            passphrase: passphrase.into(),
            m_cost: 19_456,
            t_cost: 2,
            p_cost: 1,
        }
    }
}

// ─── Key derivation ───────────────────────────────────────────────────────────

/// Derive a 32-byte KEK from `passphrase` + `salt` via Argon2id.
pub(crate) fn derive_key(
    passphrase: &str,
    salt: &[u8; 16],
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> Result<[u8; 32], LiteError> {
    let mut key = [0u8; 32];
    let argon2 = argon2::Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        argon2::Params::new(m_cost, t_cost, p_cost, Some(32)).map_err(|e| {
            LiteError::Encryption {
                detail: format!("argon2 params invalid: {e}"),
            }
        })?,
    );
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|e| LiteError::Encryption {
            detail: format!("argon2 key derivation failed: {e}"),
        })?;
    Ok(key)
}

// ─── Native-only helpers (salt sidecar + KEK resolution) ─────────────────────

#[cfg(not(target_arch = "wasm32"))]
fn salt_sidecar_path(db_path: &std::path::Path) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.salt", db_path.display()))
}

#[cfg(not(target_arch = "wasm32"))]
fn load_or_create_salt(db_path: &std::path::Path) -> Result<[u8; 16], LiteError> {
    let sidecar = salt_sidecar_path(db_path);

    if sidecar.exists() {
        let bytes = std::fs::read(&sidecar).map_err(|e| LiteError::Encryption {
            detail: format!("failed to read salt sidecar {}: {e}", sidecar.display()),
        })?;
        if bytes.len() != 16 {
            return Err(LiteError::Encryption {
                detail: format!(
                    "salt sidecar {} has wrong length: expected 16, got {}",
                    sidecar.display(),
                    bytes.len()
                ),
            });
        }
        let mut salt = [0u8; 16];
        salt.copy_from_slice(&bytes);
        Ok(salt)
    } else {
        let mut salt = [0u8; 16];
        getrandom::fill(&mut salt).map_err(|e| LiteError::Encryption {
            detail: format!("getrandom failed for salt generation: {e}"),
        })?;
        std::fs::write(&sidecar, salt).map_err(|e| LiteError::Encryption {
            detail: format!("failed to write salt sidecar {}: {e}", sidecar.display()),
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o600)).map_err(
                |e| LiteError::Encryption {
                    detail: format!(
                        "failed to set permissions on salt sidecar {}: {e}",
                        sidecar.display()
                    ),
                },
            )?;
        }

        Ok(salt)
    }
}

/// Resolve the 32-byte pagedb KEK for a native (non-WASM) persistent database.
///
/// - `Encryption::Plaintext` returns an all-zero key (no encryption).
/// - `Encryption::RawKey(k)` returns `k` directly.
/// - `Encryption::Passphrase { .. }` loads or generates the `.salt` sidecar
///   adjacent to `db_path`, then runs Argon2id to derive the key.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn resolve_kek_native(
    enc: &Encryption,
    db_path: &std::path::Path,
) -> Result<[u8; 32], LiteError> {
    match enc {
        Encryption::Plaintext => Ok([0u8; 32]),
        Encryption::RawKey(k) => Ok(*k),
        Encryption::Passphrase {
            passphrase,
            m_cost,
            t_cost,
            p_cost,
        } => {
            let salt = load_or_create_salt(db_path)?;
            derive_key(passphrase, &salt, *m_cost, *t_cost, *p_cost)
        }
    }
}

// ─── WASM-only helpers (OPFS salt sidecar + KEK resolution) ─────────────────

/// Open (or create) the salt sidecar file at `salt_path` inside OPFS, read
/// or generate the 16-byte random salt, and return it.
///
/// If the file does not yet exist (or is shorter than 16 bytes) a fresh salt
/// is generated via `getrandom::fill`, written at offset 0, and flushed
/// before returning.
#[cfg(target_arch = "wasm32")]
pub(crate) async fn load_or_create_salt_opfs(
    vfs: &pagedb::vfs::opfs::OpfsVfs,
    salt_path: &str,
) -> Result<[u8; 16], LiteError> {
    use pagedb::vfs::traits::{Vfs, VfsFile};
    use pagedb::vfs::types::OpenMode;

    let mut file = vfs
        .open(salt_path, OpenMode::CreateOrOpen)
        .await
        .map_err(|e| LiteError::Encryption {
            detail: format!("failed to open OPFS salt sidecar '{salt_path}': {e}"),
        })?;

    let file_len = file.len().await.map_err(|e| LiteError::Encryption {
        detail: format!("failed to query length of OPFS salt sidecar '{salt_path}': {e}"),
    })?;

    if file_len >= 16 {
        let mut salt = [0u8; 16];
        file.read_at(0, &mut salt)
            .await
            .map_err(|e| LiteError::Encryption {
                detail: format!("failed to read OPFS salt sidecar '{salt_path}': {e}"),
            })?;
        return Ok(salt);
    }

    // Generate a fresh salt and persist it.
    let mut salt = [0u8; 16];
    getrandom::fill(&mut salt).map_err(|e| LiteError::Encryption {
        detail: format!("getrandom failed for OPFS salt generation: {e}"),
    })?;
    file.write_at(0, &salt)
        .await
        .map_err(|e| LiteError::Encryption {
            detail: format!("failed to write OPFS salt sidecar '{salt_path}': {e}"),
        })?;
    file.sync().await.map_err(|e| LiteError::Encryption {
        detail: format!("failed to flush OPFS salt sidecar '{salt_path}': {e}"),
    })?;

    Ok(salt)
}

/// Resolve the 32-byte pagedb KEK for an OPFS-backed persistent database.
///
/// - [`Encryption::Plaintext`] returns an all-zero key (no encryption).
/// - [`Encryption::RawKey(k)`] returns `k` directly.
/// - [`Encryption::Passphrase { .. }`] loads or generates a 16-byte random
///   salt persisted in an OPFS sidecar file at `__nodedb_salt` (adjacent to
///   the database root in the OPFS origin sandbox), then runs Argon2id to
///   derive the key.
///
/// `vfs` is used only for salt I/O; pass a clone so the caller can forward
/// the original into `Db::open`.
#[cfg(target_arch = "wasm32")]
pub(crate) async fn resolve_kek_opfs(
    enc: &Encryption,
    vfs: &pagedb::vfs::opfs::OpfsVfs,
) -> Result<[u8; 32], LiteError> {
    match enc {
        Encryption::Plaintext => Ok([0u8; 32]),
        Encryption::RawKey(k) => Ok(*k),
        Encryption::Passphrase {
            passphrase,
            m_cost,
            t_cost,
            p_cost,
        } => {
            let salt = load_or_create_salt_opfs(vfs, "__nodedb_salt").await?;
            derive_key(passphrase, &salt, *m_cost, *t_cost, *p_cost)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_passphrase_and_salt_derives_same_key() {
        let salt = [0x42u8; 16];
        let k1 = derive_key("hunter2", &salt, 8, 1, 1).unwrap();
        let k2 = derive_key("hunter2", &salt, 8, 1, 1).unwrap();
        assert_eq!(k1, k2);
    }

    #[test]
    fn different_salt_derives_different_key() {
        let salt_a = [0x01u8; 16];
        let salt_b = [0x02u8; 16];
        let k1 = derive_key("same-pass", &salt_a, 8, 1, 1).unwrap();
        let k2 = derive_key("same-pass", &salt_b, 8, 1, 1).unwrap();
        assert_ne!(k1, k2);
    }

    #[test]
    fn plaintext_resolves_to_zero_key() {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("dummy.pagedb");
            let kek = resolve_kek_native(&Encryption::Plaintext, &path).unwrap();
            assert_eq!(kek, [0u8; 32]);
        }
    }

    #[test]
    fn raw_key_resolves_directly() {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("dummy.pagedb");
            let raw = [0xABu8; 32];
            let kek = resolve_kek_native(&Encryption::RawKey(raw), &path).unwrap();
            assert_eq!(kek, raw);
        }
    }

    #[test]
    fn debug_does_not_leak_secrets() {
        let passphrase_variant = Encryption::passphrase("my-secret-pass");
        let debug_str = format!("{passphrase_variant:?}");
        assert!(
            !debug_str.contains("my-secret-pass"),
            "passphrase leaked in Debug"
        );
        assert!(debug_str.contains("Passphrase"));

        let raw_variant = Encryption::RawKey([0xDE; 32]);
        let debug_str = format!("{raw_variant:?}");
        assert!(!debug_str.contains("222"), "raw key bytes leaked in Debug");
        assert!(debug_str.contains("RawKey"));

        let plain = Encryption::Plaintext;
        let debug_str = format!("{plain:?}");
        assert!(debug_str.contains("Plaintext"));
    }
}
