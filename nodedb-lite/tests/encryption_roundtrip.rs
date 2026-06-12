//! At-rest encryption round-trip: passphrase-derived KEK persists data across
//! reopen, and the salt sidecar makes the same passphrase reproduce the key.

use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault};

/// Data written under a passphrase survives a close/reopen with the SAME
/// passphrase, and a plaintext `.salt` sidecar is created next to the database.
#[tokio::test]
async fn encrypted_value_survives_reopen_with_same_passphrase() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("enc.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::passphrase("correct horse"))
            .await
            .expect("open encrypted");
        let db = NodeDbLite::open(storage, 1).await.expect("open db");
        db.kv_put("col", "key", b"secret-value")
            .await
            .expect("kv_put");
        db.kv_flush().await.expect("kv_flush");
    }

    // Salt sidecar must exist and be exactly 16 bytes.
    let salt_path = format!("{}.salt", path.display());
    let salt = std::fs::read(&salt_path).expect("salt sidecar exists");
    assert_eq!(salt.len(), 16, "salt sidecar must be 16 bytes");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::passphrase("correct horse"))
            .await
            .expect("reopen encrypted");
        let db = NodeDbLite::open(storage, 1).await.expect("reopen db");
        let got = db.kv_get("col", "key").await.expect("kv_get");
        assert_eq!(
            got.as_deref(),
            Some(b"secret-value".as_slice()),
            "value must survive reopen under the same passphrase"
        );
    }
}

/// Reopening with a DIFFERENT passphrase must not surface the original data.
/// The wrong KEK fails page authentication; the native recovery path renames
/// the unreadable database aside and starts fresh, so the secret value is not
/// readable under the wrong key.
#[tokio::test]
async fn wrong_passphrase_does_not_reveal_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("enc_wrong.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::passphrase("right-key"))
            .await
            .expect("open encrypted");
        let db = NodeDbLite::open(storage, 1).await.expect("open db");
        db.kv_put("col", "key", b"top-secret")
            .await
            .expect("kv_put");
        db.kv_flush().await.expect("kv_flush");
    }

    // Reopen with the wrong passphrase: the original plaintext must never be
    // returned. (Recovery may yield a fresh empty store rather than an error.)
    let storage = PagedbStorageDefault::open(&path, Encryption::passphrase("WRONG-key"))
        .await
        .expect("reopen attempt");
    let db = NodeDbLite::open(storage, 1).await.expect("open db");
    let got = db.kv_get("col", "key").await.expect("kv_get");
    assert_ne!(
        got.as_deref(),
        Some(b"top-secret".as_slice()),
        "the secret value must not be readable under a different passphrase"
    );
}
