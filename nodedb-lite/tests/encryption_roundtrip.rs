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

/// Write `top-secret` under the `right-key` passphrase and close.
async fn seed_encrypted_store(path: &std::path::Path) {
    let storage = PagedbStorageDefault::open(path, Encryption::passphrase("right-key"))
        .await
        .expect("open encrypted");
    let db = NodeDbLite::open(storage, 1).await.expect("open db");
    db.kv_put("col", "key", b"top-secret")
        .await
        .expect("kv_put");
    db.kv_flush().await.expect("kv_flush");
}

/// Opening with a DIFFERENT passphrase must fail rather than yield a store.
///
/// The wrong KEK cannot authenticate the database header, and the open is
/// refused. That refusal *is* the confidentiality guarantee: there is no
/// handle to read through, so the ciphertext cannot be surfaced as plaintext.
#[tokio::test]
async fn wrong_passphrase_does_not_reveal_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("enc_wrong.pagedb");
    seed_encrypted_store(&path).await;

    let opened = PagedbStorageDefault::open(&path, Encryption::passphrase("WRONG-key")).await;

    assert!(
        opened.is_err(),
        "a store must not open under a passphrase that cannot authenticate its header"
    );
}

/// A failed unlock must leave the store intact and still openable.
///
/// A wrong passphrase is overwhelmingly a typo, not corruption, so the failed
/// attempt must not treat the database as damaged — renaming it aside and
/// starting fresh would silently destroy the user's data on a mistyped key.
/// The proof is that the correct passphrase still opens it afterwards and the
/// original value is unchanged.
#[tokio::test]
async fn a_failed_unlock_leaves_the_store_intact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("enc_retry.pagedb");
    seed_encrypted_store(&path).await;

    let failed = PagedbStorageDefault::open(&path, Encryption::passphrase("WRONG-key")).await;
    assert!(
        failed.is_err(),
        "the wrong passphrase must not open the store"
    );
    drop(failed);

    let storage = PagedbStorageDefault::open(&path, Encryption::passphrase("right-key"))
        .await
        .expect("the correct passphrase must still open the store after a failed attempt");
    let db = NodeDbLite::open(storage, 1).await.expect("reopen db");
    let got = db.kv_get("col", "key").await.expect("kv_get");
    assert_eq!(
        got.as_deref(),
        Some(b"top-secret".as_slice()),
        "a failed unlock must not discard or alter the stored data"
    );
}
