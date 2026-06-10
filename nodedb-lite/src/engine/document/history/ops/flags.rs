// SPDX-License-Identifier: Apache-2.0

//! The collection-level bitemporal flag, persisted in `Namespace::Meta`.

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Meta key prefix for the document bitemporal flag.
const META_DOCUMENT_BITEMPORAL_PREFIX: &str = "document_bitemporal:";

/// Query whether a document collection has bitemporal tracking enabled.
///
/// Returns `false` for any collection that has not had the flag explicitly set.
pub async fn is_bitemporal<S: StorageEngine>(
    storage: &S,
    collection: &str,
) -> Result<bool, LiteError> {
    let key = format!("{META_DOCUMENT_BITEMPORAL_PREFIX}{collection}");
    Ok(storage
        .get(Namespace::Meta, key.as_bytes())
        .await?
        .map(|v| v.first().copied() == Some(1))
        .unwrap_or(false))
}

/// Mark a document collection as bitemporal (or non-bitemporal). Idempotent.
pub async fn set_bitemporal<S: StorageEngine>(
    storage: &S,
    collection: &str,
    bitemporal: bool,
) -> Result<(), LiteError> {
    let key = format!("{META_DOCUMENT_BITEMPORAL_PREFIX}{collection}");
    storage
        .put(Namespace::Meta, key.as_bytes(), &[bitemporal as u8])
        .await
}

#[cfg(test)]
mod tests {
    use crate::storage::pagedb_storage::PagedbStorageMem;

    use super::*;

    async fn mem_storage() -> PagedbStorageMem {
        PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage")
    }

    #[tokio::test]
    async fn flag_default_false() {
        let s = mem_storage().await;
        assert!(!is_bitemporal(&s, "coll").await.unwrap());
    }

    #[tokio::test]
    async fn flag_roundtrip() {
        let s = mem_storage().await;
        set_bitemporal(&s, "coll", true).await.unwrap();
        assert!(is_bitemporal(&s, "coll").await.unwrap());
        set_bitemporal(&s, "coll", false).await.unwrap();
        assert!(!is_bitemporal(&s, "coll").await.unwrap());
    }
}
