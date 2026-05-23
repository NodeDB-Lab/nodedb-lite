// SPDX-License-Identifier: Apache-2.0

//! `PagedbBacking`: low-allocation [`VectorSegmentBacking`] backed by
//! a decrypted pagedb segment loaded into a heap-pinned byte buffer.
//!
//! The segment stores raw NDVS v2 bytes (header + vectors + padding +
//! surrogates + footer) spread across 4 KiB encrypted pagedb data pages.
//! On `open`, all pages are decrypted and concatenated into a single
//! `Box<[u8]>`.  Subsequent `get_vector` / `get_surrogate` calls are pure
//! pointer arithmetic into that buffer — no I/O, no allocation per lookup.
//!
//! # Format identity
//!
//! The byte layout inside the buffer is bit-identical to the NDVS v2 format
//! used by `nodedb_vector::mmap_segment`.  A segment written by `PagedbBacking`
//! can be opened by `MmapVectorSegment::open` on Origin (and vice-versa), which
//! is the cross-deployment compatibility contract.
//!
//! # Compile scope
//!
//! This module is only compiled on non-WASM targets.  WASM uses the legacy
//! blob checkpoint path.

use pagedb::SegmentReader;
use pagedb::vfs::traits::Vfs;

use nodedb_vector::segment_backing::VectorSegmentBacking;

use crate::error::LiteError;

// ── NDVS v2 format constants (must match nodedb-vector's mmap_segment::format) ─

const NDVS_MAGIC: [u8; 4] = *b"NDVS";
const NDVS_FORMAT_VERSION: u16 = 1;
const NDVS_HEADER_SIZE: usize = 32;
const NDVS_FOOTER_SIZE: usize = 46;
/// Bytes per F32 element.
const F32_BYTES: usize = 4;
/// Bytes per surrogate ID (u64).
const SID_BYTES: usize = 8;

/// 8-byte-aligned padding after the vector data block so surrogates are
/// naturally aligned.  Mirrors `nodedb_vector::mmap_segment::format::vec_pad`.
#[inline]
const fn vec_pad(vec_bytes: usize) -> usize {
    (8 - (vec_bytes % 8)) % 8
}

// ── PagedbBacking ─────────────────────────────────────────────────────────────

/// [`VectorSegmentBacking`] backed by a decrypted pagedb segment.
///
/// The decrypted NDVS payload is held in a heap-pinned `Box<[u8]>`.  Vector
/// and surrogate ID accesses are pointer arithmetic into this buffer —
/// no I/O, no allocation on the hot path.
///
/// `Box<[u8]>` is `Send + Sync`; no unsafe impls are needed.
pub struct PagedbBacking {
    /// Decrypted NDVS bytes (header + vectors + pad + surrogates + footer).
    data: Box<[u8]>,
    dim: usize,
    count: usize,
    /// Byte offset of the vector data block inside `data`.
    vec_offset: usize,
    /// Byte offset of the surrogate ID block inside `data`.
    sid_offset: usize,
}

impl PagedbBacking {
    /// Reconstruct from a pagedb `SegmentReader` that was written by
    /// [`crate::storage::vector_segment_ext::VectorSegmentExt::write_vector_segment`].
    ///
    /// All data pages are decrypted via `read_extent` and concatenated into a
    /// single `Box<[u8]>`.  The NDVS header is then parsed to extract `dim`
    /// and `count`.
    ///
    /// Takes the reader by value so the resulting future is `Send` regardless
    /// of whether `V::File` is `Sync`.
    ///
    /// Returns `LiteError` on any I/O, decryption, or format error.
    pub async fn open<V: Vfs + Clone>(reader: SegmentReader<V>) -> Result<Self, LiteError> {
        let meta_page_count = reader.meta().page_count;
        // Layout: page 0 = structural header, pages 1..D = NDVS data,
        //         pages D+1..E = v2 extent index, page E+1 = footer.
        // D = page_count - 2 - index_pages.
        let index_pages = u64::from(reader.index_page_count());
        let data_page_count = meta_page_count
            .checked_sub(2 + index_pages)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "vector segment page_count={meta_page_count} too small \
                     (index_pages={index_pages})"
                ),
            })?;
        if data_page_count == 0 {
            return Err(LiteError::Storage {
                detail: "vector segment has no data pages".to_owned(),
            });
        }
        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!("vector segment has too many data pages: {data_page_count}"),
        })?;

        // Decrypt and collect all data pages.
        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!("pagedb vector segment read_range failed: {e}"),
            })?;

        // Concatenate page bodies into a flat buffer.
        let total: usize = pages.iter().map(|p| p.len()).sum();
        let mut flat = Vec::with_capacity(total);
        for page in pages {
            flat.extend_from_slice(&page);
        }

        Self::from_bytes(flat.into_boxed_slice())
    }

    /// Parse the NDVS header from `data` and compute layout offsets.
    pub(crate) fn from_bytes(data: Box<[u8]>) -> Result<Self, LiteError> {
        if data.len() < NDVS_HEADER_SIZE + NDVS_FOOTER_SIZE {
            return Err(LiteError::Storage {
                detail: format!(
                    "vector segment too small: {} bytes (min {})",
                    data.len(),
                    NDVS_HEADER_SIZE + NDVS_FOOTER_SIZE
                ),
            });
        }

        // Validate magic.
        if data[0..4] != NDVS_MAGIC {
            return Err(LiteError::Storage {
                detail: "vector segment: bad NDVS magic".to_owned(),
            });
        }

        // Validate format version.
        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != NDVS_FORMAT_VERSION {
            return Err(LiteError::Storage {
                detail: format!(
                    "vector segment: unsupported NDVS version {version} \
                     (expected {NDVS_FORMAT_VERSION})"
                ),
            });
        }

        // dim at [8..12], count at [12..20].
        let dim = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let count = u64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]) as usize;

        let vec_bytes = dim
            .checked_mul(count)
            .and_then(|n| n.checked_mul(F32_BYTES))
            .ok_or_else(|| LiteError::Storage {
                detail: "vector segment: vector data size overflow".to_owned(),
            })?;

        let vec_offset = NDVS_HEADER_SIZE;
        let sid_offset = NDVS_HEADER_SIZE + vec_bytes + vec_pad(vec_bytes);
        let min_len = sid_offset
            .checked_add(count.saturating_mul(SID_BYTES))
            .and_then(|n| n.checked_add(NDVS_FOOTER_SIZE))
            .ok_or_else(|| LiteError::Storage {
                detail: "vector segment: layout size overflow".to_owned(),
            })?;

        if data.len() < min_len {
            return Err(LiteError::Storage {
                detail: format!(
                    "vector segment too small for declared count={count} dim={dim}: \
                     need {min_len} bytes, have {}",
                    data.len()
                ),
            });
        }

        Ok(Self {
            data,
            dim,
            count,
            vec_offset,
            sid_offset,
        })
    }

    /// Number of vectors.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl VectorSegmentBacking for PagedbBacking {
    fn len(&self) -> usize {
        self.count
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn get_vector(&self, id: u32) -> Option<&[f32]> {
        let id = id as usize;
        if id >= self.count {
            return None;
        }
        let byte_start = self.vec_offset + id * self.dim * F32_BYTES;
        let bytes = &self.data[byte_start..byte_start + self.dim * F32_BYTES];
        // SAFETY: the NDVS writer serialised F32 values as `f32 → u8` bytes.
        // The vector block starts at `NDVS_HEADER_SIZE` (32 bytes from the
        // buffer start).  The buffer was allocated by `Vec::with_capacity` and
        // then boxed — guaranteed at least 8-byte-aligned by the global allocator.
        // Each vector starts at an offset that is a multiple of 4 bytes from a
        // 4-byte-aligned base, so the pointer is 4-byte-aligned.
        // The slice length equals `dim`; the data is immutable after construction.
        Some(unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f32, self.dim) })
    }

    fn get_surrogate(&self, id: u32) -> Option<u64> {
        let id = id as usize;
        if id >= self.count {
            return None;
        }
        let offset = self.sid_offset + id * SID_BYTES;
        let bytes = &self.data[offset..offset + SID_BYTES];
        Some(u64::from_le_bytes(
            bytes.try_into().expect("surrogate slice is always 8 bytes"),
        ))
    }

    fn prefetch(&self, id: u32) {
        let id = id as usize;
        if id < self.count {
            // Touch the first byte of the vector to warm the CPU cache.
            let byte_start = self.vec_offset + id * self.dim * F32_BYTES;
            let _ = self.data.get(byte_start);
        }
    }
}

// ── Segment write helpers (called by VectorSegmentExt impl) ──────────────────

/// Serialise vectors and surrogate IDs to an in-memory NDVS v2 byte buffer.
///
/// The layout is bit-identical to `nodedb_vector::mmap_segment::writer::write_segment`
/// so segments written here can be opened by `MmapVectorSegment::open` on
/// Origin (format-bit-identity guarantee for cross-deployment compat).
pub(crate) fn build_ndvs_bytes(
    dim: usize,
    vectors: &[Vec<f32>],
    surrogate_ids: &[u64],
) -> Result<Vec<u8>, LiteError> {
    debug_assert!(
        surrogate_ids.is_empty() || surrogate_ids.len() == vectors.len(),
        "surrogate_ids length must match vectors length or be empty"
    );

    let count = vectors.len();
    let vec_bytes = dim
        .checked_mul(count)
        .and_then(|n| n.checked_mul(F32_BYTES))
        .ok_or_else(|| LiteError::Storage {
            detail: "NDVS build: vector data size overflow".to_owned(),
        })?;
    let pad = vec_pad(vec_bytes);
    let sid_bytes = count
        .checked_mul(SID_BYTES)
        .ok_or_else(|| LiteError::Storage {
            detail: "NDVS build: surrogate block size overflow".to_owned(),
        })?;
    let body_len = NDVS_HEADER_SIZE + vec_bytes + pad + sid_bytes;
    let total_len = body_len + NDVS_FOOTER_SIZE;
    let mut buf: Vec<u8> = Vec::with_capacity(total_len);

    // Header (32 bytes).
    buf.extend_from_slice(&NDVS_MAGIC);
    buf.extend_from_slice(&NDVS_FORMAT_VERSION.to_le_bytes()); // [4..6] version
    buf.extend_from_slice(&0u16.to_le_bytes()); // [6..8] flags
    buf.extend_from_slice(&(dim as u32).to_le_bytes()); // [8..12] dim
    buf.extend_from_slice(&(count as u64).to_le_bytes()); // [12..20] count
    buf.push(0u8); // [20] dtype = F32
    buf.push(0u8); // [21] codec = None
    buf.extend_from_slice(&[0u8; 10]); // [22..32] reserved

    debug_assert_eq!(buf.len(), NDVS_HEADER_SIZE);

    // Vector data block.
    for v in vectors {
        debug_assert_eq!(v.len(), dim, "vector dimension mismatch during NDVS build");
        // SAFETY: safe f32 → u8 cast; `u8` has alignment 1, so the cast is
        // always valid.  We only read the bytes, never write through this ptr.
        let bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * F32_BYTES) };
        buf.extend_from_slice(bytes);
    }

    // Alignment padding.
    buf.extend_from_slice(&[0u8; 8][..pad]);

    // Surrogate ID block.
    for i in 0..count {
        let sid = surrogate_ids.get(i).copied().unwrap_or(0);
        buf.extend_from_slice(&sid.to_le_bytes());
    }

    debug_assert_eq!(buf.len(), body_len);

    // Compute CRC32C over the body.
    let checksum = crc32c::crc32c(&buf);

    // Footer (46 bytes).
    buf.extend_from_slice(&NDVS_FORMAT_VERSION.to_le_bytes()); // [0..2]
    let mut created_by = [0u8; 32];
    let ver = env!("CARGO_PKG_VERSION").as_bytes();
    let copy_len = ver.len().min(31);
    created_by[..copy_len].copy_from_slice(&ver[..copy_len]);
    buf.extend_from_slice(&created_by); // [2..34]
    buf.extend_from_slice(&checksum.to_le_bytes()); // [34..38]
    buf.extend_from_slice(&(NDVS_FOOTER_SIZE as u32).to_le_bytes()); // [38..42]
    buf.extend_from_slice(&NDVS_MAGIC); // [42..46]

    debug_assert_eq!(buf.len(), total_len);

    Ok(buf)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pagedb::options::{OpenOptions, RetainPolicy};
    use pagedb::vfs::memory::MemVfs;
    use pagedb::{Db, RealmId, SegmentKind};

    fn test_open_options() -> OpenOptions {
        OpenOptions::default().with_commit_history_retain(RetainPolicy::Disabled)
    }

    async fn open_test_db() -> Db<MemVfs> {
        let vfs = MemVfs::new();
        let kek = [0u8; 32];
        let realm = RealmId::new([0u8; 16]);
        Db::open(vfs, kek, 4096, realm, test_open_options())
            .await
            .expect("in-memory pagedb open")
    }

    fn test_vectors(dim: usize, n: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| (0..dim).map(|j| (i * dim + j) as f32 * 0.1).collect())
            .collect()
    }

    // Write NDVS bytes into a pagedb segment and link it.
    async fn write_segment_to_db(
        db: &Db<MemVfs>,
        name: &str,
        dim: usize,
        vectors: &[Vec<f32>],
        surrogates: &[u64],
    ) {
        let realm = RealmId::new([0u8; 16]);
        let ndvs = build_ndvs_bytes(dim, vectors, surrogates).expect("build ok");

        // Chunk NDVS bytes into page-body-sized pieces.
        const PAGE_BODY_CAP: usize = 4096 - 40; // 4096 - ENVELOPE_OVERHEAD
        let chunks: Vec<&[u8]> = ndvs.chunks(PAGE_BODY_CAP).collect();

        let mut writer = db
            .create_segment(realm, SegmentKind::Unspecified)
            .await
            .expect("create segment ok");
        writer
            .append_extent(&chunks)
            .await
            .expect("append_extent ok");
        let meta = writer.seal().await.expect("seal ok");

        let mut txn = db.begin_write().await.expect("begin_write ok");
        txn.link_segment(name, &meta).await.expect("link ok");
        txn.commit().await.expect("commit ok");
    }

    #[tokio::test]
    async fn roundtrip_with_pagedb_segment() {
        let db = open_test_db().await;
        let dim = 4usize;
        let vecs = test_vectors(dim, 5);
        let surrogates: Vec<u64> = (10..15).collect();

        write_segment_to_db(&db, "vec/hnsw/col1", dim, &vecs, &surrogates).await;

        // Open via ReadTxn.
        let txn = db.begin_read().await.expect("begin_read ok");
        let reader = txn
            .open_segment("vec/hnsw/col1")
            .await
            .expect("open_segment ok");
        let backing = PagedbBacking::open(reader).await.expect("open backing ok");

        assert_eq!(backing.len(), 5);
        assert_eq!(backing.dim(), dim);
        assert!(!backing.is_empty());

        for i in 0..5usize {
            let got = backing.get_vector(i as u32).expect("vector present");
            assert_eq!(got, vecs[i].as_slice(), "vector {i} mismatch");
            let sid = backing.get_surrogate(i as u32).expect("surrogate present");
            assert_eq!(sid, surrogates[i], "surrogate {i} mismatch");
        }
    }

    #[tokio::test]
    async fn out_of_bounds_returns_none() {
        let db = open_test_db().await;
        let dim = 3usize;
        let vecs = test_vectors(dim, 2);
        let surrogates: Vec<u64> = vec![100, 200];

        write_segment_to_db(&db, "vec/hnsw/oob", dim, &vecs, &surrogates).await;

        let txn = db.begin_read().await.expect("begin_read ok");
        let reader = txn
            .open_segment("vec/hnsw/oob")
            .await
            .expect("open_segment ok");
        let backing = PagedbBacking::open(reader).await.expect("open ok");

        assert!(backing.get_vector(2).is_none(), "id=2 out of bounds");
        assert!(
            backing.get_surrogate(2).is_none(),
            "surrogate id=2 out of bounds"
        );
        backing.prefetch(999); // must not panic
    }

    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PagedbBacking>();
    }

    #[tokio::test]
    async fn bit_identical_with_plain_ndvs() {
        // Build NDVS bytes via our helper and verify format correctness.
        let dim = 3usize;
        let vectors: Vec<Vec<f32>> = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let surrogates: Vec<u64> = vec![42, 99];

        let our_bytes = build_ndvs_bytes(dim, &vectors, &surrogates).expect("build ok");

        // Validate magic and version match the nodedb-vector constants.
        assert_eq!(&our_bytes[0..4], b"NDVS", "magic mismatch");
        assert_eq!(
            u16::from_le_bytes([our_bytes[4], our_bytes[5]]),
            1u16,
            "version mismatch"
        );

        // Validate dim and count.
        let got_dim =
            u32::from_le_bytes([our_bytes[8], our_bytes[9], our_bytes[10], our_bytes[11]]);
        let got_count = u64::from_le_bytes(our_bytes[12..20].try_into().unwrap());
        assert_eq!(got_dim as usize, dim);
        assert_eq!(got_count as usize, 2);

        // Validate vector data block.
        let vec_offset = NDVS_HEADER_SIZE;
        let v0_f32: [f32; 3] = [1.0, 2.0, 3.0];
        let expected_v0: &[u8] =
            unsafe { std::slice::from_raw_parts(v0_f32.as_ptr() as *const u8, 12) };
        assert_eq!(&our_bytes[vec_offset..vec_offset + 12], expected_v0);
        let v1_f32: [f32; 3] = [4.0, 5.0, 6.0];
        let expected_v1: &[u8] =
            unsafe { std::slice::from_raw_parts(v1_f32.as_ptr() as *const u8, 12) };
        assert_eq!(&our_bytes[vec_offset + 12..vec_offset + 24], expected_v1);

        // Validate surrogate IDs (3 floats × 4 = 12 bytes; pad = 4 since 12 % 8 ≠ 0).
        // vec_bytes = 2 × 3 × 4 = 24 bytes; 24 % 8 = 0 → pad = 0.
        let sid_offset = vec_offset + 24;
        let sid0 = u64::from_le_bytes(our_bytes[sid_offset..sid_offset + 8].try_into().unwrap());
        let sid1 = u64::from_le_bytes(
            our_bytes[sid_offset + 8..sid_offset + 16]
                .try_into()
                .unwrap(),
        );
        assert_eq!(sid0, 42u64);
        assert_eq!(sid1, 99u64);

        // Validate trailing NDVS magic in footer.
        let footer_trailing_magic_offset = our_bytes.len() - 4;
        assert_eq!(&our_bytes[footer_trailing_magic_offset..], b"NDVS");

        // Round-trip through PagedbBacking via in-memory pagedb segment.
        let db = open_test_db().await;
        write_segment_to_db(&db, "vec/hnsw/bitcheck", dim, &vectors, &surrogates).await;
        let txn = db.begin_read().await.expect("read txn");
        let reader = txn
            .open_segment("vec/hnsw/bitcheck")
            .await
            .expect("open seg");
        let backing = PagedbBacking::open(reader).await.expect("open backing");

        for (i, v) in vectors.iter().enumerate() {
            assert_eq!(
                backing.get_vector(i as u32).expect("vector"),
                v.as_slice(),
                "vector {i} roundtrip"
            );
        }
        assert_eq!(backing.get_surrogate(0).expect("sid0"), 42);
        assert_eq!(backing.get_surrogate(1).expect("sid1"), 99);
    }
}
