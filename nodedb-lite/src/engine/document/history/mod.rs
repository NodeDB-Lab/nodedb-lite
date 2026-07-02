// SPDX-License-Identifier: Apache-2.0

//! Bitemporal history storage for schemaless document collections.
//!
//! When a document collection is created with `bitemporal=true`, every
//! document mutation writes a versioned record to `Namespace::DocumentHistory`.
//!
//! Key layout:  `{collection}:{doc_id}\x00{system_from_ms:020}`
//! Value layout: `[tag:u8][valid_from_ms:i64 LE][valid_until_ms:i64 LE][body_msgpack...]`
//!
//! The 20-digit zero-padded decimal `system_from_ms` gives lexicographic
//! ordering that matches temporal ordering, so the most-recent version of a
//! document is the last key under its prefix.
//!
//! `\x00` is the reserved version separator; doc_ids containing a NUL byte
//! are rejected at write time.
//!
//! The collection-level bitemporal flag is persisted in `Namespace::Meta`
//! under key `document_bitemporal:{collection}` (1 byte: 0x00 = false, 0x01 = true).

pub mod key;
pub mod ops;
pub mod value;
