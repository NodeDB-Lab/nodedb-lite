// SPDX-License-Identifier: Apache-2.0

//! Composite key encoding: `[namespace_byte, ...key_bytes]`.
//!
//! Namespacing is achieved by prefixing every key with a single namespace
//! byte, identical to the original key-encoding convention (namespace byte
//! first). B+ tree order is preserved because all keys within a namespace
//! share the same leading byte and are sorted lexicographically among
//! themselves.

use nodedb_types::Namespace;

/// Build a composite key: `[namespace_byte, ...key_bytes]`.
///
/// Prepends the namespace byte. The namespace byte is always the first
/// byte; B+ tree order is preserved because all keys within a namespace share
/// the same leading byte and are sorted lexicographically among themselves.
pub(crate) fn prefix_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + key.len());
    k.push(ns as u8);
    k.extend_from_slice(key);
    k
}

/// Inline-stack composite key for hot read paths. Avoids heap allocation
/// when the prefixed key fits in 64 bytes (typical Lite KV keys are
/// `{ns_byte}{collection}\0{user_key}` ~ a few dozen bytes).
pub(crate) enum KeyBuf {
    Stack { data: [u8; 64], len: usize },
    Heap(Vec<u8>),
}

impl KeyBuf {
    #[inline]
    pub(crate) fn new(ns: Namespace, key: &[u8]) -> Self {
        let total = 1 + key.len();
        if total <= 64 {
            let mut data = [0u8; 64];
            data[0] = ns as u8;
            data[1..total].copy_from_slice(key);
            KeyBuf::Stack { data, len: total }
        } else {
            let mut v = Vec::with_capacity(total);
            v.push(ns as u8);
            v.extend_from_slice(key);
            KeyBuf::Heap(v)
        }
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            KeyBuf::Stack { data, len } => &data[..*len],
            KeyBuf::Heap(v) => v.as_slice(),
        }
    }
}

/// Strip the namespace prefix byte from a composite key returned by pagedb.
///
/// Returns an empty slice if `composite` has length ≤ 1 (defensive).
pub(crate) fn strip_prefix(composite: &[u8]) -> &[u8] {
    if composite.len() > 1 {
        &composite[1..]
    } else {
        &[]
    }
}

/// Exclusive end marker for namespace `n`: the first key that is strictly
/// greater than any key in namespace `n`.
///
/// For `n < 0xFF` this is `[n+1]` (one-byte boundary). `n == 0xFF` is not
/// assigned to any `Namespace` variant today and would require a two-byte
/// sentinel (`[0xFF, 0x00, ...]`). We assert this is unreachable to surface
/// any future `Namespace` addition that would violate the assumption.
pub(crate) fn ns_end(ns: Namespace) -> Vec<u8> {
    let b = ns as u8;
    assert!(
        b < 0xFF,
        "Namespace byte 0xFF would overflow the single-byte end-marker; \
         add a two-byte sentinel before assigning Namespace values in the 0xFF range"
    );
    vec![b + 1]
}
