// SPDX-License-Identifier: Apache-2.0
//! Key-encoding helpers shared across the sorted-index sub-modules.

/// Separator byte used between pk and timestamp in windowed score entry keys.
/// Value 0x1F (ASCII unit separator) is unlikely to appear in raw pk bytes and
/// is never a valid UTF-8 continuation byte.
pub(super) const SCORE_TS_SEPARATOR: u8 = 0x1F;

pub(super) fn score_prefix(index_name: &str) -> String {
    format!("kv_sorted:{index_name}:score:")
}

pub(super) fn pk_entry_key(index_name: &str, pk: &[u8]) -> Vec<u8> {
    let mut k = format!("kv_sorted:{index_name}:pk:").into_bytes();
    k.extend_from_slice(pk);
    k
}

/// Encode a score as a big-endian `[u8; 8]` such that lexicographic order
/// matches ascending numeric order for positive f64 values, and descending
/// indexes store a bitwise-NOT of that.
#[allow(dead_code)]
pub(super) fn f64_to_sort_bytes(score: f64) -> [u8; 8] {
    let bits = score.to_bits();
    // If positive (sign bit 0): flip sign bit so positive > negative lexicographically.
    // If negative (sign bit 1): flip all bits so more-negative < less-negative.
    let key_bits = if bits >> 63 == 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    key_bits.to_be_bytes()
}

pub(super) fn sort_bytes_to_f64(bytes: &[u8; 8]) -> f64 {
    let bits = u64::from_be_bytes(*bytes);
    let original = if bits >> 63 != 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    f64::from_bits(original)
}
