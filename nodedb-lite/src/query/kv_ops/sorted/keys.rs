// SPDX-License-Identifier: Apache-2.0
//! Storage keys and values of sorted-index entries, all in `Namespace::Meta`.
//!
//! Every entry key of one index starts with
//! `kv_sorted: | u32 len | index name | tag`. The length prefix makes the
//! prefix of one index a prefix of no other index's keys, whatever bytes the
//! names hold.
//!
//! - Score entry, tag `s`: `{score:8}{pk}`, or `{score:8}{ts:8}{pk}` for a
//!   windowed index. Every field before the pk is fixed-width, so the pk is
//!   the remainder and keys sort by score, then timestamp, then pk. The
//!   value is the row's expiry deadline, u64 little-endian, 0 for none.
//! - Pk entry, tag `p`: `{pk}`. The value is a [`PkEntry`]: what the row's
//!   score entry holds, so a write finds the entry it replaces.

/// First bytes of every sorted-index entry key.
const ROOT: &[u8] = b"kv_sorted:";
const SCORE: u8 = b's';
const PK: u8 = b'p';

/// Width of an encoded score and of an encoded timestamp.
pub(super) const FIELD_LEN: usize = 8;

/// Prefix of every entry of the index `index_name`.
pub(crate) fn entries_prefix(index_name: &str) -> Vec<u8> {
    // A name longer than u32::MAX bytes cannot be an index name: names come
    // from SQL identifiers.
    let len = u32::try_from(index_name.len()).unwrap_or(u32::MAX);
    let mut key = Vec::with_capacity(ROOT.len() + 4 + index_name.len() + 1);
    key.extend_from_slice(ROOT);
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(index_name.as_bytes());
    key
}

/// Prefix of every score entry of the index `index_name`.
pub(super) fn score_prefix(index_name: &str) -> Vec<u8> {
    let mut key = entries_prefix(index_name);
    key.push(SCORE);
    key
}

/// The score entry key of a non-windowed index.
pub(super) fn score_key(index_name: &str, score: &[u8; FIELD_LEN], pk: &[u8]) -> Vec<u8> {
    let mut key = score_prefix(index_name);
    key.extend_from_slice(score);
    key.extend_from_slice(pk);
    key
}

/// The pk entry key of the row `pk`.
pub(super) fn pk_entry_key(index_name: &str, pk: &[u8]) -> Vec<u8> {
    let mut key = entries_prefix(index_name);
    key.push(PK);
    key.extend_from_slice(pk);
    key
}

/// Encode a score so that byte order matches ascending numeric order.
///
/// A non-negative value has its sign bit flipped, so it sorts above every
/// negative value. A negative value has every bit flipped, so a more negative
/// value sorts lower.
pub(crate) fn f64_to_sort_bytes(score: f64) -> [u8; FIELD_LEN] {
    let bits = score.to_bits();
    let key_bits = if bits >> 63 == 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    key_bits.to_be_bytes()
}

/// Reverse [`f64_to_sort_bytes`].
pub(super) fn sort_bytes_to_f64(bytes: &[u8; FIELD_LEN]) -> f64 {
    let bits = u64::from_be_bytes(*bytes);
    let original = if bits >> 63 != 0 {
        bits ^ (1u64 << 63)
    } else {
        !bits
    };
    f64::from_bits(original)
}

/// Complement every byte: reverses byte order for a descending field.
pub(super) fn complement(bytes: [u8; FIELD_LEN]) -> [u8; FIELD_LEN] {
    bytes.map(|b| !b)
}

/// The key-order bytes of `score`: complemented when higher scores rank
/// first.
pub(super) fn encode_score(score: f64, desc: bool) -> [u8; FIELD_LEN] {
    let bytes = f64_to_sort_bytes(score);
    if desc { complement(bytes) } else { bytes }
}

/// Reverse [`encode_score`].
pub(super) fn decode_score(bytes: &[u8; FIELD_LEN], desc: bool) -> f64 {
    let bytes = if desc { complement(*bytes) } else { *bytes };
    sort_bytes_to_f64(&bytes)
}

/// What a row contributes to one sorted index: its score entry's fields in
/// key-order bytes, and its expiry deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PkEntry {
    pub score: [u8; FIELD_LEN],
    /// Present exactly when the index is windowed.
    pub ts: Option<[u8; FIELD_LEN]>,
    /// Expiry deadline in ms since epoch, 0 for none.
    pub deadline: u64,
}

impl PkEntry {
    /// Stored form: `{score:8}{deadline:8 LE}`, then `{ts:8}` when windowed.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(3 * FIELD_LEN);
        out.extend_from_slice(&self.score);
        out.extend_from_slice(&self.deadline.to_le_bytes());
        if let Some(ts) = &self.ts {
            out.extend_from_slice(ts);
        }
        out
    }

    /// Decode a stored pk entry. `None` for bytes no [`Self::encode`] wrote.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let score: [u8; FIELD_LEN] = bytes.get(..FIELD_LEN)?.try_into().ok()?;
        let deadline_bytes: [u8; FIELD_LEN] =
            bytes.get(FIELD_LEN..2 * FIELD_LEN)?.try_into().ok()?;
        let ts = match bytes.len() {
            n if n == 2 * FIELD_LEN => None,
            n if n == 3 * FIELD_LEN => Some(bytes[2 * FIELD_LEN..].try_into().ok()?),
            _ => return None,
        };
        Some(Self {
            score,
            ts,
            deadline: u64::from_le_bytes(deadline_bytes),
        })
    }

    /// Whether `other` sits at the same place in key order.
    pub fn same_position(&self, other: &Self) -> bool {
        self.score == other.score && self.ts == other.ts
    }

    /// The score entry key this entry occupies for the row `pk`.
    pub fn score_key(&self, index_name: &str, pk: &[u8]) -> Vec<u8> {
        match &self.ts {
            Some(ts) => super::window::windowed_score_key(index_name, &self.score, ts, pk),
            None => score_key(index_name, &self.score, pk),
        }
    }

    /// The score entry's stored value: the deadline.
    pub fn score_value(&self) -> Vec<u8> {
        self.deadline.to_le_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_sort_numerically_and_round_trip() {
        let values = [
            f64::NEG_INFINITY,
            -100.0,
            -0.5,
            0.0,
            0.5,
            100.0,
            f64::INFINITY,
        ];
        for desc in [false, true] {
            let encoded: Vec<_> = values.iter().map(|v| encode_score(*v, desc)).collect();
            for pair in encoded.windows(2) {
                if desc {
                    assert!(pair[0] > pair[1]);
                } else {
                    assert!(pair[0] < pair[1]);
                }
            }
            for (v, e) in values.iter().zip(&encoded) {
                assert_eq!(decode_score(e, desc).to_bits(), v.to_bits());
            }
        }
    }

    #[test]
    fn index_prefixes_do_not_nest() {
        let a = entries_prefix("a");
        let b = entries_prefix("a:score:x");
        assert!(!b.starts_with(&a));
        assert!(!a.starts_with(&b));
    }

    #[test]
    fn a_pk_entry_round_trips_with_and_without_timestamp() {
        for ts in [None, Some([7u8; FIELD_LEN])] {
            let entry = PkEntry {
                score: encode_score(3.5, false),
                ts,
                deadline: 42,
            };
            assert_eq!(PkEntry::decode(&entry.encode()), Some(entry));
        }
        assert_eq!(PkEntry::decode(&[0u8; 9]), None);
    }
}
