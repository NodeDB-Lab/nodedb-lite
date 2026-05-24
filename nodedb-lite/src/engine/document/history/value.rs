// SPDX-License-Identifier: Apache-2.0

//! Value encoding for versioned document history.
//!
//! Layout: `[tag:u8][valid_from_ms:i64 LE][valid_until_ms:i64 LE][body_msgpack...]`
//!
//! The 17-byte header encodes the tag and both temporal bounds. The remaining
//! bytes are the raw MessagePack document body (empty for tombstones and
//! GDPR-erased entries).

use crate::error::LiteError;

/// Minimum encoded value length: 1 (tag) + 8 (valid_from_ms) + 8 (valid_until_ms).
const HEADER_LEN: usize = 17;

/// Tag byte values for versioned document records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VersionTag {
    /// Document exists and is readable.
    Live = 0x00,
    /// Document was deleted; body is empty.
    Tombstone = 0xFF,
    /// Document was GDPR-erased; body is empty.
    GdprErased = 0xFE,
}

impl VersionTag {
    /// Parse a raw tag byte.
    ///
    /// Returns `None` for unrecognised tag values (forward-compatibility guard).
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(Self::Live),
            0xFF => Some(Self::Tombstone),
            0xFE => Some(Self::GdprErased),
            _ => None,
        }
    }
}

/// Decoded view of a versioned document record.
#[derive(Debug, Clone)]
pub struct DecodedVersion {
    /// Status tag for this version.
    pub tag: VersionTag,
    /// Valid-time lower bound (ms since epoch, inclusive).
    pub valid_from_ms: i64,
    /// Valid-time upper bound (ms since epoch, exclusive). `i64::MAX` = open.
    pub valid_until_ms: i64,
    /// Raw MessagePack body. Empty for tombstone / GDPR-erased entries.
    pub body: Vec<u8>,
}

impl DecodedVersion {
    /// Whether this version is a live (readable) document.
    pub fn is_live(&self) -> bool {
        self.tag == VersionTag::Live
    }
}

/// Encode a versioned value payload.
///
/// The tag byte and both temporal bounds are written as a fixed 17-byte
/// header followed by the raw `body` bytes.
pub fn encode_value(
    tag: VersionTag,
    valid_from_ms: i64,
    valid_until_ms: i64,
    body: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.push(tag as u8);
    buf.extend_from_slice(&valid_from_ms.to_le_bytes());
    buf.extend_from_slice(&valid_until_ms.to_le_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Decode a versioned value payload.
///
/// Returns an error if `bytes` is shorter than the 17-byte header or if the
/// tag byte is not a recognised `VersionTag`.
pub fn decode_value(bytes: &[u8]) -> Result<DecodedVersion, LiteError> {
    if bytes.len() < HEADER_LEN {
        return Err(LiteError::Serialization {
            detail: format!(
                "versioned document value too short: {} bytes (need at least {HEADER_LEN})",
                bytes.len()
            ),
        });
    }
    let tag_byte = bytes[0];
    let tag = VersionTag::from_u8(tag_byte).ok_or_else(|| LiteError::Serialization {
        detail: format!("unknown version tag byte: 0x{tag_byte:02X}"),
    })?;
    let valid_from_ms = i64::from_le_bytes(
        bytes[1..9]
            .try_into()
            .expect("length checked above — 8 bytes"),
    );
    let valid_until_ms = i64::from_le_bytes(
        bytes[9..17]
            .try_into()
            .expect("length checked above — 8 bytes"),
    );
    Ok(DecodedVersion {
        tag,
        valid_from_ms,
        valid_until_ms,
        body: bytes[HEADER_LEN..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_live_roundtrip() {
        let body = b"msgpack_body_here";
        let encoded = encode_value(VersionTag::Live, 1_000, 2_000, body);
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(decoded.tag, VersionTag::Live);
        assert_eq!(decoded.valid_from_ms, 1_000);
        assert_eq!(decoded.valid_until_ms, 2_000);
        assert_eq!(decoded.body, body);
    }

    #[test]
    fn encode_decode_tombstone() {
        let encoded = encode_value(VersionTag::Tombstone, 500, i64::MAX, &[]);
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(decoded.tag, VersionTag::Tombstone);
        assert!(decoded.body.is_empty());
    }

    #[test]
    fn decode_too_short_returns_error() {
        assert!(decode_value(&[0x00; 16]).is_err());
    }

    #[test]
    fn decode_unknown_tag_returns_error() {
        let mut buf = vec![0u8; 17];
        buf[0] = 0xAB; // unrecognised
        assert!(decode_value(&buf).is_err());
    }

    #[test]
    fn is_live_false_for_tombstone() {
        let encoded = encode_value(VersionTag::Tombstone, 0, 0, &[]);
        let decoded = decode_value(&encoded).unwrap();
        assert!(!decoded.is_live());
    }
}
