// SPDX-License-Identifier: Apache-2.0

//! Storage keys of the secondary-index catalog and the order-preserving
//! value encoding its entries sort by.
//!
//! Every key lives in `Namespace::Meta` under [`ROOT`]. `ROOT` starts with the
//! byte `0xFF`, which no UTF-8 text key can start with, so no other Meta key
//! (all of them are text) can collide with an index key.
//!
//! An entry key is
//! `ROOT 0x01 | u32 len | collection | u32 len | index name | value | doc id`.
//! The length prefixes make the `(collection, index)` prefix of one index a
//! prefix of no other index's keys. The value encoding is self-delimiting, so
//! the document id is the terminal component: one key per `(value, doc)`, and
//! an equality lookup is a prefix scan over `prefix | value`.
//!
//! A value encodes as one class tag byte followed by the payload. Classes sort
//! `bool < number < datetime < string < other`; within a class the bytes sort
//! as the values do, so a range scan over one class is a byte-range scan.

use nodedb_types::value::Value;

/// First bytes of every index key.
pub(crate) const ROOT: &[u8] = b"\xFFidx";
const ENTRY: u8 = 0x01;
const DEF: u8 = 0x02;
const FORMAT: u8 = 0x03;
const LEGACY_CLEARED: u8 = 0x04;
const SORTED_DEF: u8 = 0x05;

/// Version of the entry key layout and value encoding written by this build.
/// Entries stored under another version are rebuilt from rows at open.
pub(crate) const FORMAT_VERSION: u32 = 1;

pub(crate) const TAG_BOOL: u8 = 0x10;
pub(crate) const TAG_NUMBER: u8 = 0x20;
pub(crate) const TAG_DATETIME: u8 = 0x30;
pub(crate) const TAG_STRING: u8 = 0x40;
pub(crate) const TAG_OTHER: u8 = 0x50;

/// Terminator of a variable-length payload. A `0x00` inside the payload is
/// escaped as `0x00 0xFF`, so a payload that is a prefix of another still
/// sorts first and never runs into the document id.
const TERMINATOR: [u8; 2] = [0x00, 0x01];
const ESCAPED_ZERO: [u8; 2] = [0x00, 0xFF];

/// The key holding [`FORMAT_VERSION`].
pub(crate) fn format_key() -> Vec<u8> {
    let mut key = ROOT.to_vec();
    key.push(FORMAT);
    key
}

/// The key whose presence records that entries of the layout used before
/// definitions were persisted are gone.
pub(crate) fn legacy_cleared_key() -> Vec<u8> {
    let mut key = ROOT.to_vec();
    key.push(LEGACY_CLEARED);
    key
}

/// Prefix of every stored index definition.
pub(crate) fn defs_prefix() -> Vec<u8> {
    let mut key = ROOT.to_vec();
    key.push(DEF);
    key
}

/// The key one index definition is stored under.
pub(crate) fn def_key(collection: &str, name: &str) -> Vec<u8> {
    let mut key = defs_prefix();
    push_component(&mut key, collection);
    push_component(&mut key, name);
    key
}

/// Prefix of every stored sorted-index definition.
pub(crate) fn sorted_defs_prefix() -> Vec<u8> {
    let mut key = ROOT.to_vec();
    key.push(SORTED_DEF);
    key
}

/// The key one sorted-index definition is stored under.
pub(crate) fn sorted_def_key(name: &str) -> Vec<u8> {
    let mut key = sorted_defs_prefix();
    push_component(&mut key, name);
    key
}

/// Prefix of every index entry of every collection.
pub(crate) fn entries_prefix() -> Vec<u8> {
    let mut key = ROOT.to_vec();
    key.push(ENTRY);
    key
}

/// Prefix of every entry of every index on `collection`.
pub(crate) fn collection_prefix(collection: &str) -> Vec<u8> {
    let mut key = entries_prefix();
    push_component(&mut key, collection);
    key
}

/// Prefix of every entry of one index.
pub(crate) fn index_prefix(collection: &str, name: &str) -> Vec<u8> {
    let mut key = collection_prefix(collection);
    push_component(&mut key, name);
    key
}

/// The entry key for one encoded value of one document.
pub(crate) fn entry_key(index_prefix: &[u8], encoded_value: &[u8], doc_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(index_prefix.len() + encoded_value.len() + doc_id.len());
    key.extend_from_slice(index_prefix);
    key.extend_from_slice(encoded_value);
    key.extend_from_slice(doc_id.as_bytes());
    key
}

/// An entry key split into its components.
pub(crate) struct ParsedEntry<'a> {
    pub collection: &'a str,
    pub doc_id: &'a str,
}

/// Split an entry key. `None` for a key that is not a well-formed entry.
pub(crate) fn parse_entry(key: &[u8]) -> Option<ParsedEntry<'_>> {
    let rest = key.strip_prefix(entries_prefix().as_slice())?;
    let (collection, rest) = read_component(rest)?;
    let (_index, rest) = read_component(rest)?;
    let value_len = encoded_len(rest)?;
    let doc_id = std::str::from_utf8(rest.get(value_len..)?).ok()?;
    Some(ParsedEntry { collection, doc_id })
}

/// The document id of an entry under `index_prefix`.
pub(crate) fn entry_doc_id<'a>(key: &'a [u8], index_prefix: &[u8]) -> Option<&'a str> {
    let rest = key.strip_prefix(index_prefix)?;
    let value_len = encoded_len(rest)?;
    std::str::from_utf8(rest.get(value_len..)?).ok()
}

/// The document id an entry carries for a row keyed by bytes, as strict and
/// key-value rows are: the key bytes in lowercase hex, which is UTF-8 and
/// reversible whatever the bytes are.
pub(crate) fn doc_id_of_key(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut id = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        id.push(HEX[(b >> 4) as usize] as char);
        id.push(HEX[(b & 0x0F) as usize] as char);
    }
    id
}

/// The key bytes of a document id [`doc_id_of_key`] made. `None` for an id
/// that is not such a hex string.
pub(crate) fn key_of_doc_id(id: &str) -> Option<Vec<u8>> {
    fn nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    let bytes = id.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks(2)
        .map(|pair| Some((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

fn push_component(key: &mut Vec<u8>, component: &str) {
    // A component longer than u32::MAX bytes cannot be a collection or index
    // name: both come from SQL identifiers.
    let len = u32::try_from(component.len()).unwrap_or(u32::MAX);
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(component.as_bytes());
}

fn read_component(bytes: &[u8]) -> Option<(&str, &[u8])> {
    let len_bytes: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let end = 4usize.checked_add(len)?;
    let component = std::str::from_utf8(bytes.get(4..end)?).ok()?;
    Some((component, bytes.get(end..)?))
}

// ── Value encoding ──────────────────────────────────────────────────────────

/// Encode one value as exactly its own type: a string stays a string. `None`
/// for a value no index holds: NULL, arrays and objects.
pub(crate) fn encode_typed(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Null | Value::Array(_) | Value::Object(_) => None,
        Value::Bool(b) => Some(vec![TAG_BOOL, u8::from(*b)]),
        Value::Integer(i) => Some(encode_number(*i as f64)),
        Value::Float(f) => Some(encode_number(*f)),
        // Coerced comparison reads a decimal as the f64 it denotes.
        Value::Decimal(d) => d.to_string().parse::<f64>().ok().map(encode_number),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => Some(encode_datetime(dt.micros)),
        Value::String(s) => Some(encode_escaped(TAG_STRING, s.as_bytes())),
        other => {
            let bytes = zerompk::to_msgpack_vec(other).ok()?;
            Some(encode_escaped(TAG_OTHER, &bytes))
        }
    }
}

/// Encode a value under every class a coerced SQL comparison can match it in.
///
/// SQL equality and ordering on documents coerce: the string `'7'` equals the
/// number `7`, and a timestamp string compares as a timestamp. A string is
/// therefore stored as a string, and also as a number when it parses as one
/// and as a datetime when it parses as one. A lookup probes the same classes,
/// so the entries it finds are a superset of the rows the comparison matches;
/// the reader checks each candidate against the comparison itself.
pub(crate) fn encode_coercible(value: &Value) -> Vec<Vec<u8>> {
    let Value::String(s) = value else {
        return encode_typed(value).into_iter().collect();
    };
    let mut keys = vec![encode_escaped(TAG_STRING, s.as_bytes())];
    if let Ok(n) = s.parse::<f64>() {
        keys.push(encode_number(n));
    }
    if let Some(dt) = nodedb_types::NdbDateTime::parse(s) {
        keys.push(encode_datetime(dt.micros));
    }
    keys
}

/// Byte length of the encoded value at the start of `bytes`.
pub(crate) fn encoded_len(bytes: &[u8]) -> Option<usize> {
    match *bytes.first()? {
        TAG_BOOL => (bytes.len() >= 2).then_some(2),
        TAG_NUMBER | TAG_DATETIME => (bytes.len() >= 9).then_some(9),
        TAG_STRING | TAG_OTHER => {
            let mut i = 1;
            while i + 1 < bytes.len() {
                if bytes[i] == 0x00 {
                    match bytes[i + 1] {
                        0x01 => return Some(i + 2),
                        0xFF => i += 2,
                        _ => return None,
                    }
                } else {
                    i += 1;
                }
            }
            None
        }
        _ => None,
    }
}

fn encode_number(f: f64) -> Vec<u8> {
    // -0.0 equals 0.0 and every NaN is one value, so each gets one key.
    let f = if f == 0.0 {
        0.0
    } else if f.is_nan() {
        f64::NAN
    } else {
        f
    };
    let bits = f.to_bits();
    let sortable = if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    let mut out = Vec::with_capacity(9);
    out.push(TAG_NUMBER);
    out.extend_from_slice(&sortable.to_be_bytes());
    out
}

fn encode_datetime(micros: i64) -> Vec<u8> {
    let sortable = (micros as u64) ^ (1u64 << 63);
    let mut out = Vec::with_capacity(9);
    out.push(TAG_DATETIME);
    out.extend_from_slice(&sortable.to_be_bytes());
    out
}

fn encode_escaped(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 3);
    out.push(tag);
    for &b in payload {
        if b == 0x00 {
            out.extend_from_slice(&ESCAPED_ZERO);
        } else {
            out.push(b);
        }
    }
    out.extend_from_slice(&TERMINATOR);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed(v: Value) -> Vec<u8> {
        encode_typed(&v).expect("indexable")
    }

    #[test]
    fn numbers_sort_numerically_across_integer_and_float() {
        let ordered = [
            Value::Float(f64::NEG_INFINITY),
            Value::Integer(-100),
            Value::Float(-2.5),
            Value::Integer(0),
            Value::Float(0.5),
            Value::Integer(2),
            Value::Integer(10),
            Value::Integer(100),
            Value::Float(f64::INFINITY),
        ];
        let keys: Vec<Vec<u8>> = ordered.iter().cloned().map(typed).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "{keys:?}");
    }

    #[test]
    fn equal_numbers_share_one_key() {
        assert_eq!(typed(Value::Integer(3)), typed(Value::Float(3.0)));
        assert_eq!(typed(Value::Float(-0.0)), typed(Value::Float(0.0)));
    }

    #[test]
    fn a_string_prefix_sorts_before_its_extension() {
        let a = typed(Value::String("a".into()));
        let a_nul = typed(Value::String("a\0".into()));
        let ab = typed(Value::String("ab".into()));
        assert!(a < a_nul && a_nul < ab);
    }

    #[test]
    fn classes_never_collide() {
        let s = typed(Value::String("1".into()));
        let n = typed(Value::Integer(1));
        let b = typed(Value::Bool(true));
        assert_ne!(s, n);
        assert_ne!(n, b);
        assert!(b < n && n < s);
    }

    #[test]
    fn a_numeric_string_is_also_a_number() {
        let keys = encode_coercible(&Value::String("7".into()));
        assert!(keys.contains(&typed(Value::Integer(7))));
        assert!(keys.contains(&typed(Value::String("7".into()))));
    }

    #[test]
    fn values_containing_separators_keep_their_document_id() {
        let prefix = index_prefix("c:x", "i:y");
        for value in ["", ":", "a:b", "\0", "\0\u{1}", "x\0y"] {
            let encoded = typed(Value::String(value.into()));
            let key = entry_key(&prefix, &encoded, "doc:1");
            assert_eq!(entry_doc_id(&key, &prefix), Some("doc:1"), "{value:?}");
            let parsed = parse_entry(&key).expect("entry");
            assert_eq!((parsed.collection, parsed.doc_id), ("c:x", "doc:1"));
        }
    }

    #[test]
    fn a_byte_key_round_trips_through_its_document_id() {
        for bytes in [&b""[..], b"a", b"\x00\xff:x", &[0x80, 0x01]] {
            let id = doc_id_of_key(bytes);
            assert_eq!(key_of_doc_id(&id).as_deref(), Some(bytes));
        }
        assert!(key_of_doc_id("abc").is_none());
        assert!(key_of_doc_id("zz").is_none());
    }

    #[test]
    fn collection_prefixes_do_not_nest() {
        let a = collection_prefix("a");
        let ab = collection_prefix("a:b");
        assert!(!ab.starts_with(&a));
        assert!(!a.starts_with(&ab));
    }
}
