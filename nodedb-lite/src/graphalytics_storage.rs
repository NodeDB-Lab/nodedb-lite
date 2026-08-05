// SPDX-License-Identifier: Apache-2.0

//! Exact durable edge-value encoding for the Graphalytics importer.

use nodedb_types::Namespace;
#[cfg(test)]
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::graphalytics_external_sort::SortedEdge;
use crate::query::graph_ops::edges::parse_durable_vertex_store_key;
#[cfg(test)]
use crate::query::graph_ops::edges::{
    DURABLE_VERTEX_MARKER, durable_vertex_store_key, edge_store_key,
};
use crate::storage::engine::WriteOp;

const COLLECTION: &str = "graphalytics";
const EDGE_LABEL: &str = "EDGE";

#[cfg(test)]
pub(crate) struct WeightProperties(pub(crate) f64);

#[cfg(test)]
impl zerompk::ToMessagePack for WeightProperties {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_u8(7)?;
        writer.write_map_len(1)?;
        writer.write_string("weight")?;
        writer.write_array_len(2)?;
        writer.write_u8(3)?;
        writer.write_f64(self.0)
    }
}

fn encode_stored_edge(source: &str, destination: &str, properties: &[u8]) -> Vec<u8> {
    // The tagged five-field object has under 96 bytes of fixed framing;
    // reserve 128 so valid identifiers never force a second allocation.
    let mut encoded = Vec::with_capacity(source.len() + destination.len() + properties.len() + 128);
    encoded.extend_from_slice(&[0x92, 0x07, 0x85]);
    push_string_value(&mut encoded, "collection", COLLECTION);
    push_string_value(&mut encoded, "src", source);
    push_string_value(&mut encoded, "label", EDGE_LABEL);
    push_string_value(&mut encoded, "dst", destination);
    push_string(&mut encoded, "props");
    encoded.extend_from_slice(&[0x92, 0x05]);
    push_binary(&mut encoded, properties);
    encoded
}

fn push_string_value(encoded: &mut Vec<u8>, key: &str, value: &str) {
    push_string(encoded, key);
    encoded.extend_from_slice(&[0x92, 0x04]);
    push_string(encoded, value);
}

fn push_string(encoded: &mut Vec<u8>, value: &str) {
    let len = value.len();
    match len {
        0..=31 => encoded.push(0xa0 | len as u8),
        32..=255 => encoded.extend_from_slice(&[0xd9, len as u8]),
        256..=65_535 => {
            encoded.push(0xda);
            encoded.extend_from_slice(&(len as u16).to_be_bytes());
        }
        _ => {
            encoded.push(0xdb);
            encoded.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
    encoded.extend_from_slice(value.as_bytes());
}

fn push_binary(encoded: &mut Vec<u8>, value: &[u8]) {
    let len = value.len();
    if len <= 255 {
        encoded.extend_from_slice(&[0xc4, len as u8]);
    } else if len <= 65_535 {
        encoded.push(0xc5);
        encoded.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        encoded.push(0xc6);
        encoded.extend_from_slice(&(len as u32).to_be_bytes());
    }
    encoded.extend_from_slice(value);
}

pub(crate) fn sorted_edge_write(edge: SortedEdge) -> Result<WriteOp, LiteError> {
    if let Some((collection, _node)) = parse_durable_vertex_store_key(&edge.key) {
        if collection != COLLECTION {
            return Err(malformed_stored_edge());
        }
        return Ok(WriteOp::Put {
            ns: Namespace::Graph,
            key: edge.key,
            value: Vec::new(),
        });
    }

    let prefix_len = COLLECTION.len() + 1;
    if edge.key.get(..COLLECTION.len()) != Some(COLLECTION.as_bytes())
        || edge.key.get(COLLECTION.len()) != Some(&0)
    {
        return Err(malformed_stored_edge());
    }
    let suffix = edge
        .key
        .get(prefix_len..)
        .ok_or_else(malformed_stored_edge)?;
    let source_end = suffix
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| prefix_len + offset)
        .ok_or_else(malformed_stored_edge)?;
    let label_start = source_end + 1;
    let label_end = label_start + EDGE_LABEL.len();
    let destination_start = label_end + 1;
    if edge.key.get(label_start..label_end) != Some(EDGE_LABEL.as_bytes())
        || edge.key.get(label_end) != Some(&0)
        || destination_start > edge.key.len()
    {
        return Err(malformed_stored_edge());
    }
    let source = std::str::from_utf8(&edge.key[prefix_len..source_end])
        .map_err(|_| malformed_stored_edge())?;
    let destination =
        std::str::from_utf8(&edge.key[destination_start..]).map_err(|_| malformed_stored_edge())?;
    let properties = encoded_weight_properties(edge.weight);
    let value = encode_stored_edge(source, destination, &properties);
    Ok(WriteOp::Put {
        ns: Namespace::Graph,
        key: edge.key,
        value,
    })
}

fn encoded_weight_properties(weight: f64) -> [u8; 21] {
    // Exact MessagePack emitted by `WeightProperties`: tagged Value::Object
    // containing one tagged f64 field. Keeping this fixed-size avoids one heap
    // allocation for every imported edge while preserving stored semantics.
    let mut encoded = [
        0x92, 0x07, 0x81, 0xa6, b'w', b'e', b'i', b'g', b'h', b't', 0x92, 0x03, 0xcb, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];
    encoded[13..].copy_from_slice(&weight.to_be_bytes());
    encoded
}

fn malformed_stored_edge() -> LiteError {
    LiteError::Storage {
        detail: "malformed Graphalytics durable edge key".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_weight_encoding_matches_canonical_messagepack() {
        assert_eq!(
            encoded_weight_properties(2.5).as_slice(),
            zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap(),
        );
    }

    #[test]
    fn compact_spill_regenerates_the_exact_stored_value_shape() {
        let WriteOp::Put { key, value, .. } = sorted_edge_write(SortedEdge {
            key: edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"),
            weight: 2.5,
        })
        .unwrap() else {
            panic!("expected put");
        };
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap();
        let legacy = crate::query::graph_ops::edges::edge_to_value(
            COLLECTION,
            "a",
            EDGE_LABEL,
            "b",
            &properties,
        )
        .unwrap();
        assert_eq!(key, edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"));
        assert_eq!(
            zerompk::from_msgpack::<Value>(&value).unwrap(),
            zerompk::from_msgpack::<Value>(&legacy).unwrap()
        );
    }

    #[test]
    fn marker_named_edge_source_remains_an_edge() {
        let source = std::str::from_utf8(DURABLE_VERTEX_MARKER).unwrap();
        let WriteOp::Put { value, .. } = sorted_edge_write(SortedEdge {
            key: edge_store_key(COLLECTION, source, EDGE_LABEL, "b"),
            weight: 2.5,
        })
        .unwrap() else {
            panic!("expected put");
        };
        assert!(!value.is_empty());
    }

    #[test]
    fn exact_vertex_marker_has_an_empty_value() {
        let WriteOp::Put { value, .. } = sorted_edge_write(SortedEdge {
            key: durable_vertex_store_key(COLLECTION, "isolated"),
            weight: 0.0,
        })
        .unwrap() else {
            panic!("expected put");
        };
        assert!(value.is_empty());
    }

    #[test]
    fn direct_string_encoding_matches_canonical_boundaries() {
        for length in [31usize, 32, 255, 256, 65_535, 65_536] {
            let source = "s".repeat(length);
            let properties = zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap();
            let direct = encode_stored_edge(&source, "b", &properties);
            let canonical = crate::query::graph_ops::edges::edge_to_value(
                COLLECTION,
                &source,
                EDGE_LABEL,
                "b",
                &properties,
            )
            .unwrap();
            assert_eq!(
                zerompk::from_msgpack::<Value>(&direct).unwrap(),
                zerompk::from_msgpack::<Value>(&canonical).unwrap(),
            );
        }
    }

    #[test]
    fn specialized_edge_encoding_matches_stored_value_shape() {
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap();
        let value = encode_stored_edge("a", "b", &properties);
        let Value::Object(edge) = zerompk::from_msgpack::<Value>(&value).unwrap() else {
            panic!("expected edge object");
        };
        assert_eq!(edge["collection"], Value::String(COLLECTION.to_string()));
        assert_eq!(edge["src"], Value::String("a".to_string()));
        assert_eq!(edge["label"], Value::String(EDGE_LABEL.to_string()));
        assert_eq!(edge["dst"], Value::String("b".to_string()));
    }
}
