// SPDX-License-Identifier: Apache-2.0

//! Exact durable edge-value encoding for the Graphalytics importer.

use nodedb_types::Namespace;
#[cfg(test)]
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::graphalytics_external_sort::SortedEdge;
#[cfg(test)]
use crate::query::graph_ops::edges::edge_store_key;
use crate::storage::engine::WriteOp;

const COLLECTION: &str = "graphalytics";
const EDGE_LABEL: &str = "EDGE";

pub(crate) struct WeightProperties(pub(crate) f64);

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

struct StoredGraphalyticsEdge<'a> {
    source: &'a str,
    destination: &'a str,
    properties: &'a [u8],
}

impl zerompk::ToMessagePack for StoredGraphalyticsEdge<'_> {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_u8(7)?;
        writer.write_map_len(5)?;
        write_string_value(writer, "collection", COLLECTION)?;
        write_string_value(writer, "src", self.source)?;
        write_string_value(writer, "label", EDGE_LABEL)?;
        write_string_value(writer, "dst", self.destination)?;
        writer.write_string("props")?;
        writer.write_array_len(2)?;
        writer.write_u8(5)?;
        writer.write_binary(self.properties)
    }
}

fn write_string_value<W: zerompk::Write>(
    writer: &mut W,
    key: &str,
    value: &str,
) -> zerompk::Result<()> {
    writer.write_string(key)?;
    writer.write_array_len(2)?;
    writer.write_u8(4)?;
    writer.write_string(value)
}

pub(crate) fn sorted_edge_write(edge: SortedEdge) -> Result<WriteOp, LiteError> {
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
    let properties =
        zerompk::to_msgpack_vec(&WeightProperties(edge.weight)).map_err(serialization_error)?;
    let value = zerompk::to_msgpack_vec(&StoredGraphalyticsEdge {
        source,
        destination,
        properties: &properties,
    })
    .map_err(serialization_error)?;
    Ok(WriteOp::Put {
        ns: Namespace::Graph,
        key: edge.key,
        value,
    })
}

fn serialization_error(error: impl std::fmt::Display) -> LiteError {
    LiteError::Serialization {
        detail: error.to_string(),
    }
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
    fn specialized_edge_encoding_matches_stored_value_shape() {
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap();
        let value = zerompk::to_msgpack_vec(&StoredGraphalyticsEdge {
            source: "a",
            destination: "b",
            properties: &properties,
        })
        .unwrap();
        let Value::Object(edge) = zerompk::from_msgpack::<Value>(&value).unwrap() else {
            panic!("expected edge object");
        };
        assert_eq!(edge["collection"], Value::String(COLLECTION.to_string()));
        assert_eq!(edge["src"], Value::String("a".to_string()));
        assert_eq!(edge["label"], Value::String(EDGE_LABEL.to_string()));
        assert_eq!(edge["dst"], Value::String("b".to_string()));
    }
}
