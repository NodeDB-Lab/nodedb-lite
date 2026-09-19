// SPDX-License-Identifier: Apache-2.0
//! Insert payload decoding: JSON, MessagePack, and ILP line protocol rows.

use nodedb_types::value::Value;

use crate::error::LiteError;

/// Decode the insert payload per format into a list of column-ordered rows.
pub(super) fn decode_payload(
    payload: &[u8],
    format: &str,
    col_names: &[String],
) -> Result<Vec<Vec<Value>>, LiteError> {
    match format {
        "json" => {
            let arr: serde_json::Value =
                sonic_rs::from_slice(payload).map_err(|e| LiteError::Serialization {
                    detail: format!("json payload decode: {e}"),
                })?;
            match arr {
                serde_json::Value::Array(objects) => objects
                    .into_iter()
                    .map(|obj| json_object_to_row(obj, col_names))
                    .collect(),
                serde_json::Value::Object(_) => Ok(vec![json_object_to_row(arr, col_names)?]),
                _ => Err(LiteError::Serialization {
                    detail: "json payload must be an object or array of objects".into(),
                }),
            }
        }
        "msgpack" => {
            // Try array of rows first, then single row.
            let top: Value =
                zerompk::from_msgpack(payload).map_err(|e| LiteError::Serialization {
                    detail: format!("msgpack payload decode: {e}"),
                })?;
            match top {
                Value::Array(items) => items
                    .into_iter()
                    .map(|v| value_object_to_row(v, col_names))
                    .collect(),
                obj @ Value::Object(_) => Ok(vec![value_object_to_row(obj, col_names)?]),
                _ => Err(LiteError::Serialization {
                    detail: "msgpack payload must be an object or array of objects".into(),
                }),
            }
        }
        "ilp" => parse_ilp(payload, col_names),
        other => Err(LiteError::BadRequest {
            detail: format!("unknown columnar insert format '{other}'; expected json/msgpack/ilp"),
        }),
    }
}

/// Minimal InfluxDB Line Protocol parser.
///
/// Grammar: `measurement[,tag=val]* field=val[,field=val]* [timestamp]`
/// Produces one row per non-empty, non-comment line. Column names that are
/// not in `col_names` are silently skipped; absent columns default to Null.
fn parse_ilp(payload: &[u8], col_names: &[String]) -> Result<Vec<Vec<Value>>, LiteError> {
    let text = std::str::from_utf8(payload).map_err(|e| LiteError::Serialization {
        detail: format!("ILP payload is not valid UTF-8: {e}"),
    })?;

    let mut rows: Vec<Vec<Value>> = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split on the first unescaped space to separate the key+tags from fields.
        let (key_part, rest) = split_ilp_space(line).ok_or_else(|| LiteError::Serialization {
            detail: format!("ILP line missing field set: {line}"),
        })?;

        // Split timestamp off the end of rest (optional trailing integer after space).
        let (fields_part, _timestamp) = split_ilp_space(rest)
            .map(|(f, t)| (f, Some(t)))
            .unwrap_or((rest, None));

        // Parse measurement and tags from key_part.
        let mut pairs: std::collections::HashMap<String, Value> = std::collections::HashMap::new();

        // key_part: measurement,tag=val,tag=val
        let mut key_iter = key_part.splitn(2, ',');
        let _measurement = key_iter.next().unwrap_or("");
        if let Some(tags) = key_iter.next() {
            for kv in tags.split(',') {
                if let Some((k, v)) = kv.split_once('=') {
                    pairs.insert(k.to_string(), Value::String(v.to_string()));
                }
            }
        }

        // Parse fields.
        for kv in fields_part.split(',') {
            if let Some((k, v)) = kv.split_once('=') {
                let val = parse_ilp_field_value(v);
                pairs.insert(k.to_string(), val);
            }
        }

        // Build column-ordered row.
        let row: Vec<Value> = col_names
            .iter()
            .map(|name| pairs.get(name).cloned().unwrap_or(Value::Null))
            .collect();
        rows.push(row);
    }

    Ok(rows)
}

/// Split an ILP line on the first unescaped space.
fn split_ilp_space(s: &str) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == b' ' {
            return Some((&s[..i], s[i + 1..].trim_start()));
        }
        i += 1;
    }
    None
}

/// Parse an ILP field value string to a `Value`.
fn parse_ilp_field_value(v: &str) -> Value {
    // Integer suffix: 12345i
    if let Some(stripped) = v.strip_suffix('i')
        && let Ok(n) = stripped.parse::<i64>()
    {
        return Value::Integer(n);
    }
    // Boolean
    match v {
        "true" | "True" | "TRUE" | "t" | "T" => return Value::Bool(true),
        "false" | "False" | "FALSE" | "f" | "F" => return Value::Bool(false),
        _ => {}
    }
    // Quoted string
    if v.starts_with('"') && v.ends_with('"') && v.len() >= 2 {
        return Value::String(v[1..v.len() - 1].replace("\\\"", "\""));
    }
    // Float
    if let Ok(f) = v.parse::<f64>() {
        return Value::Float(f);
    }
    // Fall back to string
    Value::String(v.to_string())
}

/// Convert a serde_json object to a column-ordered row.
fn json_object_to_row(
    obj: serde_json::Value,
    col_names: &[String],
) -> Result<Vec<Value>, LiteError> {
    match obj {
        serde_json::Value::Object(map) => {
            let row: Vec<Value> = col_names
                .iter()
                .map(|name| map.get(name).map(json_value_to_ndb).unwrap_or(Value::Null))
                .collect();
            Ok(row)
        }
        _ => Err(LiteError::Serialization {
            detail: "each element in JSON array must be an object".into(),
        }),
    }
}

fn json_value_to_ndb(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_value_to_ndb).collect()),
        serde_json::Value::Object(map) => {
            let m: std::collections::HashMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), json_value_to_ndb(v)))
                .collect();
            Value::Object(m)
        }
    }
}

/// Convert a Value::Object to a column-ordered row.
fn value_object_to_row(obj: Value, col_names: &[String]) -> Result<Vec<Value>, LiteError> {
    match obj {
        Value::Object(map) => {
            let row: Vec<Value> = col_names
                .iter()
                .map(|name| map.get(name).cloned().unwrap_or(Value::Null))
                .collect();
            Ok(row)
        }
        _ => Err(LiteError::Serialization {
            detail: "each msgpack element must be an object map".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ilp_basic() {
        let col_names = vec!["host".to_string(), "cpu".to_string(), "ts".to_string()];
        let ilp = b"cpu,host=server01 cpu=0.64 1465839830100400200";
        let rows = parse_ilp(ilp, &col_names).unwrap();
        assert_eq!(rows.len(), 1);
        // host tag
        assert_eq!(rows[0][0], Value::String("server01".into()));
        // cpu field float
        assert_eq!(rows[0][1], Value::Float(0.64));
    }

    #[test]
    fn parse_ilp_integer_field() {
        let col_names = vec!["count".to_string()];
        let ilp = b"events count=42i";
        let rows = parse_ilp(ilp, &col_names).unwrap();
        assert_eq!(rows[0][0], Value::Integer(42));
    }

    #[test]
    fn parse_ilp_bool_field() {
        let col_names = vec!["active".to_string()];
        let ilp = b"status active=true";
        let rows = parse_ilp(ilp, &col_names).unwrap();
        assert_eq!(rows[0][0], Value::Bool(true));
    }

    #[test]
    fn parse_ilp_comment_and_empty_lines() {
        let col_names = vec!["v".to_string()];
        let ilp = b"# comment\n\nevents v=1i";
        let rows = parse_ilp(ilp, &col_names).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(1));
    }

    #[test]
    fn json_object_to_row_basic() {
        let obj = serde_json::json!({"a": 1, "b": "hello"});
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let row = json_object_to_row(obj, &cols).unwrap();
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::String("hello".into()));
        assert_eq!(row[2], Value::Null);
    }
}
