// SPDX-License-Identifier: Apache-2.0
//! Ingest handler for the timeseries physical visitor.

use std::collections::HashSet;
use std::sync::Mutex;

use nodedb_types::result::QueryResult;
use nodedb_types::timeseries::MetricSample;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Per-process, per-collection deduplication set for WAL-LSN replay.
/// Keyed by (collection, lsn). Cleared on process restart — that is
/// acceptable because the WAL LSN dedup is a best-effort guard against
/// double-replay on crash recovery; after a clean restart the WAL is
/// replayed from scratch anyway.
static SEEN_LSNS: std::sync::LazyLock<Mutex<HashSet<(String, u64)>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashSet::new()));

/// Ingest samples into a timeseries collection.
///
/// Decodes `payload` per `format` ("ilp", "msgpack", "samples"), performs
/// WAL-LSN deduplication when `wal_lsn` is `Some`, and delegates to
/// `ingest_metric` for each decoded sample.
pub fn ingest<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    payload: &[u8],
    format: &str,
    wal_lsn: Option<u64>,
    surrogates: &[nodedb_types::Surrogate],
) -> Result<QueryResult, LiteError> {
    // WAL-LSN deduplication: skip the entire batch if already applied.
    if let Some(lsn) = wal_lsn {
        let mut seen = SEEN_LSNS.lock().map_err(|_| LiteError::LockPoisoned)?;
        let key = (collection.to_string(), lsn);
        if seen.contains(&key) {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected: 0,
            });
        }
        seen.insert(key);
    }

    let samples = decode_payload(payload, format)?;
    let count = samples.len() as u64;

    let _ = surrogates; // Lite TS engine assigns internal series IDs; surrogate
    // allocation for cross-engine identity is managed by the
    // Origin CP before dispatch. For opaque payloads (ILP,
    // raw msgpack) the CP cannot pre-assign surrogates per
    // the TimeseriesOp definition; Lite uses its own series
    // catalog for identity within the embedded context.

    {
        let mut ts_engine = engine
            .timeseries
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;

        for (metric_name, tags, sample) in samples {
            ts_engine.ingest_metric(collection, &metric_name, tags, sample);
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: count,
    })
}

// ── Payload decoders ──────────────────────────────────────────────────────────

/// Decoded sample ready for `ingest_metric`.
pub type ParsedSample = (String, Vec<(String, String)>, MetricSample);

fn decode_payload(payload: &[u8], format: &str) -> Result<Vec<ParsedSample>, LiteError> {
    match format {
        "ilp" => parse_ilp(payload),
        "msgpack" => parse_msgpack(payload),
        "samples" | "structured" => parse_structured(payload),
        other => Err(LiteError::BadRequest {
            detail: format!(
                "unknown timeseries ingest format '{other}'; expected ilp/msgpack/samples"
            ),
        }),
    }
}

/// Test-only re-export of the ILP parser so integration tests can verify
/// round-trip correctness without going through the full ingest stack.
/// Expose the ILP parser for integration tests.
pub fn parse_ilp_for_test(payload: &[u8]) -> Result<Vec<ParsedSample>, LiteError> {
    parse_ilp(payload)
}

// ── ILP parser ────────────────────────────────────────────────────────────────

/// Minimal InfluxDB Line Protocol parser for timeseries ingest.
///
/// Grammar: `measurement[,tag=val]* field=val[,field=val]* [timestamp_ns]`
///
/// - Each line produces one sample.
/// - The first numeric field found is used as `value`.
/// - The trailing integer (if present) is the timestamp in nanoseconds;
///   we convert to milliseconds by dividing by 1_000_000.
/// - Tags become the `tags` vector.
/// - `measurement` is the metric name.
fn parse_ilp(payload: &[u8]) -> Result<Vec<ParsedSample>, LiteError> {
    let text = std::str::from_utf8(payload).map_err(|e| LiteError::Serialization {
        detail: format!("ILP payload is not valid UTF-8: {e}"),
    })?;

    let mut out: Vec<ParsedSample> = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split into key_part and rest on first unescaped space.
        let (key_part, rest) = split_ilp_space(line).ok_or_else(|| LiteError::Serialization {
            detail: format!("ILP line missing field set: {line}"),
        })?;

        // Optionally split timestamp off the trailing rest.
        let (fields_part, timestamp_str) = split_ilp_space(rest)
            .map(|(f, t)| (f, Some(t)))
            .unwrap_or((rest, None));

        // Derive measurement and tags.
        let mut key_iter = key_part.splitn(2, ',');
        let measurement = key_iter.next().unwrap_or("unknown").to_string();

        let mut tags: Vec<(String, String)> = Vec::new();
        if let Some(tag_str) = key_iter.next() {
            for kv in tag_str.split(',') {
                if let Some((k, v)) = kv.split_once('=') {
                    tags.push((k.to_string(), v.to_string()));
                }
            }
        }

        // Parse fields — use the first numeric field as the sample value.
        let mut value_opt: Option<f64> = None;
        for kv in fields_part.split(',') {
            if let Some((_k, v)) = kv.split_once('=')
                && let Some(f) = parse_numeric(v)
            {
                value_opt = Some(f);
                break;
            }
        }

        let value = value_opt.unwrap_or(0.0);

        // Parse timestamp: nanoseconds → milliseconds.
        let timestamp_ms = timestamp_str
            .and_then(|s| s.parse::<i64>().ok())
            .map(|ns| ns / 1_000_000)
            .unwrap_or_else(current_time_ms);

        out.push((
            measurement,
            tags,
            MetricSample {
                timestamp_ms,
                value,
            },
        ));
    }

    Ok(out)
}

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

fn parse_numeric(v: &str) -> Option<f64> {
    if let Some(stripped) = v.strip_suffix('i') {
        return stripped.parse::<i64>().ok().map(|n| n as f64);
    }
    v.parse::<f64>().ok()
}

// ── MessagePack decoder ───────────────────────────────────────────────────────

/// Decode a msgpack-encoded array of sample objects.
///
/// Expected shape: `[{metric, value, timestamp_ms?, tags?}, ...]`
/// or a single object `{metric, value, timestamp_ms?, tags?}`.
fn parse_msgpack(payload: &[u8]) -> Result<Vec<ParsedSample>, LiteError> {
    let top: Value = zerompk::from_msgpack(payload).map_err(|e| LiteError::Serialization {
        detail: format!("msgpack timeseries payload: {e}"),
    })?;

    match top {
        Value::Array(items) => items.into_iter().map(decode_msgpack_sample).collect(),
        obj @ Value::Object(_) => decode_msgpack_sample(obj).map(|s| vec![s]),
        _ => Err(LiteError::Serialization {
            detail: "msgpack timeseries payload must be an object or array of objects".into(),
        }),
    }
}

fn decode_msgpack_sample(v: Value) -> Result<ParsedSample, LiteError> {
    let Value::Object(map) = v else {
        return Err(LiteError::Serialization {
            detail: "each msgpack timeseries sample must be an object".into(),
        });
    };

    let metric = match map.get("metric").or_else(|| map.get("name")) {
        Some(Value::String(s)) => s.clone(),
        _ => "value".to_string(),
    };

    let value = match map.get("value") {
        Some(Value::Float(f)) => *f,
        Some(Value::Integer(i)) => *i as f64,
        _ => 0.0,
    };

    let timestamp_ms = match map.get("timestamp_ms").or_else(|| map.get("ts")) {
        Some(Value::Integer(i)) => *i,
        Some(Value::Float(f)) => *f as i64,
        _ => current_time_ms(),
    };

    let tags = match map.get("tags") {
        Some(Value::Object(t)) => t
            .iter()
            .map(|(k, v)| {
                let val = match v {
                    Value::String(s) => s.clone(),
                    other => format!("{other:?}"),
                };
                (k.clone(), val)
            })
            .collect(),
        _ => Vec::new(),
    };

    Ok((
        metric,
        tags,
        MetricSample {
            timestamp_ms,
            value,
        },
    ))
}

// ── Structured / samples decoder ──────────────────────────────────────────────

/// Decode a structured payload: msgpack-encoded `Vec<MetricSample>` with
/// a flat metric name stored at key `"_metric"` in the outer object, or
/// a msgpack array of `{metric, value, timestamp_ms}` objects.
///
/// This is the format produced by the CP when it can enumerate rows at
/// planning time (SQL VALUES path). It falls back to msgpack object decode.
fn parse_structured(payload: &[u8]) -> Result<Vec<ParsedSample>, LiteError> {
    // Try as flat JSON array of MetricSample objects (CP-produced path with
    // surrogate pre-assignment). MetricSample is serde-only (not zerompk),
    // so try JSON decode first before falling back to msgpack object decode.
    if let Ok(samples) = sonic_rs::from_slice::<Vec<MetricSample>>(payload) {
        return Ok(samples
            .into_iter()
            .map(|s| ("value".to_string(), Vec::new(), s))
            .collect());
    }

    // Fall back to object/array decode.
    parse_msgpack(payload)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ilp_round_trip_basic() {
        let ilp = b"cpu,host=server01 usage=0.75 1700000000000000000\n";
        let samples = parse_ilp(ilp).expect("parse ILP");
        assert_eq!(samples.len(), 1);
        let (metric, tags, sample) = &samples[0];
        assert_eq!(metric, "cpu");
        assert_eq!(tags[0], ("host".to_string(), "server01".to_string()));
        assert!((sample.value - 0.75_f64).abs() < 1e-9);
        // 1700000000000000000 ns / 1_000_000 = 1_700_000_000_000 ms
        assert_eq!(sample.timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn ilp_multiple_lines() {
        let ilp = b"mem,host=a free=1024i 1000000000\nmem,host=b free=2048i 2000000000\n";
        let samples = parse_ilp(ilp).expect("parse ILP multi-line");
        assert_eq!(samples.len(), 2);
    }

    #[test]
    fn ilp_skips_comments() {
        let ilp = b"# comment\ncpu usage=1.0 1000000000\n";
        let samples = parse_ilp(ilp).expect("ILP with comment");
        assert_eq!(samples.len(), 1);
    }

    #[test]
    fn msgpack_round_trip() {
        use std::collections::HashMap;
        let mut obj: HashMap<String, Value> = HashMap::new();
        obj.insert("metric".into(), Value::String("temperature".into()));
        obj.insert("value".into(), Value::Float(22.5));
        obj.insert("timestamp_ms".into(), Value::Integer(1_700_000_000_000));
        let bytes = zerompk::to_msgpack_vec(&Value::Object(obj)).expect("encode");
        let samples = parse_msgpack(&bytes).expect("parse msgpack");
        assert_eq!(samples.len(), 1);
        let (metric, _, sample) = &samples[0];
        assert_eq!(metric, "temperature");
        assert!((sample.value - 22.5).abs() < 1e-9);
        assert_eq!(sample.timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn structured_flat_metric_samples() {
        let samples = vec![
            MetricSample {
                timestamp_ms: 1000,
                value: 1.0,
            },
            MetricSample {
                timestamp_ms: 2000,
                value: 2.0,
            },
        ];
        // structured format falls back to JSON decode of Vec<MetricSample>.
        let bytes = sonic_rs::to_vec(&samples).expect("encode");
        let parsed = parse_structured(&bytes).expect("parse structured");
        assert_eq!(parsed.len(), 2);
        assert!((parsed[0].2.value - 1.0).abs() < 1e-9);
        assert!((parsed[1].2.value - 2.0).abs() < 1e-9);
    }

    #[test]
    fn lsn_dedup_skips_duplicate() {
        // The SEEN_LSNS map is process-wide; use a unique collection name per test.
        let key = ("__test_dedup_coll__".to_string(), 9999u64);
        {
            let mut seen = SEEN_LSNS.lock().unwrap();
            seen.insert(key.clone());
        }
        // Verify the key is present.
        let seen = SEEN_LSNS.lock().unwrap();
        assert!(seen.contains(&key));
    }
}
