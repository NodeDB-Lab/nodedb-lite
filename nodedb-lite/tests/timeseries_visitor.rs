// SPDX-License-Identifier: Apache-2.0
//! Integration tests for the timeseries physical visitor (Scan + Ingest).
//!
//! The unit tests in `timeseries_ops/{reads,writes}.rs` cover the internal
//! parsing and projection helpers directly. These tests verify that the ILP
//! parser (exposed via `parse_ilp_for_test`) produces the correct output and
//! that the bucketed-scan / bitemporal-cutoff logic holds for the engine
//! primitives used by the scan handler.

use nodedb_lite::error::LiteError;
use nodedb_lite::query::timeseries_ops::reads::{
    BucketScanParams, bucket_scan, reject_unsupported_gap_fill,
};
use nodedb_types::result::QueryResult;
use nodedb_types::timeseries::{MetricSample, TimeRange};
use nodedb_types::value::Value;

/// Build a minimal `TimeseriesEngine` and populate it.
fn ingest_samples(
    collection: &str,
    samples: &[(i64, f64)],
) -> nodedb_lite::engine::timeseries::TimeseriesEngine {
    let mut eng = nodedb_lite::engine::timeseries::TimeseriesEngine::new();
    for (ts, val) in samples {
        eng.ingest_metric(
            collection,
            "cpu",
            vec![("host".into(), "server01".into())],
            MetricSample {
                timestamp_ms: *ts,
                value: *val,
            },
        );
    }
    eng
}

// ── ILP ingest round-trip ─────────────────────────────────────────────────────

#[test]
fn ts_ingest_ilp_round_trip() {
    let ilp = b"cpu,host=server01 usage=0.75 1700000000000000000\n\
          cpu,host=server02 usage=0.50 1700000001000000000\n";

    let samples =
        nodedb_lite::query::timeseries_ops::writes::parse_ilp_for_test(ilp).expect("parse ILP");

    assert_eq!(samples.len(), 2);

    let (metric0, tags0, s0) = &samples[0];
    assert_eq!(metric0, "cpu");
    assert_eq!(tags0[0], ("host".to_string(), "server01".to_string()));
    assert!((s0.value - 0.75_f64).abs() < 1e-9);
    // 1_700_000_000_000_000_000 ns / 1_000_000 = 1_700_000_000_000 ms
    assert_eq!(s0.timestamp_ms, 1_700_000_000_000_i64);

    let (metric1, tags1, s1) = &samples[1];
    assert_eq!(metric1, "cpu");
    assert_eq!(tags1[0], ("host".to_string(), "server02".to_string()));
    assert!((s1.value - 0.50_f64).abs() < 1e-9);
    assert_eq!(s1.timestamp_ms, 1_700_000_001_000_i64);
}

// ── Bucketed scan with gap-fill ───────────────────────────────────────────────

#[test]
fn ts_scan_bucketed_with_gap_fill() {
    // Data at t=1000, t=2000, t=4000 (bucket 3000 is empty — gap).
    let eng = ingest_samples("metrics", &[(1000, 10.0), (2000, 20.0), (4000, 40.0)]);

    let buckets = eng.aggregate_by_bucket("metrics", &TimeRange::new(0, 5000), 1000);

    let starts: Vec<i64> = buckets.iter().map(|(b, _, _, _, _)| *b).collect();
    assert!(starts.contains(&1000), "missing bucket at 1000");
    assert!(starts.contains(&2000), "missing bucket at 2000");
    assert!(starts.contains(&4000), "missing bucket at 4000");
    // Bucket 3000 has no data and `aggregate_by_bucket` only returns non-empty
    // buckets; gap-fill is applied by the reads handler, not the engine primitive.
    assert!(
        !starts.contains(&3000),
        "gap bucket 3000 should be absent from engine output"
    );

    let b2k = buckets.iter().find(|(b, _, _, _, _)| *b == 2000).unwrap();
    assert_eq!(b2k.1, 1);
    assert!((b2k.2 - 20.0).abs() < 1e-9);
}

// ── Bitemporal scan with system_as_of_ms cutoff ───────────────────────────────

#[test]
fn ts_scan_system_as_of_cutoff() {
    // Samples at t=1000 (before cutoff) and t=9000 (after cutoff).
    let eng = ingest_samples("metrics", &[(1000, 1.0), (9000, 9.0)]);

    let all = eng.scan("metrics", &TimeRange::new(0, i64::MAX));
    assert_eq!(all.len(), 2);

    // The scan handler applies system_as_of_ms as a proxy filter on ts.
    let cutoff = 5000_i64;
    let filtered: Vec<_> = all.into_iter().filter(|(ts, _, _)| *ts <= cutoff).collect();
    assert_eq!(filtered.len(), 1, "only sample at t=1000 should survive");
    assert_eq!(filtered[0].0, 1000);
    assert!((filtered[0].1 - 1.0).abs() < 1e-9);
}

// ── Gap-fill strategies ───────────────────────────────────────────────────────

/// Bucketed scan over data at 1000, 2000, 4000 with a 1000 ms bucket.
fn gap_fill_scan(gap_fill: &str) -> Result<QueryResult, LiteError> {
    let eng = ingest_samples("metrics", &[(1000, 10.0), (2000, 20.0), (4000, 40.0)]);
    bucket_scan(
        &eng,
        "metrics",
        &BucketScanParams {
            range: TimeRange::new(0, 5000),
            bucket_ms: 1000,
            aggregates: &[],
            gap_fill,
            projection: &[],
            limit: 0,
        },
    )
}

fn bucket_row(result: &QueryResult, bucket: i64) -> Vec<Value> {
    result
        .rows
        .iter()
        .find(|row| row[0] == Value::Integer(bucket))
        .unwrap_or_else(|| panic!("no row for bucket {bucket}"))
        .clone()
}

fn unsupported_detail(result: Result<QueryResult, LiteError>) -> String {
    match result {
        Err(LiteError::Unsupported { detail }) => detail,
        Err(other) => panic!("expected Unsupported, got {other}"),
        Ok(_) => panic!("expected Unsupported, got rows"),
    }
}

#[test]
fn gap_fill_linear_is_refused_naming_the_strategy() {
    let detail = unsupported_detail(gap_fill_scan("linear"));
    assert!(detail.contains("'linear'"), "detail: {detail}");
    assert!(matches!(
        reject_unsupported_gap_fill("linear"),
        Err(LiteError::Unsupported { .. })
    ));
}

#[test]
fn gap_fill_next_is_refused_naming_the_strategy() {
    let detail = unsupported_detail(gap_fill_scan("next"));
    assert!(detail.contains("'next'"), "detail: {detail}");
    assert!(matches!(
        reject_unsupported_gap_fill("next"),
        Err(LiteError::Unsupported { .. })
    ));
}

#[test]
fn gap_fill_unknown_strategy_is_refused_naming_the_strategy() {
    let detail = unsupported_detail(gap_fill_scan("cubic"));
    assert!(detail.contains("'cubic'"), "detail: {detail}");
}

#[test]
fn gap_fill_prev_carries_the_last_bucket_forward() {
    let result = gap_fill_scan("prev").expect("prev scan");
    let filled = bucket_row(&result, 3000);
    let previous = bucket_row(&result, 2000);
    assert_eq!(filled[1..], previous[1..]);
    assert_eq!(filled[1], Value::Integer(1));
}

#[test]
fn gap_fill_null_and_none_emit_null_for_missing_buckets() {
    for strategy in ["null", "none", ""] {
        let result = gap_fill_scan(strategy).expect("scan");
        let filled = bucket_row(&result, 3000);
        assert!(
            filled[1..].iter().all(|v| *v == Value::Null),
            "strategy {strategy:?} filled with {filled:?}"
        );
        let present = bucket_row(&result, 2000);
        assert_eq!(present[1], Value::Integer(1));
    }
}

#[test]
fn gap_fill_numeric_literal_fills_missing_buckets() {
    let result = gap_fill_scan("7.5").expect("numeric scan");
    let filled = bucket_row(&result, 3000);
    assert!(
        filled[1..].iter().all(|v| *v == Value::Float(7.5)),
        "filled with {filled:?}"
    );
}
