// SPDX-License-Identifier: Apache-2.0
//! Time windows of sorted indexes.
//!
//! A windowed index stores an entry for every row whatever its timestamp,
//! and a read keeps the entries stamped at or after the current window's
//! start. Window starts follow UTC:
//!
//! - daily: midnight;
//! - weekly: Monday 00:00;
//! - monthly: the 1st 00:00;
//! - custom: `start_ms` while now is in `[start_ms, end_ms)`, and no entry
//!   at all outside that range.

use std::collections::HashMap;

use nodedb_types::value::Value;

use crate::index::sorted_def::{WindowDef, WindowKind};

use super::keys::{FIELD_LEN, complement, score_prefix};

const MS_PER_DAY: u64 = 86_400_000;

/// Build the score entry key of a windowed index:
/// `{score_prefix}{score:8}{ts:8}{pk}`. Both fields are key-order bytes.
pub(super) fn windowed_score_key(
    index_name: &str,
    score: &[u8; FIELD_LEN],
    ts: &[u8; FIELD_LEN],
    pk: &[u8],
) -> Vec<u8> {
    let mut key = score_prefix(index_name);
    key.extend_from_slice(score);
    key.extend_from_slice(ts);
    key.extend_from_slice(pk);
    key
}

/// Read the timestamp column `column` of a row's typed columns as ms since
/// epoch. `None` when the column is absent, not numeric, or negative.
pub(super) fn extract_ts_from_value(columns: &HashMap<String, Value>, column: &str) -> Option<u64> {
    match columns.get(column)? {
        Value::Integer(n) => u64::try_from(*n).ok(),
        Value::Float(f) if *f >= 0.0 && f.is_finite() => Some(*f as u64),
        _ => None,
    }
}

/// The key-order bytes of a timestamp: big-endian, complemented when the
/// timestamp sorts descending.
pub(super) fn encode_ts(ts_ms: u64, desc: bool) -> [u8; FIELD_LEN] {
    let bytes = ts_ms.to_be_bytes();
    if desc { complement(bytes) } else { bytes }
}

/// Reverse [`encode_ts`].
pub(super) fn decode_ts(bytes: &[u8; FIELD_LEN], desc: bool) -> u64 {
    let bytes = if desc { complement(*bytes) } else { *bytes };
    u64::from_be_bytes(bytes)
}

/// The start (inclusive) of the window current at `now_ms`. `None` for an
/// index with no window: every entry is visible.
pub(super) fn window_start(window: &WindowDef, now_ms: u64) -> Option<u64> {
    match window.kind {
        WindowKind::None => None,
        WindowKind::Daily => Some(start_of_day(now_ms)),
        WindowKind::Weekly => Some(start_of_week(now_ms)),
        WindowKind::Monthly => Some(start_of_month(now_ms)),
        WindowKind::Custom => {
            if now_ms >= window.start_ms && now_ms < window.end_ms {
                Some(window.start_ms)
            } else {
                Some(u64::MAX)
            }
        }
    }
}

fn start_of_day(now_ms: u64) -> u64 {
    (now_ms / MS_PER_DAY) * MS_PER_DAY
}

fn start_of_week(now_ms: u64) -> u64 {
    let days = now_ms / MS_PER_DAY;
    // 1970-01-01 was a Thursday: (days + 3) % 7 is 0 on a Monday.
    let since_monday = (days + 3) % 7;
    (days - since_monday) * MS_PER_DAY
}

fn start_of_month(now_ms: u64) -> u64 {
    let days = (now_ms / MS_PER_DAY) as i64;
    let (year, month) = civil_year_month(days);
    days_from_civil(year, month, 1) as u64 * MS_PER_DAY
}

/// The `(year, month)` of a day count since 1970-01-01 (proleptic
/// Gregorian, Howard Hinnant's `civil_from_days`).
fn civil_year_month(days: i64) -> (i64, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

/// The day count since 1970-01-01 of a civil date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year.rem_euclid(400);
    let month = i64::from(month);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(kind: WindowKind, start_ms: u64, end_ms: u64) -> WindowDef {
        WindowDef {
            kind,
            timestamp_column: "ts".into(),
            timestamp_desc: false,
            start_ms,
            end_ms,
        }
    }

    // 2024-05-02 (a Thursday) 15:30:00 UTC.
    const NOW: u64 = 1_714_663_800_000;

    #[test]
    fn calendar_windows_start_at_utc_boundaries() {
        // 2024-05-02 00:00, 2024-04-29 (Monday) 00:00, 2024-05-01 00:00 UTC.
        assert_eq!(
            window_start(&window(WindowKind::Daily, 0, 0), NOW),
            Some(1_714_608_000_000)
        );
        assert_eq!(
            window_start(&window(WindowKind::Weekly, 0, 0), NOW),
            Some(1_714_348_800_000)
        );
        assert_eq!(
            window_start(&window(WindowKind::Monthly, 0, 0), NOW),
            Some(1_714_521_600_000)
        );
        assert_eq!(window_start(&WindowDef::none(), NOW), None);
    }

    #[test]
    fn month_start_handles_january_and_leap_february() {
        // 2024-01-15 and 2024-02-29 12:00 UTC.
        assert_eq!(start_of_month(1_705_320_000_000), 1_704_067_200_000);
        assert_eq!(start_of_month(1_709_208_000_000), 1_706_745_600_000);
    }

    #[test]
    fn custom_window_is_empty_outside_its_range() {
        let custom = window(WindowKind::Custom, 1_000, 2_000);
        assert_eq!(window_start(&custom, 1_500), Some(1_000));
        assert_eq!(window_start(&custom, 2_000), Some(u64::MAX));
        assert_eq!(window_start(&custom, 999), Some(u64::MAX));
    }

    #[test]
    fn timestamps_round_trip_in_both_directions() {
        for desc in [false, true] {
            assert_eq!(decode_ts(&encode_ts(1_234, desc), desc), 1_234);
        }
        assert!(encode_ts(1, true) > encode_ts(2, true));
    }
}
