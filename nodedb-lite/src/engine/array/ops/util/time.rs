// SPDX-License-Identifier: Apache-2.0

//! Wall-clock helpers for Array op handlers.

/// Current Unix time in milliseconds, as the `i64` used throughout the
/// Array engine for system/valid time. Saturates to `0` if the clock is
/// before the Unix epoch.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
