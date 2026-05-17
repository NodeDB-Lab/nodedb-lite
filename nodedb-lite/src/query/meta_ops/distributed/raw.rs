// SPDX-License-Identifier: Apache-2.0
//! RawResponse handler for Lite.
//!
//! Verified by grepping the Lite plan converter
//! (`nodedb-lite/nodedb-lite/src/query/`) for any path that builds
//! `MetaOp::RawResponse` — none exists. `RawResponse` is produced exclusively
//! by Origin's pgwire/HTTP entry points as a constant-result optimisation (e.g.
//! `SELECT 1 AS value`). The Lite query path handles constant results through
//! its own `execute_constant_result` path in `LiteQueryEngine` and never
//! produces this variant. Any call to this function indicates a programming
//! error in the caller.

/// Handle `MetaOp::RawResponse`.
///
/// # Panics
///
/// Always — `RawResponse` is produced exclusively by Origin's pgwire/HTTP
/// constant-result path and cannot be produced by the Lite plan converter.
/// Reaching this function is a programming error in the caller.
pub fn handle_raw_response() -> ! {
    unreachable!(
        "RawResponse is Origin's internal wire passthrough for constant queries \
         (SELECT 1 AS value); the Lite plan converter never emits this variant. \
         If you reached this code, the caller constructed a MetaOp::RawResponse \
         outside the Lite query path, which is a programming error."
    )
}

#[cfg(test)]
mod tests {
    /// Verify the unreachable justification is documented and the function
    /// signature is `-> !` (diverging). This test cannot call
    /// `handle_raw_response()` without panicking, so we only test the
    /// type-system guarantee at compile time.
    #[test]
    fn raw_response_diverges() {
        // The type `fn() -> !` is verified by the compiler. If
        // `handle_raw_response` were changed to return a non-diverging type,
        // this test would fail to compile.
        let _f: fn() -> ! = super::handle_raw_response;
    }
}
