//! Real-transport definition sync tests — require a live Origin node.
//!
//! Every test in this file is `#[ignore]` because Origin does not yet emit
//! `DefinitionSync` (0x70) frames.  Lite's receive path is wired in
//! `nodedb-lite/src/sync/transport.rs` (lines 317–319) and
//! `nodedb-lite/src/nodedb/sync_delegate.rs` (line 82), but nothing on the
//! Origin side constructs or sends a `DefinitionSyncMsg`.
//!
//! ## What blocks promotion
//!
//! Origin's `nodedb/nodedb/src/control/server/sync/` contains no code that
//! emits `SyncMessageType::DefinitionSync` (0x70).  The type and opcode are
//! defined in `nodedb/nodedb-types/src/sync/wire/timeseries.rs` and
//! `nodedb/nodedb-types/src/sync/wire/frame.rs`, but the Origin DDL handlers
//! for `CREATE FUNCTION`, `CREATE TRIGGER`, and `CREATE PROCEDURE` do not
//! post a `DefinitionSyncMsg` to any connected Lite session.
//!
//! ## How to promote
//!
//! 1. Locate the Origin DDL commit path for functions/triggers/procedures
//!    (likely in `nodedb/nodedb/src/control/server/` or the event plane's
//!    post-DDL hook).
//! 2. After each successful DDL commit, encode a `DefinitionSyncMsg` with
//!    `action = "put"` (or `"delete"` for DROP) and broadcast it to all
//!    sessions subscribed to the affected namespace.
//! 3. Remove `#[ignore]` from the tests below and run:
//!    `cargo nextest run -p nodedb-lite definition_sync_interop`
//! 4. Update `docs/lite-support-matrix.md`: change "Definition sync" from
//!    EXPERIMENTAL — NOT IN 0.1.0-beta.1 to PREVIEW once the round-trip gate
//!    passes; to BETA after the full suite passes.

mod common;

/// Smoke test: Origin creates a function; Lite receives the DefinitionSync
/// frame and stores the definition locally.
///
/// Ignored until Origin emits `SyncMessageType::DefinitionSync` (0x70) from
/// its DDL commit path.
#[test]
#[ignore = "Origin does not emit DefinitionSync frames; see module doc"]
fn definition_sync_function_put() {
    let _origin = common::origin::OriginServer::spawn();
    // When unignored this test should:
    // 1. Connect a Lite sync client to _origin.sync_addr().
    // 2. Issue `CREATE FUNCTION ...` against Origin via pgwire or HTTP.
    // 3. Assert Lite receives a DefinitionSync frame with action = "put".
    // 4. Assert the function definition is persisted in the Lite catalog.
    todo!(
        "implement once Origin emits DefinitionSyncMsg from DDL commit path; \
         see nodedb/nodedb/src/control/server/sync/ and the DDL handlers"
    );
}

/// Smoke test: Origin drops a function; Lite receives the DefinitionSync
/// frame and removes the definition locally.
///
/// Ignored until Origin emits `SyncMessageType::DefinitionSync` (0x70) for
/// DROP operations.
#[test]
#[ignore = "Origin does not emit DefinitionSync frames; see module doc"]
fn definition_sync_function_delete() {
    let _origin = common::origin::OriginServer::spawn();
    // When unignored this test should:
    // 1. Seed Origin with a function via a prior CREATE FUNCTION.
    // 2. Connect a Lite sync client that receives the put frame.
    // 3. Issue `DROP FUNCTION ...` against Origin.
    // 4. Assert Lite receives a DefinitionSync frame with action = "delete".
    // 5. Assert the function definition is absent from the Lite catalog.
    todo!(
        "implement once Origin emits DefinitionSyncMsg for DROP; \
         see nodedb/nodedb/src/control/server/sync/ and DDL handlers"
    );
}

/// Smoke test: Origin creates a trigger; Lite receives and persists it.
///
/// Ignored for the same reason as `definition_sync_function_put`.
#[test]
#[ignore = "Origin does not emit DefinitionSync frames; see module doc"]
fn definition_sync_trigger_put() {
    let _origin = common::origin::OriginServer::spawn();
    todo!(
        "implement once Origin emits DefinitionSyncMsg from DDL commit path; \
         see nodedb/nodedb/src/control/server/sync/ and DDL handlers"
    );
}

/// Smoke test: Origin creates a procedure; Lite receives and persists it.
///
/// Ignored for the same reason as `definition_sync_function_put`.
#[test]
#[ignore = "Origin does not emit DefinitionSync frames; see module doc"]
fn definition_sync_procedure_put() {
    let _origin = common::origin::OriginServer::spawn();
    todo!(
        "implement once Origin emits DefinitionSyncMsg from DDL commit path; \
         see nodedb/nodedb/src/control/server/sync/ and DDL handlers"
    );
}
