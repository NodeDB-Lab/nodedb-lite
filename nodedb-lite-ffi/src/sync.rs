// SPDX-License-Identifier: Apache-2.0

//! CRDT sync to an Origin server.

use std::os::raw::c_char;

use nodedb_lite::sync::SyncConfig;

use crate::error::record_error;
use crate::handle::NodeDbHandle;
use crate::status::{NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK};
use crate::util::{ffi_guard, handle_ref, ptr_to_str};

/// Start background CRDT sync to an Origin server.
///
/// Connects via WebSocket to the given URL, authenticates with the JWT token,
/// and continuously pushes pending deltas / receives shape updates.
/// Reconnects on its own.
///
/// Returns `NODEDB_OK` on successful launch (sync runs asynchronously), or
/// `NODEDB_ERR_FAILED` when sync is already running on this handle.
///
/// The database owns the task: `nodedb_close` stops it before tearing down
/// the runtime, and `nodedb_stop_sync` stops it on demand.
///
/// # Safety
/// `url` and `jwt_token` must be valid null-terminated UTF-8 strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_start_sync(
    handle: *mut NodeDbHandle,
    url: *const c_char,
    jwt_token: *const c_char,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(url_str) = ptr_to_str(url) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(jwt_str) = ptr_to_str(jwt_token) else {
            return NODEDB_ERR_UTF8;
        };

        let config = SyncConfig {
            url: url_str.to_string(),
            jwt_token: jwt_str.to_string(),
            client_version: format!("nodedb-lite-ffi/{}", env!("CARGO_PKG_VERSION")),
            min_backoff: std::time::Duration::from_secs(1),
            max_backoff: std::time::Duration::from_secs(60),
            ping_interval: std::time::Duration::from_secs(30),
            max_batch_size: 100,
            token_provider: None,
            token_lifetime_secs: 0,
            tenant_id: 1,
        };

        // start_sync spawns the loop, so it needs a runtime context.
        let _guard = h.rt.enter();
        if h.db.start_sync(config).is_none() {
            record_error(
                "sync is already running on this handle: stop it with \
                 nodedb_stop_sync before starting it again",
            );
            return NODEDB_ERR_FAILED;
        }

        NODEDB_OK
    })
}

/// Stop background sync started by `nodedb_start_sync`.
///
/// The database handle stays open and usable, and sync can be restarted with
/// a new token. Blocks until the sync task has wound down.
///
/// Returns `NODEDB_OK` if sync was running and was stopped,
/// `NODEDB_ERR_FAILED` if no sync was active, `NODEDB_ERR_NULL` for a NULL
/// handle.
///
/// # Safety
/// `handle` must be a valid handle returned by `nodedb_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_stop_sync(handle: *mut NodeDbHandle) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        if !h.rt.block_on(h.db.stop_sync()) {
            record_error("no sync task is running on this handle");
            return NODEDB_ERR_FAILED;
        }
        NODEDB_OK
    })
}
