// SPDX-License-Identifier: Apache-2.0

//! CRDT sync to an Origin server.

use std::os::raw::c_char;

use crate::handle::NodeDbHandle;
use crate::status::{NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK};
use crate::util::{ffi_guard, handle_ref, ptr_to_str};

/// Start background CRDT sync to an Origin server.
///
/// Connects via WebSocket to the given URL, authenticates with the JWT token,
/// and continuously pushes pending deltas / receives shape updates.
/// Runs forever in the background with auto-reconnect.
///
/// Returns `NODEDB_OK` on successful launch (sync runs asynchronously).
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

        let config = nodedb_lite::sync::SyncConfig {
            url: url_str.to_string(),
            jwt_token: jwt_str.to_string(),
            client_version: format!("nodedb-lite-ffi/{}", env!("CARGO_PKG_VERSION")),
            min_backoff: std::time::Duration::from_secs(1),
            max_backoff: std::time::Duration::from_secs(60),
            ping_interval: std::time::Duration::from_secs(30),
            max_batch_size: 100,
            token_provider: None,
            token_lifetime_secs: 0,
        };

        // start_sync requires a tokio runtime context for spawning the background task.
        let _guard = h.rt.enter();
        let _sync_client = h.db.start_sync(config);

        NODEDB_OK
    })
}
