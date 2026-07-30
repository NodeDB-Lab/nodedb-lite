// SPDX-License-Identifier: Apache-2.0

//! Status codes shared by every FFI entry point.

/// Error codes returned by FFI functions.
pub const NODEDB_OK: i32 = 0;
pub const NODEDB_ERR_NULL: i32 = -1;
pub const NODEDB_ERR_UTF8: i32 = -2;
pub const NODEDB_ERR_FAILED: i32 = -3;
pub const NODEDB_ERR_NOT_FOUND: i32 = -4;
