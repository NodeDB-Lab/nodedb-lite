//! Shared helpers for the integration test crates.
//!
//! Each test crate declares `pub mod common;`. The `pub` path from the crate
//! root makes every helper reachable, so a crate that uses only some helpers
//! gets no `dead_code` or `unused_imports` warnings for the rest.

pub mod array_wire;
pub mod clock;
pub mod harness;
pub mod mock_array;
pub mod mock_delegate;
pub mod ops;
pub mod origin;
pub mod schema;
pub mod sql;
pub mod ws_origin;

pub use clock::{hlc, hlc1, hlc2, replica};
pub use harness::{SyncHarness, make_outbound_harness};
pub use mock_delegate::MockDelegate;
pub use ops::{delete_op, erase_op, put_op};
pub use schema::simple_schema;
pub use ws_origin::MockOrigin;
