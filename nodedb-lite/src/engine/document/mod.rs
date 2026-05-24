// SPDX-License-Identifier: Apache-2.0

//! Document engine components for NodeDB-Lite.

pub mod history;

pub use history::ops::{
    is_bitemporal, set_bitemporal, versioned_get_as_of, versioned_get_current, versioned_put,
    versioned_tombstone,
};
pub use history::value::{DecodedVersion, VersionTag};
