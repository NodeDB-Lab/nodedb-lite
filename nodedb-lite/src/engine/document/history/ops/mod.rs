// SPDX-License-Identifier: Apache-2.0

//! Async storage operations for versioned document history.
//!
//! These functions are the write and read primitives for bitemporal document
//! collections, split by concern:
//!
//! - [`flags`]    — the collection-level bitemporal flag in `Namespace::Meta`.
//! - [`write`]    — appending live versions and tombstones.
//! - [`read`]     — resolving the current version and point-in-time lookups.
//! - [`backfill`] — rebuilding the `LatestVersion` index for legacy databases.

pub mod backfill;
pub mod flags;
pub mod read;
pub mod write;

pub use backfill::backfill_latest_version;
pub use flags::{is_bitemporal, set_bitemporal};
pub use read::{scan_live_documents, versioned_get_as_of, versioned_get_current};
pub use write::{versioned_put, versioned_tombstone};
