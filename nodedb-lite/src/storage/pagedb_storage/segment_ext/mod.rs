// SPDX-License-Identifier: Apache-2.0

//! Segment-backed trait impls carried by `PagedbStorage`.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod array;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod vector;
