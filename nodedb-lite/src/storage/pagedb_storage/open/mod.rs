// SPDX-License-Identifier: Apache-2.0

//! Constructors, one per VFS backing.

pub(crate) mod memory;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod opfs;
