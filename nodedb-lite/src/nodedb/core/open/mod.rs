// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` constructors and cold-start restore helpers.

mod constructors;
// Sync outbound queue wiring is native-only — Lite's sync path is compiled
// out entirely on wasm32, so this whole module would otherwise be dead code.
#[cfg(not(target_arch = "wasm32"))]
mod outbound;
// The post-open store-recovery driver is native-only: it renames the corrupt
// on-disk store aside and recreates it, which OPFS (wasm32) cannot do.
#[cfg(not(target_arch = "wasm32"))]
mod recovery;
mod restore;
