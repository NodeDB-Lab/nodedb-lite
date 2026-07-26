// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` constructors and cold-start restore helpers.

mod constructors;
// Sync outbound queue wiring is native-only — Lite's sync path is compiled
// out entirely on wasm32, so this whole module would otherwise be dead code.
#[cfg(not(target_arch = "wasm32"))]
mod outbound;
mod restore;
