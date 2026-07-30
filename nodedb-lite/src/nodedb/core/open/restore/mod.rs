// SPDX-License-Identifier: Apache-2.0

//! Cold-start restore helpers, one module per index that has to come back from
//! storage before `open_inner` can hand out a database.
//!
//! Every helper follows the same recovery discipline: a checkpoint that fails
//! its checksum, or is otherwise unusable, is discarded and reported rather
//! than trusted, so the caller can rebuild from the authoritative source
//! instead of serving a half-restored index.

mod crdt;
mod csr;
mod fts;
mod hnsw;
mod sparse;
mod spatial;
