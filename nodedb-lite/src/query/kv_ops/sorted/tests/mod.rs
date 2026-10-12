// SPDX-License-Identifier: Apache-2.0
//! Sorted-index behavior end to end: registration through the physical
//! visitor, maintenance by every KV write path, reads, and lifecycle.

mod harness;
mod lifecycle;
mod reads;
mod windows;
mod writes;
