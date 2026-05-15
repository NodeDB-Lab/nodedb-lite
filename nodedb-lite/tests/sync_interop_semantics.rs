//! §8 — Handshake and vector-clock semantic tests.
//!
//! Verifies the behavioural contract between Lite and a real Origin server:
//!
//!   §8.1  Global-clock encoding accepted, resume semantics preserved.
//!   §8.2  Compatibility sentinel: fails loudly on field/version divergence.
//!   §8.3  Fork detection scenarios (lite_id + epoch).
//!
//! All tests run in the `heavy` nextest group (serialised, port 9090).

mod common;
mod semantics;
