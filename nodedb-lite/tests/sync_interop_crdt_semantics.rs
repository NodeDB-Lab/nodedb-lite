//! §12 — CRDT semantics: Origin-side rejection paths and Lite policy resolution.
//!
//! Each test exercises one `CompensationHint` variant or policy-resolution path
//! against a real Origin server.
//!
//! All tests run in the `heavy` nextest group (serialised, port 9090).

mod common;
mod crdt_semantics;
