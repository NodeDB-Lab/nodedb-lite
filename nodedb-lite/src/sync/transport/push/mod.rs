//! Outbound push loops — each tick drains every engine's outbound queue and
//! writes wire frames to the WebSocket sink. Per-engine push helpers live in
//! sibling modules; the shared send / encode primitives live in `send`; the
//! tick coordinator loops (`delta_push_loop`, `ping_loop`) live in `loops`.

mod columnar;
// `control` is visible within the `transport` module so `transport::tests` can
// drive `push_collection_schemas` / `push_crdt_deltas` directly for the
// schema-before-delta ordering test; not part of the public API.
pub(in crate::sync::transport) mod control;
mod fts;
mod loops;
mod send;
mod spatial;
mod timeseries;
mod vector;

pub(in crate::sync::transport) use loops::{delta_push_loop, ping_loop};
