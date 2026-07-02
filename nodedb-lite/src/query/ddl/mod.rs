//! DDL statement interception and dispatch for the Lite query engine.

pub mod alter;
pub mod columnar;
pub mod continuous_agg;
pub mod convert;
pub mod dispatch;
pub mod document;
pub mod engine_meta;
pub mod htap;
pub mod kv;
pub mod parser;
pub mod strict;
#[cfg(test)]
mod tests;
pub mod timeseries;

pub(crate) use parser::describe_strict_collection;
