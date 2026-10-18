//! The native engine of typeline, which reads dataclasses from delimited text and writes them back.
//!
//! The Python bindings are built with the `python` feature; the rest is plain Rust.

pub mod dates;
pub mod rows;
pub mod text;

#[cfg(feature = "python")]
mod python;
