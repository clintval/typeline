//! The `typeline._native` extension module, which reads records from delimited text and writes
//! them back.

use pyo3::prelude::*;

mod read;
mod write;

/// The native engine of typeline.
#[pymodule(gil_used = false)]
mod _native {
    #[pymodule_export]
    use super::read::{RecordBuilder, RowReader};

    #[pymodule_export]
    use super::write::RecordFormatter;
}
