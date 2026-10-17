//! Reading records natively from rows of delimited text.

use std::collections::HashMap;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyIterator, PyList, PyString, PyTuple, PyType,
};

use crate::rows::{CHUNK, Event, Options, Row, Rows, Source};
use crate::text::{json_string, looks_like_json, on_line, plain_float, plain_int};

/// How a field is read from the text of its column.
#[derive(Clone, Copy)]
enum Lane {
    /// Keep the text as it is.
    Text,
    /// Parse the text as an integer.
    Int,
    /// Parse the text as a float.
    Float,
    /// Parse the text as `true` or `false`.
    Bool,
    /// Look the text up in a table, e.g. of an enum's members by value.
    Lookup,
    /// Decode the text as JSON with the field's typed decoder.
    Decode,
    /// Hand the text to the field's reader.
    Call,
}

impl Lane {
    fn named(name: &str) -> PyResult<Self> {
        match name {
            "text" => Ok(Self::Text),
            "int" => Ok(Self::Int),
            "float" => Ok(Self::Float),
            "bool" => Ok(Self::Bool),
            "lookup" => Ok(Self::Lookup),
            "decode" => Ok(Self::Decode),
            "call" => Ok(Self::Call),
            _ => Err(PyValueError::new_err(format!("Unknown lane '{name}'!"))),
        }
    }
}

/// The text of a field: a slice of a row read natively, or a string from Python.
enum Cell<'a, 'py> {
    Slice(&'a str),
    Object(Bound<'py, PyAny>),
}

impl<'py> Cell<'_, 'py> {
    /// The text of the field, unless it is not a string that can be read as UTF-8.
    fn text(&self) -> Option<&str> {
        match self {
            Cell::Slice(text) => Some(text),
            Cell::Object(object) => object.cast::<PyString>().ok()?.to_str().ok(),
        }
    }

    /// The field as a Python object.
    fn object(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self {
            Cell::Slice(text) => PyString::new(py, text).into_any(),
            Cell::Object(object) => object.clone(),
        }
    }
}

/// A field of a record, the column it is read from, and how.
///
/// A field that is not read in its lane is decoded by its typed decoder, as JSON when its text
/// looks like JSON and else as a JSON string. Whatever fails there is handed to its reader, which
/// reads the text as typeline always has and explains why it cannot.
struct Field {
    name: Py<PyString>,
    column: usize,
    lane: Lane,
    none: Option<String>,
    json_like: bool,
    table: HashMap<String, Py<PyAny>>,
    decode: Option<Py<PyAny>>,
    read: Option<Py<PyAny>>,
}

impl Field {
    /// Reads the value of this field from the text of its column.
    fn read<'py>(
        &self,
        py: Python<'py>,
        cell: &Cell<'_, 'py>,
        line_number: Option<u64>,
        decode_error: &Bound<'py, PyType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(text) = cell.text() else {
            return self.fall_back(cell.object(py), line_number);
        };
        if self.none.as_deref() == Some(text) {
            return Ok(py.None().into_bound(py));
        }
        match self.lane {
            Lane::Text => return Ok(cell.object(py)),
            Lane::Call => return self.fall_back(cell.object(py), line_number),
            Lane::Int => {
                if let Some(value) = plain_int(text) {
                    return Ok(value.into_pyobject(py)?.into_any());
                }
            }
            Lane::Float => {
                if let Some(value) = plain_float(text) {
                    return Ok(PyFloat::new(py, value).into_any());
                }
            }
            Lane::Bool => match text {
                "true" => return Ok(PyBool::new(py, true).to_owned().into_any()),
                "false" => return Ok(PyBool::new(py, false).to_owned().into_any()),
                _ => {}
            },
            Lane::Lookup => {
                if let Some(value) = self.table.get(text) {
                    return Ok(value.bind(py).clone());
                }
            }
            Lane::Decode => {}
        }
        let Some(decode) = &self.decode else {
            return self.fall_back(cell.object(py), line_number);
        };
        let json = if self.json_like && looks_like_json(text) {
            PyBytes::new(py, text.as_bytes())
        } else {
            PyBytes::new(py, &json_string(text))
        };
        match decode.bind(py).call1((json,)) {
            Err(error) if error.is_instance(py, decode_error) => {
                self.fall_back(cell.object(py), line_number)
            }
            decoded => decoded,
        }
    }

    /// Reads the text with the field's reader, or keeps it when the field has none.
    fn fall_back<'py>(
        &self,
        text: Bound<'py, PyAny>,
        line_number: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match &self.read {
            Some(read) => read.bind(text.py()).call1((text, line_number)),
            None => Ok(text),
        }
    }
}

/// The field that holds the columns no other field takes.
struct Extra {
    name: Py<PyString>,
    columns: Vec<usize>,
    rest: usize,
}

/// Builds records of one type from rows of text, one field per column.
///
/// A record is built as msgspec builds a dataclass: its fields are set without calling
/// `__init__`, and then `__post_init__` is called if the type has one.
#[pyclass(frozen, module = "typeline._native")]
pub struct RecordBuilder {
    record_type: Py<PyType>,
    alloc: ffi::allocfunc,
    post_init: Option<Py<PyAny>>,
    fields: Vec<Field>,
    width: usize,
    extra: Option<Extra>,
    row_fields: Option<Py<PyAny>>,
    file_name: Option<String>,
    decode_error: Py<PyType>,
    validation_error: Py<PyType>,
}

/// The description of a field that Python hands the builder.
type FieldSpec<'py> = (
    Bound<'py, PyString>,
    usize,
    String,
    Option<String>,
    bool,
    Option<Bound<'py, PyDict>>,
    Option<Bound<'py, PyAny>>,
    Option<Bound<'py, PyAny>>,
);

#[pymethods]
impl RecordBuilder {
    #[new]
    #[pyo3(signature = (
        record_type, fields, width, extra, row_fields, file_name, decode_error, validation_error
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        record_type: Bound<'_, PyType>,
        fields: Vec<FieldSpec<'_>>,
        width: usize,
        extra: Option<(Bound<'_, PyString>, Vec<usize>, usize)>,
        row_fields: Option<Bound<'_, PyAny>>,
        file_name: Option<String>,
        decode_error: Bound<'_, PyType>,
        validation_error: Bound<'_, PyType>,
    ) -> PyResult<Self> {
        // SAFETY: every type has a `tp_alloc` slot, which takes the type and a count of items.
        let alloc = unsafe {
            let slot = ffi::PyType_GetSlot(record_type.as_type_ptr(), ffi::Py_tp_alloc);
            if slot.is_null() {
                return Err(PyTypeError::new_err(
                    "Records of this type cannot be allocated!",
                ));
            }
            std::mem::transmute::<*mut std::ffi::c_void, ffi::allocfunc>(slot)
        };
        let post_init = record_type.getattr("__post_init__").ok().map(Bound::unbind);
        let fields = fields
            .into_iter()
            .map(
                |(name, column, lane, none, json_like, table, decode, read)| {
                    let table = match table {
                        Some(table) => table
                            .iter()
                            .map(|(key, value)| Ok((key.extract::<String>()?, value.unbind())))
                            .collect::<PyResult<_>>()?,
                        None => HashMap::new(),
                    };
                    Ok(Field {
                        name: name.unbind(),
                        column,
                        lane: Lane::named(&lane)?,
                        none,
                        json_like,
                        table,
                        decode: decode.map(Bound::unbind),
                        read: read.map(Bound::unbind),
                    })
                },
            )
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            record_type: record_type.unbind(),
            alloc,
            post_init,
            fields,
            width,
            extra: extra.map(|(name, columns, rest)| Extra {
                name: name.unbind(),
                columns,
                rest,
            }),
            row_fields: row_fields.map(Bound::unbind),
            file_name,
            decode_error: decode_error.unbind(),
            validation_error: validation_error.unbind(),
        })
    }

    /// Build a record from a row, naming the line it starts on in errors.
    #[pyo3(signature = (row, line_number))]
    fn build<'py>(
        &self,
        py: Python<'py>,
        row: &Bound<'py, PyList>,
        line_number: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cells: Vec<Cell<'_, 'py>> = row.iter().map(Cell::Object).collect();
        self.build_cells(py, &cells, line_number)
    }
}

impl RecordBuilder {
    /// Builds a record from the cells of a row.
    fn build_cells<'py>(
        &self,
        py: Python<'py>,
        cells: &[Cell<'_, 'py>],
        line_number: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let found = cells.len();
        if found != self.width && (self.extra.is_none() || found < self.width) {
            let at_least = if self.extra.is_none() {
                ""
            } else {
                "at least "
            };
            return Err(PyValueError::new_err(format!(
                "Expected {at_least}{} columns but found {found}{} for record type: {}.",
                self.width,
                on_line(line_number, self.file_name.as_deref()),
                self.record_type.bind(py).name()?,
            )));
        }
        let decode_error = self.decode_error.bind(py);
        let record = self.allocate(py)?;
        for field in &self.fields {
            let value = field.read(py, &cells[field.column], line_number, decode_error)?;
            set_field(&record, field.name.bind(py), &value)?;
        }
        if let Some(extra) = &self.extra {
            let texts: Vec<Bound<'py, PyAny>> = extra
                .columns
                .iter()
                .copied()
                .chain(extra.rest..found)
                .map(|column| cells[column].object(py))
                .collect();
            set_field(
                &record,
                extra.name.bind(py),
                PyTuple::new(py, texts)?.as_any(),
            )?;
        }
        if let Some(row_fields) = &self.row_fields {
            let row = PyList::new(py, cells.iter().map(|cell| cell.object(py)))?;
            let values = row_fields.bind(py).call1((row, line_number))?;
            for (name, value) in values.cast::<PyDict>()?.iter() {
                set_field(&record, name.cast::<PyString>()?, &value)?;
            }
        }
        if let Some(post_init) = &self.post_init {
            post_init
                .bind(py)
                .call1((&record,))
                .map_err(|error| self.invalid(py, error))?;
        }
        Ok(record)
    }

    /// Allocates a record without calling its `__init__`.
    fn allocate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // SAFETY: `alloc` is the record type's own `tp_alloc`, called with that type.
        unsafe {
            let record = (self.alloc)(self.record_type.as_ptr().cast(), 0);
            Bound::from_owned_ptr_or_err(py, record)
        }
    }

    /// Raises errors of `__post_init__` as msgspec does: a `TypeError` or `ValueError` is invalid.
    fn invalid(&self, py: Python<'_>, error: PyErr) -> PyErr {
        if !(error.is_instance_of::<PyTypeError>(py) || error.is_instance_of::<PyValueError>(py)) {
            return error;
        }
        let message = error
            .value(py)
            .str()
            .map_or_else(|_| String::new(), |text| text.to_string());
        match self.validation_error.bind(py).call1((message,)) {
            Ok(invalid) => {
                let invalid = PyErr::from_value(invalid);
                invalid.set_cause(py, Some(error));
                invalid
            }
            Err(failed) => failed,
        }
    }
}

/// Sets a field of a record, even a frozen one, as `object.__setattr__` does.
fn set_field(
    record: &Bound<'_, PyAny>,
    name: &Bound<'_, PyString>,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    // SAFETY: all three pointers are live objects held by the caller.
    let status =
        unsafe { ffi::PyObject_GenericSetAttr(record.as_ptr(), name.as_ptr(), value.as_ptr()) };
    if status < 0 {
        Err(PyErr::fetch(record.py()))
    } else {
        Ok(())
    }
}

/// Text read from a Python text stream, in chunks when it can `read`, else line by line.
struct Stream {
    handle: Py<PyAny>,
    read: Option<Py<PyAny>>,
    lines: Option<Py<PyIterator>>,
}

impl Source for Stream {
    type Error = PyErr;

    fn read(&mut self, text: &mut String) -> PyResult<bool> {
        Python::attach(|py| {
            let chunk = match (&self.read, &self.lines) {
                (Some(read), _) => read.bind(py).call1((CHUNK,))?,
                (None, Some(lines)) => match lines.bind(py).clone().next() {
                    Some(line) => line?,
                    None => return Ok(false),
                },
                (None, None) => return Ok(false),
            };
            let Ok(chunk) = chunk.cast::<PyString>() else {
                return Err(PyTypeError::new_err(format!(
                    "Delimited data must be read as text, not {}!",
                    chunk.get_type().name()?
                )));
            };
            if self.read.is_some() && chunk.is_empty()? {
                return Ok(false);
            }
            text.push_str(chunk.to_str()?);
            Ok(true)
        })
    }
}

/// Reads records natively from a Python text stream, one row at a time.
///
/// Comments between rows are handed to `on_comment` as they are reached. Once the stream is read
/// to its end, or reading it fails, it is closed if `close_when_read` is set.
#[pyclass(module = "typeline._native")]
pub struct RowReader {
    rows: Rows<Stream>,
    on_comment: Option<Py<PyAny>>,
    builder: Option<Py<RecordBuilder>>,
    invalid: Option<Py<PyAny>>,
    validation_error: Option<Py<PyType>>,
    #[pyo3(get, set)]
    close_when_read: bool,
    done: bool,
}

#[pymethods]
impl RowReader {
    #[new]
    #[pyo3(signature = (handle, delimiter, quoting, comment_prefixes, on_comment))]
    fn new(
        handle: Bound<'_, PyAny>,
        delimiter: char,
        quoting: bool,
        comment_prefixes: Vec<String>,
        on_comment: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let Ok(delimiter) = u8::try_from(delimiter).map(|byte| byte.is_ascii().then_some(byte))
        else {
            return Err(PyValueError::new_err(format!(
                "The delimiter must be one ASCII character, not {delimiter:?}!"
            )));
        };
        let Some(delimiter) = delimiter else {
            return Err(PyValueError::new_err(
                "The delimiter must be one ASCII character!",
            ));
        };
        let read = handle.getattr("read").ok().map(Bound::unbind);
        let lines = if read.is_none() {
            Some(handle.try_iter()?.unbind())
        } else {
            None
        };
        let stream = Stream {
            handle: handle.unbind(),
            read,
            lines,
        };
        let options = Options {
            delimiter,
            quoting,
            comment_prefixes,
        };
        Ok(Self {
            rows: Rows::new(stream, options),
            on_comment: on_comment.map(Bound::unbind),
            builder: None,
            invalid: None,
            validation_error: None,
            close_when_read: false,
            done: false,
        })
    }

    /// Read the next row, e.g. a header, as its fields and the line it starts on.
    fn read_row(&mut self, py: Python<'_>) -> PyResult<Option<(Vec<String>, u64)>> {
        loop {
            match self.rows.read()? {
                None => return Ok(None),
                Some(Event::Comment { line, text }) => {
                    if let Some(on_comment) = &self.on_comment {
                        on_comment.bind(py).call1((line, text))?;
                    }
                }
                Some(Event::Row { line, row }) => {
                    return Ok(Some((row.iter().map(str::to_string).collect(), line)));
                }
            }
        }
    }

    /// Build records with a builder, explaining an invalid record with `invalid`.
    fn bind(
        &mut self,
        builder: Py<RecordBuilder>,
        invalid: Py<PyAny>,
        validation_error: Py<PyType>,
    ) {
        self.builder = Some(builder);
        self.invalid = Some(invalid);
        self.validation_error = Some(validation_error);
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        if self.done {
            return Ok(None);
        }
        let next = self.next_record(py);
        if !matches!(next, Ok(Some(_))) {
            self.done = true;
            if self.close_when_read {
                self.rows.source().handle.bind(py).call_method0("close")?;
            }
        }
        next
    }
}

impl RowReader {
    /// Reads the next record, handing comments before it to `on_comment`.
    fn next_record<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let Some(builder) = &self.builder else {
            return Err(PyValueError::new_err("No record type to build records of!"));
        };
        let builder = builder.get();
        loop {
            match self.rows.read()? {
                None => return Ok(None),
                Some(Event::Comment { line, text }) => {
                    if let Some(on_comment) = &self.on_comment {
                        on_comment.bind(py).call1((line, text))?;
                    }
                }
                Some(Event::Row { line, row }) => {
                    let cells: Vec<Cell<'_, 'py>> = row.iter().map(Cell::Slice).collect();
                    return match builder.build_cells(py, &cells, Some(line)) {
                        Ok(record) => Ok(Some(record)),
                        Err(error) => Err(explain(
                            py,
                            error,
                            row,
                            line,
                            self.invalid.as_ref(),
                            self.validation_error.as_ref(),
                        )),
                    };
                }
            }
        }
    }
}

/// Explains why a record could not be built from a row, when it was invalid.
fn explain(
    py: Python<'_>,
    error: PyErr,
    row: Row<'_>,
    line: u64,
    invalid: Option<&Py<PyAny>>,
    validation_error: Option<&Py<PyType>>,
) -> PyErr {
    let (Some(invalid), Some(validation_error)) = (invalid, validation_error) else {
        return error;
    };
    if !error.is_instance(py, validation_error.bind(py)) {
        return error;
    }
    let explained = PyList::new(py, row.iter())
        .and_then(|row| invalid.bind(py).call1((row, line, error.value(py))));
    match explained {
        Ok(explained) => {
            let explained = PyErr::from_value(explained);
            explained.set_cause(py, Some(error));
            explained
        }
        Err(failed) => failed,
    }
}
