//! Reading records natively from rows of delimited text.

use std::collections::HashMap;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyInt, PyIterator, PyList, PyString, PyTuple, PyType,
};

use crate::rows::{CHUNK, Event, Options, Rows, Source};
use crate::text::{json_string, looks_like_json, on_line, plain_float, plain_int};

/// How a field is read from the text of its column.
#[derive(Clone, Copy)]
enum Lane {
    /// Keep the text as it is.
    Text,
    /// Read a JSON integer, e.g. `-12`.
    Int,
    /// Read a JSON number, or `nan`, `inf`, or `-inf`, as Python writes them.
    Float,
    /// Read `true` or `false`.
    Bool,
    /// Look the text up in a table, e.g. of an enum's members by value.
    Lookup,
    /// Read the text with a codec.
    Codec,
    /// Decode the text as JSON with the field's typed decoder.
    Decode,
}

impl Lane {
    fn named(name: &str) -> PyResult<Self> {
        match name {
            "text" => Ok(Self::Text),
            "int" => Ok(Self::Int),
            "float" => Ok(Self::Float),
            "bool" => Ok(Self::Bool),
            "lookup" => Ok(Self::Lookup),
            "codec" => Ok(Self::Codec),
            "decode" => Ok(Self::Decode),
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

/// A field of a record held in one column, and how it is read from that column's text.
struct Field {
    column: usize,
    lane: Lane,
    none: Option<String>,
    json_like: bool,
    table: HashMap<String, Py<PyAny>>,
    read: Option<Py<PyAny>>,
    trusted: Option<Py<PyAny>>,
    coerce: Option<Py<PyAny>>,
    label: String,
    place: String,
}

impl Field {
    /// Reads the value of this field from the text of its column.
    fn read<'py>(
        &self,
        py: Python<'py>,
        cell: &Cell<'_, 'py>,
        line_number: Option<u64>,
        file_name: Option<&str>,
        decode_error: &Bound<'py, PyType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(text) = cell.text() else {
            let unreadable = PyValueError::new_err("The text is not valid UTF-8!");
            return Err(self.unreadable(py, "?", line_number, file_name, Some(unreadable)));
        };
        if self.none.as_deref() == Some(text) {
            return Ok(py.None().into_bound(py));
        }
        let read = match self.lane {
            Lane::Text => Some(Ok(cell.object(py))),
            Lane::Int => int(py, text).map(Ok),
            Lane::Float => float(text).map(|value| Ok(PyFloat::new(py, value).into_any())),
            Lane::Bool => match text {
                "true" => Some(Ok(PyBool::new(py, true).to_owned().into_any())),
                "false" => Some(Ok(PyBool::new(py, false).to_owned().into_any())),
                _ => None,
            },
            Lane::Lookup => self.table.get(text).map(|value| Ok(value.bind(py).clone())),
            Lane::Codec => Some(self.codec(py, cell)),
            Lane::Decode => Some(self.decode(py, text, decode_error)),
        };
        match read {
            Some(Ok(value)) => Ok(value),
            Some(Err(cause)) => Err(self.unreadable(py, text, line_number, file_name, Some(cause))),
            None => Err(self.unreadable(py, text, line_number, file_name, None)),
        }
    }

    /// Reads text with the field's codec, keeping a value of the field's class as it is and
    /// coercing any other value to the field's type.
    fn codec<'py>(&self, py: Python<'py>, cell: &Cell<'_, 'py>) -> PyResult<Bound<'py, PyAny>> {
        let (Some(read), Some(coerce)) = (&self.read, &self.coerce) else {
            return Err(PyValueError::new_err("A codec field needs a codec!"));
        };
        let value = read.bind(py).call1((cell.object(py),))?;
        match &self.trusted {
            Some(trusted) if value.get_type().is(trusted.bind(py)) => Ok(value),
            _ => coerce.bind(py).call1((value,)),
        }
    }

    /// Decodes text with the field's typed decoder: as JSON when it looks like JSON, and else,
    /// or when it is not valid JSON after all, as a JSON string.
    fn decode<'py>(
        &self,
        py: Python<'py>,
        text: &str,
        decode_error: &Bound<'py, PyType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(decode) = &self.read else {
            return Err(PyValueError::new_err("A decoded field needs a decoder!"));
        };
        let decode = decode.bind(py);
        if self.json_like && looks_like_json(text) {
            match decode.call1((PyBytes::new(py, text.as_bytes()),)) {
                Err(error)
                    if error.is_instance(py, decode_error)
                        && jiter::JsonValue::parse(text.as_bytes(), false).is_err() => {}
                decoded => return decoded,
            }
        }
        decode.call1((PyBytes::new(py, &json_string(text)),))
    }

    /// Explains that the field could not be read from its text, and why when there is a cause.
    fn unreadable(
        &self,
        py: Python<'_>,
        text: &str,
        line_number: Option<u64>,
        file_name: Option<&str>,
        cause: Option<PyErr>,
    ) -> PyErr {
        let reason = cause
            .as_ref()
            .and_then(|cause| cause.value(py).str().ok())
            .map(|reason| format!(" {reason}"))
            .unwrap_or_default();
        let error = PyValueError::new_err(format!(
            "Could not read {} from text '{text}'{}{}!{reason}",
            self.label,
            self.place,
            on_line(line_number, file_name),
        ));
        error.set_cause(py, cause);
        error
    }
}

/// Reads a JSON integer, e.g. `-12`, of any size.
fn int<'py>(py: Python<'py>, text: &str) -> Option<Bound<'py, PyAny>> {
    if let Some(value) = plain_int(text) {
        return value.into_pyobject(py).ok().map(Bound::into_any);
    }
    let digits = text.strip_prefix('-').unwrap_or(text).as_bytes();
    let integer =
        digits.first().is_some_and(|first| *first != b'0') && digits.iter().all(u8::is_ascii_digit);
    if integer {
        py.get_type::<PyInt>().call1((text,)).ok()
    } else {
        None
    }
}

/// Reads a JSON number, or `nan`, `inf`, or `-inf`, as Python writes them.
fn float(text: &str) -> Option<f64> {
    match text {
        "nan" => Some(f64::NAN),
        "inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        _ => plain_float(text),
    }
}

/// Where the value of a field of a record comes from.
enum Origin {
    /// A field held in one column, read by the field at an index.
    Column(usize),
    /// The columns no other field takes, as a tuple of text.
    Extra,
    /// A field read from the whole row by Python, e.g. one counted across many columns.
    Row(Py<PyString>),
}

/// How the value of a field is handed to the record.
enum Pass {
    /// As the next positional argument of the record type.
    Positional,
    /// As a keyword argument of the record type.
    Keyword(Py<PyString>),
    /// Set on the record once it is built, for a field the record type does not take.
    After(Py<PyString>),
}

/// Builds records of one type from rows of text.
///
/// A record whose type uses the `__init__` its dataclass generated is built as that `__init__`
/// builds it, only faster: its fields are set directly, and then its `__post_init__` is called.
/// A record of any other type is built by calling its type with its fields.
#[pyclass(frozen, module = "typeline._native")]
pub struct RecordBuilder {
    record_type: Py<PyType>,
    record_name: String,
    alloc: Option<ffi::allocfunc>,
    post_init: Option<Py<PyAny>>,
    fields: Vec<Field>,
    slots: Vec<(Origin, Pass)>,
    width: usize,
    extra: Option<(Vec<usize>, usize)>,
    row_fields: Option<Py<PyAny>>,
    file_name: Option<String>,
    decode_error: Py<PyType>,
}

/// The description of a field held in one column that Python hands the builder.
type FieldSpec<'py> = (
    usize,
    String,
    Option<String>,
    bool,
    Option<Bound<'py, PyDict>>,
    Option<Bound<'py, PyAny>>,
    Option<Bound<'py, PyAny>>,
    Option<Bound<'py, PyAny>>,
    String,
    String,
);

#[pymethods]
impl RecordBuilder {
    #[new]
    #[pyo3(signature = (
        record_type, direct, fields, slots, width, extra, row_fields, file_name, decode_error
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        record_type: Bound<'_, PyType>,
        direct: bool,
        fields: Vec<FieldSpec<'_>>,
        slots: Vec<(Bound<'_, PyString>, i64, String)>,
        width: usize,
        extra: Option<(Vec<usize>, usize)>,
        row_fields: Option<Bound<'_, PyAny>>,
        file_name: Option<String>,
        decode_error: Bound<'_, PyType>,
    ) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(
                |(column, lane, none, json_like, table, read, trusted, coerce, label, place)| {
                    let table = match table {
                        Some(table) => table
                            .iter()
                            .map(|(key, value)| Ok((key.extract::<String>()?, value.unbind())))
                            .collect::<PyResult<_>>()?,
                        None => HashMap::new(),
                    };
                    Ok(Field {
                        column,
                        lane: Lane::named(&lane)?,
                        none,
                        json_like,
                        table,
                        read: read.map(Bound::unbind),
                        trusted: trusted.map(Bound::unbind),
                        coerce: coerce.map(Bound::unbind),
                        label,
                        place,
                    })
                },
            )
            .collect::<PyResult<Vec<_>>>()?;
        let slots = slots
            .into_iter()
            .map(|(name, source, pass)| {
                let source = match source {
                    -1 => Origin::Extra,
                    -2 => Origin::Row(name.clone().unbind()),
                    index => Origin::Column(usize::try_from(index).map_err(|_| {
                        PyValueError::new_err(format!("Unknown source of field '{name}'!"))
                    })?),
                };
                let pass = match pass.as_str() {
                    "positional" => Pass::Positional,
                    "keyword" => Pass::Keyword(name.unbind()),
                    "after" => Pass::After(name.unbind()),
                    _ => return Err(PyValueError::new_err(format!("Unknown pass '{pass}'!"))),
                };
                Ok((source, pass))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let alloc = if direct {
            // SAFETY: every type has a `tp_alloc` slot, which takes the type and a count of items.
            unsafe {
                let slot = ffi::PyType_GetSlot(record_type.as_type_ptr(), ffi::Py_tp_alloc);
                if slot.is_null() {
                    return Err(PyTypeError::new_err(
                        "Records of this type cannot be allocated!",
                    ));
                }
                Some(std::mem::transmute::<*mut std::ffi::c_void, ffi::allocfunc>(slot))
            }
        } else {
            None
        };
        let post_init = if direct {
            record_type.getattr("__post_init__").ok().map(Bound::unbind)
        } else {
            None
        };
        Ok(Self {
            record_name: record_type.name()?.to_string(),
            record_type: record_type.unbind(),
            alloc,
            post_init,
            fields,
            slots,
            width,
            extra,
            row_fields: row_fields.map(Bound::unbind),
            file_name,
            decode_error: decode_error.unbind(),
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
        let file_name = self.file_name.as_deref();
        if found != self.width && (self.extra.is_none() || found < self.width) {
            let at_least = if self.extra.is_none() {
                ""
            } else {
                "at least "
            };
            return Err(PyValueError::new_err(format!(
                "Expected {at_least}{} columns but found {found}{} for record type: {}.",
                self.width,
                on_line(line_number, file_name),
                self.record_name,
            )));
        }
        let decode_error = self.decode_error.bind(py);
        let row_values = match &self.row_fields {
            Some(row_fields) => {
                let row = PyList::new(py, cells.iter().map(|cell| cell.object(py)))?;
                Some(row_fields.bind(py).call1((row, line_number))?)
            }
            None => None,
        };
        let allocated = match self.alloc {
            // SAFETY: `alloc` is the record type's own `tp_alloc`, called with that type.
            Some(alloc) => Some(unsafe {
                Bound::from_owned_ptr_or_err(py, alloc(self.record_type.as_ptr().cast(), 0))?
            }),
            None => None,
        };
        let mut positional: Vec<Bound<'py, PyAny>> = Vec::new();
        let mut keywords: Option<Bound<'py, PyDict>> = None;
        let mut after: Vec<(&Py<PyString>, Bound<'py, PyAny>)> = Vec::new();
        for (origin, pass) in &self.slots {
            let value = match origin {
                Origin::Column(index) => {
                    let field = &self.fields[*index];
                    let cell = &cells[field.column];
                    field.read(py, cell, line_number, file_name, decode_error)?
                }
                Origin::Extra => {
                    let (columns, rest) = self
                        .extra
                        .as_ref()
                        .map_or((&[][..], found), |extra| (extra.0.as_slice(), extra.1));
                    let texts: Vec<Bound<'py, PyAny>> = columns
                        .iter()
                        .copied()
                        .chain(rest..found)
                        .map(|column| cells[column].object(py))
                        .collect();
                    PyTuple::new(py, texts)?.into_any()
                }
                Origin::Row(name) => match &row_values {
                    Some(values) => values.get_item(name.bind(py))?,
                    None => return Err(PyValueError::new_err("No fields are read from rows!")),
                },
            };
            match (pass, &allocated) {
                (Pass::After(name), Some(record)) => set_field(record, name.bind(py), &value)?,
                (Pass::After(name), None) => after.push((name, value)),
                (Pass::Positional, _) => positional.push(value),
                (Pass::Keyword(name), _) => keywords
                    .get_or_insert_with(|| PyDict::new(py))
                    .set_item(name.bind(py), value)?,
            }
        }
        let record = match allocated {
            Some(record) => record,
            None => self
                .record_type
                .bind(py)
                .call(PyTuple::new(py, positional)?, keywords.as_ref())
                .map_err(|error| self.unbuildable(py, error, line_number))?,
        };
        for (name, value) in after {
            set_field(&record, name.bind(py), &value)?;
        }
        if let Some(post_init) = &self.post_init {
            post_init
                .bind(py)
                .call1((&record,))
                .map_err(|error| self.unbuildable(py, error, line_number))?;
        }
        Ok(record)
    }

    /// Notes the record and line an error building a record was raised for.
    fn unbuildable(&self, py: Python<'_>, error: PyErr, line_number: Option<u64>) -> PyErr {
        let note = format!(
            "Raised while building {}{}.",
            self.record_name,
            on_line(line_number, self.file_name.as_deref())
        );
        let _ = error.value(py).call_method1("add_note", (note,));
        error
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
        let delimiter = u8::try_from(delimiter)
            .ok()
            .filter(u8::is_ascii)
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "The delimiter must be one ASCII character, not {delimiter:?}!"
                ))
            })?;
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

    /// Build records with a builder.
    fn bind(&mut self, builder: Py<RecordBuilder>) {
        self.builder = Some(builder);
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
                    return builder.build_cells(py, &cells, Some(line)).map(Some);
                }
            }
        }
    }
}
