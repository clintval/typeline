//! Reading records natively from rows of delimited text.

use std::collections::{HashMap, HashSet};

use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyIterator, PyString,
    PyTime, PyTuple, PyType, PyTzInfo,
};

use std::convert::Infallible;

use crate::dates::{self, Zone};
use crate::rows::{CHUNK, Event, Options, Rows, Source, ends_in_quotes};
use crate::text::{json_string, looks_like_json, on_line, plain_float, plain_int};

/// How a field is read from the text of its column.
enum Lane {
    /// Keep the text as it is.
    Text,
    /// Read a JSON integer, e.g. `-12`.
    Int,
    /// Read a JSON number, or `nan`, `inf`, or `-inf`, as Python writes them.
    Float,
    /// Read `true` or `false`.
    Bool,
    /// Read the first of its plain types the text reads as: a boolean, integer, float, or text.
    Union(Vec<Plain>),
    /// Look the text up in a table, e.g. of an enum's members by the text of their values.
    Lookup(HashMap<String, Py<PyAny>>),
    /// Read an ISO 8601 date, e.g. `2021-03-04`.
    Date,
    /// Read an ISO 8601 datetime, e.g. `2021-03-04T05:06:07Z`.
    DateTime,
    /// Read an ISO 8601 time, e.g. `05:06:07`.
    Time,
    /// Read a UUID of 32 hexadecimal digits, with or without hyphens.
    Uuid {
        alloc: ffi::allocfunc,
        class: Py<PyType>,
        unknown: Py<PyAny>,
    },
    /// Read a decimal with Python's `Decimal`.
    Decimal(Py<PyAny>),
    /// Read the text with a codec, whose value is the field's value.
    Codec(Py<PyAny>),
    /// Decode the text as JSON with the field's typed decoder.
    Decode { decode: Py<PyAny>, json_like: bool },
}

impl Lane {
    /// Builds a lane from its name and the details it needs, e.g. a codec.
    fn new(py: Python<'_>, name: &str, details: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let needed = || {
            details.ok_or_else(|| {
                PyValueError::new_err(format!("The lane '{name}' needs its details!"))
            })
        };
        Ok(match name {
            "text" => Self::Text,
            "int" => Self::Int,
            "float" => Self::Float,
            "bool" => Self::Bool,
            "union" => {
                let names: Vec<String> = needed()?.extract()?;
                let mut members = names
                    .iter()
                    .map(|member| match member.as_str() {
                        "bool" => Ok(Plain::Bool),
                        "int" => Ok(Plain::Int),
                        "float" => Ok(Plain::Float),
                        "text" => Ok(Plain::Text),
                        _ => Err(PyValueError::new_err(format!("Unknown member '{member}'!"))),
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                members.sort_unstable();
                Self::Union(members)
            }
            "lookup" => Self::Lookup(
                needed()?
                    .cast::<PyDict>()?
                    .iter()
                    .map(|(key, value)| Ok((key.extract::<String>()?, value.unbind())))
                    .collect::<PyResult<_>>()?,
            ),
            "date" => Self::Date,
            "datetime" => Self::DateTime,
            "time" => Self::Time,
            "uuid" => {
                let uuid = py.import("uuid")?;
                let class = uuid.getattr("UUID")?.cast_into::<PyType>()?;
                Self::Uuid {
                    alloc: alloc_of(&class)?,
                    class: class.unbind(),
                    unknown: uuid.getattr("SafeUUID")?.getattr("unknown")?.unbind(),
                }
            }
            "decimal" => Self::Decimal(py.import("decimal")?.getattr("Decimal")?.unbind()),
            "codec" => Self::Codec(needed()?.clone().unbind()),
            "decode" => {
                let (decode, json_like): (Bound<'_, PyAny>, bool) = needed()?.extract()?;
                Self::Decode {
                    decode: decode.unbind(),
                    json_like,
                }
            }
            _ => return Err(PyValueError::new_err(format!("Unknown lane '{name}'!"))),
        })
    }
}

/// A plain type of a union, in the order a union tries them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Plain {
    Bool,
    Int,
    Float,
    Text,
}

/// A field of a record held in one column, and how it is read from that column's text.
struct Field {
    name: Py<PyString>,
    column: usize,
    lane: Lane,
    none: Option<String>,
    label: String,
    place: String,
}

impl Field {
    /// Reads the value of this field from the text of its column.
    fn read<'py>(
        &self,
        py: Python<'py>,
        text: &str,
        line_number: Option<u64>,
        file_name: Option<&str>,
        decode_error: &Bound<'py, PyType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.none.as_deref() == Some(text) {
            return Ok(py.None().into_bound(py));
        }
        let read = match &self.lane {
            Lane::Text => Some(Ok(PyString::new(py, text).into_any())),
            Lane::Int => int(py, text).map(Ok),
            Lane::Float => float(text).map(|value| Ok(PyFloat::new(py, value).into_any())),
            Lane::Bool => {
                boolean(text).map(|value| Ok(PyBool::new(py, value).to_owned().into_any()))
            }
            Lane::Union(members) => members.iter().find_map(|member| match member {
                Plain::Bool => {
                    boolean(text).map(|value| Ok(PyBool::new(py, value).to_owned().into_any()))
                }
                Plain::Int => int(py, text).map(Ok),
                Plain::Float => float(text).map(|value| Ok(PyFloat::new(py, value).into_any())),
                Plain::Text => Some(Ok(PyString::new(py, text).into_any())),
            }),
            Lane::Lookup(table) => table.get(text).map(|value| Ok(value.bind(py).clone())),
            Lane::Date => Some(read_date(py, text)),
            Lane::DateTime => Some(read_datetime(py, text)),
            Lane::Time => Some(read_time(py, text)),
            Lane::Uuid {
                alloc,
                class,
                unknown,
            } => crate::text::uuid(text)
                .map(|value| build_uuid(py, *alloc, class.bind(py), unknown.bind(py), value)),
            Lane::Decimal(decimal) => Some(decimal.bind(py).call1((text,))),
            Lane::Codec(read) => Some(read.bind(py).call1((text,))),
            Lane::Decode { decode, json_like } => Some(decode_json(
                py,
                text,
                decode.bind(py),
                *json_like,
                decode_error,
            )),
        };
        match read {
            Some(Ok(value)) => Ok(value),
            Some(Err(cause)) => Err(self.unreadable(py, text, line_number, file_name, Some(cause))),
            None => Err(self.unreadable(py, text, line_number, file_name, None)),
        }
    }

    /// Explains that the field could not be read from its text, and why when there is a cause.
    ///
    /// A cause that is not an `Exception`, e.g. a `KeyboardInterrupt`, is raised as it is.
    fn unreadable(
        &self,
        py: Python<'_>,
        text: &str,
        line_number: Option<u64>,
        file_name: Option<&str>,
        cause: Option<PyErr>,
    ) -> PyErr {
        let cause = match cause {
            Some(cause) if !cause.is_instance_of::<PyException>(py) => return cause,
            cause => cause,
        };
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

/// Decodes text with a typed decoder: as JSON when it looks like JSON, and else, or when it is
/// not valid JSON after all, as a JSON string.
///
/// msgspec raises `DecodeError` itself for malformed JSON, and its subclass `ValidationError`
/// for JSON of the wrong shape, which is raised as it is.
fn decode_json<'py>(
    py: Python<'py>,
    text: &str,
    decode: &Bound<'py, PyAny>,
    json_like: bool,
    decode_error: &Bound<'py, PyType>,
) -> PyResult<Bound<'py, PyAny>> {
    if json_like && looks_like_json(text) {
        match decode.call1((PyBytes::new(py, text.as_bytes()),)) {
            Err(error) if error.get_type(py).is(decode_error) => {}
            decoded => return decoded,
        }
    }
    decode.call1((PyBytes::new(py, &json_string(text)),))
}

/// Reads a date, e.g. `2021-03-04`.
fn read_date<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let day = dates::date(text).map_err(PyValueError::new_err)?;
    Ok(PyDate::new(py, day.year, day.month, day.day)?.into_any())
}

/// Reads a datetime, e.g. `2021-03-04T05:06:07Z`.
fn read_datetime<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let (day, clock, zone) = dates::datetime(text).map_err(PyValueError::new_err)?;
    let zone = tzinfo(py, zone)?;
    Ok(PyDateTime::new(
        py,
        day.year,
        day.month,
        day.day,
        clock.hour,
        clock.minute,
        clock.second,
        clock.microsecond,
        zone.as_ref(),
    )?
    .into_any())
}

/// Reads a time, e.g. `05:06:07`.
fn read_time<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let (clock, zone) = dates::time(text).map_err(PyValueError::new_err)?;
    let zone = tzinfo(py, zone)?;
    let time = PyTime::new(
        py,
        clock.hour,
        clock.minute,
        clock.second,
        clock.microsecond,
        zone.as_ref(),
    )?;
    Ok(time.into_any())
}

/// Returns the Python time zone of a zone: none, UTC, or a fixed offset.
fn tzinfo(py: Python<'_>, zone: Zone) -> PyResult<Option<Bound<'_, PyTzInfo>>> {
    Ok(match zone {
        Zone::Naive => None,
        Zone::Utc => Some(PyTzInfo::utc(py)?.to_owned()),
        Zone::Fixed(seconds) => {
            let offset = PyDelta::new(py, 0, seconds, 0, true)?;
            Some(PyTzInfo::fixed_offset(py, offset)?)
        }
    })
}

/// Builds a UUID as Python's `uuid.UUID` holds one: its integer, and whether it is safe.
fn build_uuid<'py>(
    py: Python<'py>,
    alloc: ffi::allocfunc,
    class: &Bound<'py, PyType>,
    unknown: &Bound<'py, PyAny>,
    value: u128,
) -> PyResult<Bound<'py, PyAny>> {
    // SAFETY: `alloc` is the UUID class's own `tp_alloc`, called with that class.
    let uuid = unsafe { Bound::from_owned_ptr_or_err(py, alloc(class.as_ptr().cast(), 0))? };
    let integer = value.into_pyobject(py)?.into_any();
    set_field(&uuid, &PyString::intern(py, "int"), &integer)?;
    set_field(&uuid, &PyString::intern(py, "is_safe"), unknown)?;
    Ok(uuid)
}

/// Returns the `tp_alloc` of a type, which allocates an instance without calling `__init__`.
fn alloc_of(class: &Bound<'_, PyType>) -> PyResult<ffi::allocfunc> {
    // SAFETY: every type has a `tp_alloc` slot, which takes the type and a count of items.
    unsafe {
        let slot = ffi::PyType_GetSlot(class.as_type_ptr(), ffi::Py_tp_alloc);
        if slot.is_null() {
            return Err(PyTypeError::new_err(
                "Instances of this type cannot be allocated!",
            ));
        }
        Ok(std::mem::transmute::<*mut std::ffi::c_void, ffi::allocfunc>(slot))
    }
}

/// Reads `true` or `false`.
fn boolean(text: &str) -> Option<bool> {
    match text {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
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

/// Reads a count of ASCII digits, e.g. `007`, of any size.
fn count<'py>(py: Python<'py>, text: &str) -> Option<PyResult<Bound<'py, PyAny>>> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(match text.parse::<u64>() {
        Ok(value) => value
            .into_pyobject(py)
            .map(Bound::into_any)
            .map_err(PyErr::from),
        Err(_) => py.get_type::<PyInt>().call1((text,)),
    })
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

/// Reads a float as a float field reads one, or None when the text is not a float.
#[pyfunction]
pub fn read_float(text: &str) -> Option<f64> {
    float(text)
}

/// Where the fields of a record go as they are read.
enum Target<'py> {
    /// Onto a record allocated without its `__init__`, as its generated `__init__` would set them.
    Record(Bound<'py, PyAny>),
    /// Into the keyword arguments of the record type, or aside for a field it does not take.
    Arguments(
        Bound<'py, PyDict>,
        Vec<(Bound<'py, PyString>, Bound<'py, PyAny>)>,
    ),
}

/// Builds records of one type from rows of text.
///
/// A record whose type uses the `__init__` its dataclass generated, and whose `__new__` is
/// `object.__new__`, is built as calling the type would build it, only faster: it is allocated,
/// its fields are set directly, and then its `__post_init__` is called. A record of any other
/// type is built by calling its type with its fields.
#[pyclass(frozen, module = "typeline._native")]
pub struct RecordBuilder {
    record_type: Py<PyType>,
    record_name: String,
    direct: Option<(ffi::allocfunc, Option<Py<PyAny>>)>,
    init: HashSet<String>,
    fields: Vec<Field>,
    width: usize,
    extra: Option<(Py<PyString>, Vec<usize>)>,
    counters: Vec<Counts>,
    counter_type: Py<PyAny>,
    file_name: Option<String>,
    decode_error: Py<PyType>,
}

/// A field that counts the members of an enum, each in its own column.
struct Counts {
    name: Py<PyString>,
    members: Vec<(Py<PyAny>, String, usize)>,
}

/// The description of a field held in one column that Python hands the builder: its name, its
/// column, its lane and the details of the lane, its text for None, and how it and its column are
/// named in errors.
type FieldSpec<'py> = (
    Bound<'py, PyString>,
    usize,
    String,
    Option<String>,
    Option<Bound<'py, PyAny>>,
    String,
    String,
);

/// The description of a field that counts the members of an enum: its name, and each member with
/// the name and index of its column.
type CountsSpec<'py> = (
    Bound<'py, PyString>,
    Vec<(Bound<'py, PyAny>, String, usize)>,
);

#[pymethods]
impl RecordBuilder {
    #[new]
    #[pyo3(signature = (record_type, fields, init, width, extra, counters, file_name))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        record_type: Bound<'_, PyType>,
        fields: Vec<FieldSpec<'_>>,
        init: Option<Vec<String>>,
        width: usize,
        extra: Option<(Bound<'_, PyString>, Vec<usize>)>,
        counters: Vec<CountsSpec<'_>>,
        file_name: Option<String>,
    ) -> PyResult<Self> {
        let py = record_type.py();
        let fields = fields
            .into_iter()
            .map(|(name, column, lane, none, details, label, place)| {
                Ok(Field {
                    name: name.unbind(),
                    column,
                    lane: Lane::new(py, &lane, details.as_ref())?,
                    none,
                    label,
                    place,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let direct = match &init {
            Some(_) => None,
            None => Some((
                alloc_of(&record_type)?,
                record_type.getattr("__post_init__").ok().map(Bound::unbind),
            )),
        };
        Ok(Self {
            record_name: record_type.name()?.to_string(),
            record_type: record_type.unbind(),
            direct,
            init: init.unwrap_or_default().into_iter().collect(),
            fields,
            width,
            extra: extra.map(|(name, columns)| (name.unbind(), columns)),
            counters: counters
                .into_iter()
                .map(|(name, members)| Counts {
                    name: name.unbind(),
                    members: members
                        .into_iter()
                        .map(|(member, column, index)| (member.unbind(), column, index))
                        .collect(),
                })
                .collect(),
            counter_type: py.import("collections")?.getattr("Counter")?.unbind(),
            file_name,
            decode_error: py
                .import("msgspec")?
                .getattr("DecodeError")?
                .cast_into::<PyType>()?
                .unbind(),
        })
    }
}

impl RecordBuilder {
    /// Builds a record from the cells of a row, naming the line it starts on in errors.
    fn build<'py>(
        &self,
        py: Python<'py>,
        cells: &[&str],
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
        let mut target = match &self.direct {
            // SAFETY: `alloc` is the record type's own `tp_alloc`, called with that type, and
            // Python only hands over a type whose `__new__` is `object.__new__`, which does no
            // more than allocate, so no base is left without the state its `__new__` sets up.
            Some((alloc, _)) => Target::Record(unsafe {
                Bound::from_owned_ptr_or_err(py, alloc(self.record_type.as_ptr().cast(), 0))?
            }),
            None => Target::Arguments(PyDict::new(py), Vec::new()),
        };
        for counts in &self.counters {
            let counter = self.counter_type.bind(py).call0()?;
            for (member, column, index) in &counts.members {
                let text = cells[*index];
                let Some(count) = count(py, text) else {
                    return Err(PyValueError::new_err(format!(
                        "Could not read column '{column}' of field '{}' as a count from \
                         '{text}'{}!",
                        counts.name.bind(py),
                        on_line(line_number, file_name),
                    )));
                };
                counter.set_item(member.bind(py), count?)?;
            }
            self.place(&mut target, counts.name.bind(py), counter)?;
        }
        let decode_error = self.decode_error.bind(py);
        for field in &self.fields {
            let value = field.read(
                py,
                cells[field.column],
                line_number,
                file_name,
                decode_error,
            )?;
            self.place(&mut target, field.name.bind(py), value)?;
        }
        if let Some((name, columns)) = &self.extra {
            let texts: Vec<Bound<'py, PyString>> = columns
                .iter()
                .copied()
                .chain(self.width..found)
                .map(|column| PyString::new(py, cells[column]))
                .collect();
            self.place(
                &mut target,
                name.bind(py),
                PyTuple::new(py, texts)?.into_any(),
            )?;
        }
        match target {
            Target::Record(record) => {
                if let Some((_, Some(post_init))) = &self.direct {
                    post_init
                        .bind(py)
                        .call1((&record,))
                        .map_err(|error| self.unbuildable(py, error, line_number))?;
                }
                Ok(record)
            }
            Target::Arguments(kwargs, after) => {
                let record = self
                    .record_type
                    .bind(py)
                    .call((), Some(&kwargs))
                    .map_err(|error| self.unbuildable(py, error, line_number))?;
                for (name, value) in after {
                    set_field(&record, &name, &value)?;
                }
                Ok(record)
            }
        }
    }

    /// Hands the value of a field to where the record's fields go.
    fn place<'py>(
        &self,
        target: &mut Target<'py>,
        name: &Bound<'py, PyString>,
        value: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        match target {
            Target::Record(record) => set_field(record, name, &value),
            Target::Arguments(kwargs, _) if self.init.contains(name.to_str()?) => {
                kwargs.set_item(name, value)
            }
            Target::Arguments(_, after) => {
                after.push((name.clone(), value));
                Ok(())
            }
        }
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

/// A line of text to decode, read all at once.
#[derive(Default)]
struct Line(String);

impl Source for Line {
    type Error = Infallible;

    fn read(&mut self, text: &mut String) -> Result<bool, Infallible> {
        if self.0.is_empty() {
            return Ok(false);
        }
        text.push_str(&self.0);
        self.0.clear();
        Ok(true)
    }
}

/// Reads records natively from a Python text stream, one row at a time.
///
/// Comments between rows are handed to `on_comment` as they are reached. Once the stream is read
/// to its end, or reading it fails, it is closed if `close_when_read` is set.
#[pyclass(module = "typeline._native")]
pub struct RowReader {
    rows: Rows<Stream>,
    decoder: Rows<Line>,
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
            decoder: Rows::new(Line::default(), options.clone()),
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

    /// Decode one record from a line of text, as this reader reads records, reading nothing from
    /// its stream.
    ///
    /// The line may end with a line break, and a quoted field may hold line breaks. Blank text,
    /// a comment, text holding more than one record, and text ending inside a quoted field are
    /// refused.
    fn decode<'py>(&mut self, py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
        let Some(builder) = &self.builder else {
            return Err(PyValueError::new_err("No record type to build records of!"));
        };
        let builder = builder.get();
        let refuse = |before: &str, after: &str| {
            PyString::new(py, text).repr().map(|shown| {
                PyValueError::new_err(format!(
                    "Could not decode {} from {before}{shown}{after}!",
                    builder.record_name
                ))
            })
        };
        let decoder = &mut self.decoder;
        decoder.source_mut().0.clear();
        decoder.source_mut().0.push_str(text);
        decoder.restart();
        let Ok(first) = decoder.read();
        let row: Vec<String> = match first {
            None => return Err(refuse("blank line ", "")?),
            Some(Event::Comment { .. }) => return Err(refuse("comment line ", "")?),
            Some(Event::Row { row, .. }) => row.iter().map(str::to_string).collect(),
        };
        let Ok(next) = decoder.read();
        if next.is_some() {
            return Err(refuse("", ", which holds more than one record")?);
        }
        let options = decoder.options();
        if options.quoting && ends_in_quotes(text, options.delimiter) {
            return Err(refuse("", ", which ends inside a quoted field")?);
        }
        let cells: Vec<&str> = row.iter().map(String::as_str).collect();
        builder.build(py, &cells, None)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        if self.done {
            return Ok(None);
        }
        let next = self.next_record(py);
        if matches!(next, Ok(Some(_))) {
            return next;
        }
        self.done = true;
        if !self.close_when_read {
            return next;
        }
        let closed = self.rows.source().handle.bind(py).call_method0("close");
        next.and_then(|record| closed.map(|_| record))
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
                    let cells: Vec<&str> = row.iter().collect();
                    return builder.build(py, &cells, Some(line)).map(Some);
                }
            }
        }
    }
}
