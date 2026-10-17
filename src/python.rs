//! The `typeline._native` extension module, which reads records from the text of their fields and
//! writes them back.

use crate::text::{json_float, json_string, looks_like_json, on_line, plain_float, plain_int};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyList, PyString, PyTuple, PyType};

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

/// A field of a record, the column it is read from, and how.
///
/// A field that is not read in its lane is decoded by its typed decoder, as JSON when its text
/// looks like JSON and else as a JSON string. Whatever fails there is handed to its reader, which
/// reads the text as typeline always has and explains why it cannot.
struct Field {
    name: Py<PyString>,
    column: usize,
    lane: Lane,
    none: Option<Py<PyString>>,
    json_like: bool,
    table: Option<Py<PyDict>>,
    decode: Option<Py<PyAny>>,
    read: Option<Py<PyAny>>,
}

/// The field that holds the columns no other field takes.
struct Extra {
    name: Py<PyString>,
    columns: Vec<usize>,
    rest: usize,
}

impl Field {
    /// Reads the value of this field from the text of its column.
    fn read<'py>(
        &self,
        py: Python<'py>,
        text: Bound<'py, PyAny>,
        line_number: Option<u64>,
        decode_error: &Bound<'py, PyType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if let Some(none) = &self.none
            && text.eq(none.bind(py))?
        {
            return Ok(py.None().into_bound(py));
        }
        let plain = text
            .cast::<PyString>()
            .ok()
            .and_then(|string| string.to_str().ok());
        match (self.lane, plain) {
            (Lane::Text, _) => return Ok(text),
            (Lane::Call, _) | (_, None) => return self.fall_back(text, line_number),
            (Lane::Int, Some(plain)) => {
                if let Some(value) = plain_int(plain) {
                    return Ok(value.into_pyobject(py)?.into_any());
                }
            }
            (Lane::Float, Some(plain)) => {
                if let Some(value) = plain_float(plain) {
                    return Ok(PyFloat::new(py, value).into_any());
                }
            }
            (Lane::Bool, Some("true")) => return Ok(PyBool::new(py, true).to_owned().into_any()),
            (Lane::Bool, Some("false")) => return Ok(PyBool::new(py, false).to_owned().into_any()),
            (Lane::Lookup, Some(_)) => {
                if let Some(table) = &self.table
                    && let Some(value) = table.bind(py).get_item(&text)?
                {
                    return Ok(value);
                }
            }
            (Lane::Bool | Lane::Decode, Some(_)) => {}
        }
        let Some(decode) = &self.decode else {
            return self.fall_back(text, line_number);
        };
        let plain = plain.unwrap_or_default();
        let json = if self.json_like && looks_like_json(plain) {
            text.clone()
        } else {
            PyBytes::new(py, &json_string(plain)).into_any()
        };
        match decode.bind(py).call1((json,)) {
            Err(error) if error.is_instance(py, decode_error) => self.fall_back(text, line_number),
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

/// Builds records of one type from rows of text, one field per column.
///
/// A record is built as msgspec builds a dataclass: its fields are set without calling
/// `__init__`, and then `__post_init__` is called if the type has one.
#[pyclass(frozen, module = "typeline._native")]
struct RecordBuilder {
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
    Option<Bound<'py, PyString>>,
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
                    Ok(Field {
                        name: name.unbind(),
                        column,
                        lane: Lane::named(&lane)?,
                        none: none.map(Bound::unbind),
                        json_like,
                        table: table.map(Bound::unbind),
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
        let found = row.len();
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
            let value = field.read(py, row.get_item(field.column)?, line_number, decode_error)?;
            set_field(&record, field.name.bind(py), &value)?;
        }
        if let Some(extra) = &self.extra {
            let mut texts = Vec::with_capacity(extra.columns.len() + found - extra.rest);
            for &column in &extra.columns {
                texts.push(row.get_item(column)?);
            }
            for column in extra.rest..found {
                texts.push(row.get_item(column)?);
            }
            set_field(
                &record,
                extra.name.bind(py),
                PyTuple::new(py, texts)?.as_any(),
            )?;
        }
        if let Some(row_fields) = &self.row_fields {
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
}

impl RecordBuilder {
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

/// How a field of a record is written into text.
enum Out {
    /// Write the value natively when it is exactly a `str`, `int`, `bool`, or `float`.
    Value,
    /// Always write the value with the field's writer, e.g. through a codec.
    Call,
    /// Write the texts of several columns, which the field's writer returns.
    Many,
}

/// A field of a record and how it is written.
struct OutField {
    name: Py<PyString>,
    out: Out,
    none: Py<PyString>,
    write: Py<PyAny>,
}

/// Writes records of one type as lines of text, one field per column.
///
/// A record whose texts all need no quoting is written as one line, its texts joined by the
/// delimiter. Any other record is returned as its texts, for the csv module to quote.
#[pyclass(frozen, module = "typeline._native")]
struct RecordFormatter {
    fields: Vec<OutField>,
    extra: Option<(Py<PyString>, Py<PyAny>)>,
    delimiter: char,
    line_end: String,
    comment_prefixes: Vec<String>,
}

#[pymethods]
impl RecordFormatter {
    #[new]
    #[pyo3(signature = (fields, extra, delimiter, line_end, comment_prefixes))]
    #[allow(clippy::type_complexity)]
    fn new(
        fields: Vec<(
            Bound<'_, PyString>,
            String,
            Bound<'_, PyString>,
            Bound<'_, PyAny>,
        )>,
        extra: Option<(Bound<'_, PyString>, Bound<'_, PyAny>)>,
        delimiter: char,
        line_end: String,
        comment_prefixes: Vec<String>,
    ) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(|(name, out, none, write)| {
                let out = match out.as_str() {
                    "value" => Out::Value,
                    "call" => Out::Call,
                    "many" => Out::Many,
                    _ => return Err(PyValueError::new_err(format!("Unknown output '{out}'!"))),
                };
                Ok(OutField {
                    name: name.unbind(),
                    out,
                    none: none.unbind(),
                    write: write.unbind(),
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            fields,
            extra: extra.map(|(name, check)| (name.unbind(), check.unbind())),
            delimiter,
            line_end,
            comment_prefixes,
        })
    }

    /// Format a record as its line of text, or as its texts when the line needs quoting.
    fn format<'py>(
        &self,
        py: Python<'py>,
        record: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut texts: Vec<Bound<'py, PyAny>> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let value = record.getattr(field.name.bind(py))?;
            if let Out::Many = field.out {
                for text in field.write.bind(py).call1((value,))?.try_iter()? {
                    texts.push(text?);
                }
                continue;
            }
            let text = if value.is_none() {
                field.none.bind(py).clone().into_any()
            } else if let Out::Value = field.out
                && let Some(text) = native_text(py, &value)?
            {
                text
            } else {
                field.write.bind(py).call1((value,))?
            };
            texts.push(text);
        }
        if let Some((name, check)) = &self.extra {
            let extra = record.getattr(name.bind(py))?;
            for text in check.bind(py).call1((extra,))?.try_iter()? {
                texts.push(text?);
            }
        }
        match self.line(&texts) {
            Some(line) => Ok(PyString::new(py, &line).into_any()),
            None => Ok(PyList::new(py, texts)?.into_any()),
        }
    }
}

impl RecordFormatter {
    /// Joins texts into a line when the csv module would write them as they are, else None.
    fn line(&self, texts: &[Bound<'_, PyAny>]) -> Option<String> {
        let mut line = String::new();
        for (index, text) in texts.iter().enumerate() {
            let text = text
                .cast::<PyString>()
                .ok()
                .filter(|text| text.is_exact_instance_of::<PyString>())?;
            let text = text.to_str().ok()?;
            if text.contains([self.delimiter, '"', '\n', '\r'])
                || (index == 0
                    && self
                        .comment_prefixes
                        .iter()
                        .any(|prefix| text.starts_with(prefix.as_str())))
            {
                return None;
            }
            if index > 0 {
                line.push(self.delimiter);
            }
            line.push_str(text);
        }
        if line.is_empty() {
            return None;
        }
        line.push_str(&self.line_end);
        Some(line)
    }
}

/// Writes a value exactly of type `str`, `int`, `bool`, or `float` into text, as typeline does.
fn native_text<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if value.is_exact_instance_of::<PyString>() {
        return Ok(Some(value.clone()));
    }
    if value.is_exact_instance_of::<PyBool>() {
        let text = if value.is_truthy()? { "true" } else { "false" };
        return Ok(Some(PyString::new(py, text).into_any()));
    }
    if value.is_exact_instance_of::<pyo3::types::PyInt>() {
        return Ok(Some(match value.extract::<i64>() {
            Ok(number) => PyString::new(py, &number.to_string()).into_any(),
            Err(_) => value.str()?.into_any(),
        }));
    }
    if value.is_exact_instance_of::<PyFloat>() {
        let number = value.extract::<f64>()?;
        let text = if number.is_finite() {
            json_float(number)
        } else {
            value.repr()?.to_string()
        };
        return Ok(Some(PyString::new(py, &text).into_any()));
    }
    Ok(None)
}

/// The native engine of typeline.
#[pymodule(gil_used = false)]
mod _native {
    #[pymodule_export]
    use super::RecordBuilder;

    #[pymodule_export]
    use super::RecordFormatter;
}
