//! Writing records natively as lines of delimited text.

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType};

use crate::rows::CHUNK;
use crate::text::json_float;

/// How a field of a record is written into text.
enum Out {
    /// Write the value natively, or as the text of its JSON.
    Value,
    /// Write the value with the field's codec.
    Codec(Py<PyAny>),
    /// Write the count of each member of an enum in its own column.
    Counts(Counted),
}

/// The members of an enum a field counts, one column per member in enum order.
struct Counted {
    index_of: Py<PyDict>,
    columns: Vec<String>,
    enum_name: String,
}

impl Counted {
    /// Returns the count of each member in enum order, 0 when absent.
    ///
    /// A count is keyed by a member or its value, and a key that is neither, a member counted
    /// twice, and a count that is not a non-negative integer are refused.
    fn counts<'py>(
        &self,
        py: Python<'py>,
        name: &Bound<'py, PyString>,
        counts: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let index_of = self.index_of.bind(py);
        let mut found: Vec<Option<Bound<'py, PyAny>>> = vec![None; self.columns.len()];
        for item in counts.call_method0("items")?.try_iter()? {
            let (key, count): (Bound<'py, PyAny>, Bound<'py, PyAny>) = item?.extract()?;
            let Some(index) = index_of.get_item(&key)? else {
                return Err(PyValueError::new_err(format!(
                    "Could not write field '{name}', which counts '{}' that is not a member of {}!",
                    key.str()?,
                    self.enum_name
                )));
            };
            let index: usize = index.extract()?;
            if found[index].is_some() {
                return Err(PyValueError::new_err(format!(
                    "Could not write field '{name}', which counts '{}' twice!",
                    self.columns[index]
                )));
            }
            found[index] = Some(count);
        }
        let zero = 0_i64.into_pyobject(py)?.into_any();
        found
            .into_iter()
            .enumerate()
            .map(|(index, count)| {
                let count = count.unwrap_or_else(|| zero.clone());
                if !count.is_exact_instance_of::<PyInt>() || count.lt(0)? {
                    return Err(PyValueError::new_err(format!(
                        "Could not write field '{name}', which counts {} of '{}'; counts must \
                         be non-negative integers!",
                        count.repr()?,
                        self.columns[index]
                    )));
                }
                Ok(count)
            })
            .collect()
    }
}

/// A field of a record and how it is written.
struct OutField {
    name: Py<PyString>,
    out: Out,
    none: Py<PyString>,
    unwritable: String,
}

impl OutField {
    /// Explains that the value of this field could not be written, and why.
    fn unwritable(&self, py: Python<'_>, cause: PyErr) -> PyErr {
        if !cause.is_instance_of::<PyException>(py) {
            return cause;
        }
        let error = PyValueError::new_err(self.unwritable.clone());
        error.set_cause(py, Some(cause));
        error
    }
}

/// The text of a field, written natively or by Python.
enum Piece<'py> {
    Owned(String),
    Static(&'static str),
    Object(Bound<'py, PyString>),
}

impl Piece<'_> {
    /// The text, unless it cannot be read as UTF-8.
    fn text(&self) -> PyResult<&str> {
        match self {
            Piece::Owned(text) => Ok(text),
            Piece::Static(text) => Ok(text),
            Piece::Object(object) => object.to_str(),
        }
    }
}

/// Writes records of one type as lines of text, one field per column.
///
/// Texts are quoted as Python's `csv` module quotes them, or refused when quoting is off.
#[pyclass(frozen, module = "typeline._native")]
pub struct RecordFormatter {
    record_type: Py<PyType>,
    record_name: String,
    fields: Vec<OutField>,
    extra: Option<Py<PyString>>,
    encode: Py<PyAny>,
    delimiter: char,
    line_end: String,
    comment_prefixes: Vec<String>,
    quoting: bool,
    labels: Vec<String>,
}

#[pymethods]
impl RecordFormatter {
    #[new]
    #[pyo3(signature = (
        record_type, fields, extra, encode, delimiter, line_end, comment_prefixes, quoting, labels
    ))]
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn new(
        record_type: Bound<'_, PyType>,
        fields: Vec<(
            Bound<'_, PyString>,
            String,
            Bound<'_, PyString>,
            Option<Bound<'_, PyAny>>,
            String,
        )>,
        extra: Option<Bound<'_, PyString>>,
        encode: Bound<'_, PyAny>,
        delimiter: char,
        line_end: String,
        comment_prefixes: Vec<String>,
        quoting: bool,
        labels: Vec<String>,
    ) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(|(name, out, none, details, unwritable)| {
                let needed = || {
                    details.as_ref().ok_or_else(|| {
                        PyValueError::new_err(format!("The output '{out}' needs its details!"))
                    })
                };
                let out = match out.as_str() {
                    "value" => Out::Value,
                    "codec" => Out::Codec(needed()?.clone().unbind()),
                    "counts" => {
                        let (index_of, columns, enum_name): (Bound<'_, PyDict>, Vec<String>, _) =
                            needed()?.extract()?;
                        Out::Counts(Counted {
                            index_of: index_of.unbind(),
                            columns,
                            enum_name,
                        })
                    }
                    _ => return Err(PyValueError::new_err(format!("Unknown output '{out}'!"))),
                };
                Ok(OutField {
                    name: name.unbind(),
                    out,
                    none: none.unbind(),
                    unwritable,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            record_name: record_type.name()?.to_string(),
            record_type: record_type.unbind(),
            fields,
            extra: extra.map(Bound::unbind),
            encode: encode.unbind(),
            delimiter,
            line_end,
            comment_prefixes,
            quoting,
            labels,
        })
    }

    /// Format a record as its line of text, refusing a record of another type.
    fn format(&self, py: Python<'_>, record: &Bound<'_, PyAny>) -> PyResult<String> {
        let mut line = String::new();
        self.append(py, record, &mut line)?;
        Ok(line)
    }

    /// Format texts as one line, e.g. a header.
    fn line(&self, py: Python<'_>, texts: Vec<String>) -> PyResult<String> {
        let pieces: Vec<Piece<'_>> = texts.into_iter().map(Piece::Owned).collect();
        let mut line = String::new();
        self.join(py, &pieces, &mut line)?;
        Ok(line)
    }

    /// Write records to a stream, stopping at the first that cannot be written once the lines
    /// before it are written.
    ///
    /// Lines of a list or tuple of records are written in large chunks. Lines of any other
    /// iterable are written one at a time, since reading it may write to the stream too, e.g. a
    /// reader handing comments to `write_comment`.
    fn write_all(
        &self,
        py: Python<'_>,
        records: &Bound<'_, PyAny>,
        write: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let limit = if records.is_exact_instance_of::<PyList>()
            || records.is_exact_instance_of::<PyTuple>()
        {
            CHUNK
        } else {
            1
        };
        let mut chunk = String::new();
        let mut written = Ok(());
        for record in records.try_iter()? {
            let start = chunk.len();
            if let Err(error) = record.and_then(|record| self.append(py, &record, &mut chunk)) {
                chunk.truncate(start);
                written = Err(error);
                break;
            }
            if chunk.len() >= limit {
                write.call1((chunk.as_str(),))?;
                chunk.clear();
            }
        }
        let flushed = if chunk.is_empty() {
            Ok(())
        } else {
            write.call1((chunk.as_str(),)).map(drop)
        };
        written.and(flushed)
    }
}

impl RecordFormatter {
    /// Appends the line of a record, refusing a record of another type.
    fn append(&self, py: Python<'_>, record: &Bound<'_, PyAny>, line: &mut String) -> PyResult<()> {
        if !record.is_instance(self.record_type.bind(py))? {
            return Err(PyValueError::new_err(format!(
                "Expected {} but found {}!",
                self.record_name,
                record.get_type().name()?
            )));
        }
        let pieces = self.pieces(py, record)?;
        self.join(py, &pieces, line)
    }

    /// Writes each field of a record into its text.
    fn pieces<'py>(
        &self,
        py: Python<'py>,
        record: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Piece<'py>>> {
        let mut pieces: Vec<Piece<'py>> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let name = field.name.bind(py);
            let value = record.getattr(name)?;
            match &field.out {
                Out::Counts(counted) => {
                    for count in counted.counts(py, name, &value)? {
                        pieces.push(self.native(py, &count)?);
                    }
                }
                _ if value.is_none() => pieces.push(Piece::Object(field.none.bind(py).clone())),
                Out::Codec(write) => {
                    let text = write
                        .bind(py)
                        .call1((value,))
                        .map_err(|error| field.unwritable(py, error))?;
                    pieces.push(Piece::Object(as_text(text)?));
                }
                Out::Value => pieces.push(
                    self.native(py, &value)
                        .map_err(|error| field.unwritable(py, error))?,
                ),
            }
        }
        if let Some(name) = &self.extra {
            let extra = record.getattr(name.bind(py))?;
            for text in extra.try_iter()? {
                let text = text?;
                if !text.is_exact_instance_of::<PyString>() {
                    return Err(PyValueError::new_err(format!(
                        "The ExtraColumns field '{}' of {} must hold text, but holds {}!",
                        name.bind(py),
                        self.record_name,
                        extra.repr()?
                    )));
                }
                pieces.push(Piece::Object(text.cast_into::<PyString>()?));
            }
        }
        Ok(pieces)
    }

    /// Writes a value natively, as typeline does: exact strings as they are, integers, floats,
    /// and booleans as JSON, and any other value as its JSON text, or the string it encodes to.
    fn native<'py>(&self, py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<Piece<'py>> {
        if value.is_exact_instance_of::<PyString>() {
            return Ok(Piece::Object(value.clone().cast_into::<PyString>()?));
        }
        if value.is_exact_instance_of::<PyBool>() {
            return Ok(Piece::Static(if value.is_truthy()? {
                "true"
            } else {
                "false"
            }));
        }
        if value.is_exact_instance_of::<PyInt>() {
            return Ok(match value.extract::<i64>() {
                Ok(number) => Piece::Owned(itoa::Buffer::new().format(number).to_string()),
                Err(_) => Piece::Object(value.str()?),
            });
        }
        if value.is_exact_instance_of::<PyFloat>() {
            let number = value.extract::<f64>()?;
            return Ok(if number.is_finite() {
                Piece::Owned(json_float(number))
            } else {
                Piece::Object(value.repr()?)
            });
        }
        let json = self.encode.bind(py).call1((value,))?;
        json_text(json.cast::<PyBytes>()?.as_bytes())
            .map(Piece::Owned)
            .ok_or_else(|| PyValueError::new_err("The JSON of the value is not valid UTF-8!"))
    }

    /// Appends texts as one line, quoted as the csv module quotes them: a text holding the
    /// delimiter, a quote, or a line break, a record of one empty text, and every text of a row
    /// whose first text starts with a comment prefix, which would otherwise read back as one.
    ///
    /// Without quoting, a row that would need any of this is refused instead.
    fn join(&self, py: Python<'_>, pieces: &[Piece<'_>], line: &mut String) -> PyResult<()> {
        let first = pieces.first().map(Piece::text).transpose()?;
        let like_comment = first.is_some_and(|text| {
            self.comment_prefixes
                .iter()
                .any(|prefix| text.starts_with(prefix.as_str()))
        });
        let lone_empty = pieces.len() == 1 && first == Some("");
        for (index, piece) in pieces.iter().enumerate() {
            let text = piece.text()?;
            let breaks = text.contains([self.delimiter, '\n', '\r']);
            if breaks && !self.quoting {
                let reason = "its text holds the delimiter or a line break";
                return Err(self.unquotable(py, index, reason, Some(text)));
            }
            if index > 0 {
                line.push(self.delimiter);
            }
            if self.quoting && (breaks || like_comment || lone_empty || text.contains('"')) {
                line.push('"');
                for character in text.chars() {
                    if character == '"' {
                        line.push('"');
                    }
                    line.push(character);
                }
                line.push('"');
            } else {
                line.push_str(text);
            }
        }
        if lone_empty && !self.quoting {
            let reason = "a record of one empty field must be quoted";
            return Err(self.unquotable(py, 0, reason, None));
        }
        if like_comment && !self.quoting {
            let reason = "its text starts with a comment prefix";
            return Err(self.unquotable(py, 0, reason, first));
        }
        line.push_str(&self.line_end);
        Ok(())
    }

    /// Explains that the text at an index of a row cannot be written without quoting.
    fn unquotable(&self, py: Python<'_>, index: usize, reason: &str, text: Option<&str>) -> PyErr {
        let label = match (self.labels.get(index), &self.extra) {
            (Some(label), _) => label.clone(),
            (None, Some(name)) => format!("field '{}'", name.bind(py)),
            (None, None) => String::new(),
        };
        let shown = text
            .and_then(|text| PyString::new(py, text).repr().ok())
            .map(|text| format!(": {text}"))
            .unwrap_or_default();
        PyValueError::new_err(format!(
            "Cannot write {label} of {} without quoting, because {reason}{shown}!",
            self.record_name
        ))
    }
}

/// Returns a value as text: a string as it is, and anything else as `str` writes it.
fn as_text(value: Bound<'_, PyAny>) -> PyResult<Bound<'_, PyString>> {
    if value.is_exact_instance_of::<PyString>() {
        value.cast_into::<PyString>().map_err(Into::into)
    } else {
        value.str()
    }
}

/// Returns the text of a value encoded as JSON: the string it holds when it is a JSON string,
/// e.g. a date or an enum's value, and else the JSON itself, e.g. a list.
fn json_text(json: &[u8]) -> Option<String> {
    match json {
        [b'"', inner @ .., b'"'] if !inner.contains(&b'\\') => {
            std::str::from_utf8(inner).ok().map(str::to_string)
        }
        [b'"', ..] => jiter::Jiter::new(json).next_str().ok().map(str::to_string),
        _ => std::str::from_utf8(json).ok().map(str::to_string),
    }
}
