//! Writing records natively as lines of delimited text.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple, PyType};

use crate::rows::CHUNK;
use crate::text::json_float;

/// How a field of a record is written into text.
enum Out {
    /// Write the value natively, or as JSON, and with the field's writer when neither can.
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

/// The text of a field, written natively or by Python.
enum Piece<'py> {
    Owned(String),
    Static(&'static str),
    Object(Bound<'py, PyAny>),
}

impl<'py> Piece<'py> {
    /// The text, unless it is not exactly a string that can be read as UTF-8.
    fn text(&self) -> Option<&str> {
        match self {
            Piece::Owned(text) => Some(text),
            Piece::Static(text) => Some(text),
            Piece::Object(object) => object
                .cast::<PyString>()
                .ok()
                .filter(|text| text.is_exact_instance_of::<PyString>())?
                .to_str()
                .ok(),
        }
    }

    /// The text as a Python object.
    fn into_object(self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self {
            Piece::Owned(text) => PyString::new(py, &text).into_any(),
            Piece::Static(text) => PyString::new(py, text).into_any(),
            Piece::Object(object) => object,
        }
    }
}

/// Writes records of one type as lines of text, one field per column.
///
/// A record whose texts all need no quoting is written as one line, its texts joined by the
/// delimiter. Any other record is returned as its texts, for the csv module to quote.
#[pyclass(frozen, module = "typeline._native")]
pub struct RecordFormatter {
    fields: Vec<OutField>,
    extra: Option<(Py<PyString>, Py<PyAny>)>,
    encode: Py<PyAny>,
    delimiter: char,
    line_end: String,
    comment_prefixes: Vec<String>,
}

#[pymethods]
impl RecordFormatter {
    #[new]
    #[pyo3(signature = (fields, extra, encode, delimiter, line_end, comment_prefixes))]
    #[allow(clippy::type_complexity)]
    fn new(
        fields: Vec<(
            Bound<'_, PyString>,
            String,
            Bound<'_, PyString>,
            Bound<'_, PyAny>,
        )>,
        extra: Option<(Bound<'_, PyString>, Bound<'_, PyAny>)>,
        encode: Bound<'_, PyAny>,
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
            encode: encode.unbind(),
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
        let pieces = self.pieces(py, record)?;
        match self.line(&pieces) {
            Some(line) => Ok(PyString::new(py, &line).into_any()),
            None => Ok(
                PyList::new(py, pieces.into_iter().map(|piece| piece.into_object(py)))?.into_any(),
            ),
        }
    }

    /// Write records of the record type to a stream, handing any record that is not of the type,
    /// or whose line needs quoting, to `write_one` once the lines before it are written.
    ///
    /// Lines of a list or tuple of records are written in large chunks. Lines of any other
    /// iterable are written one at a time, since reading it may write to the stream too, e.g. a
    /// reader handing comments to `write_comment`.
    fn write_all(
        &self,
        py: Python<'_>,
        records: &Bound<'_, PyAny>,
        record_type: &Bound<'_, PyType>,
        write: &Bound<'_, PyAny>,
        write_one: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let mut chunk = String::with_capacity(CHUNK);
        let written = self.write_chunks(py, records, record_type, write, write_one, &mut chunk);
        let flushed = if chunk.is_empty() {
            Ok(())
        } else {
            write.call1((chunk.as_str(),)).map(drop)
        };
        written.and(flushed)
    }
}

impl RecordFormatter {
    /// Writes records into chunks, writing each chunk once it is full, and leaving the last.
    fn write_chunks(
        &self,
        py: Python<'_>,
        records: &Bound<'_, PyAny>,
        record_type: &Bound<'_, PyType>,
        write: &Bound<'_, PyAny>,
        write_one: &Bound<'_, PyAny>,
        chunk: &mut String,
    ) -> PyResult<()> {
        let limit = if records.is_exact_instance_of::<PyList>()
            || records.is_exact_instance_of::<PyTuple>()
        {
            CHUNK
        } else {
            1
        };
        for record in records.try_iter()? {
            let record = record?;
            let line = if record.is_instance(record_type)? {
                self.line(&self.pieces(py, &record)?)
            } else {
                None
            };
            if let Some(line) = line {
                chunk.push_str(&line);
                if chunk.len() >= limit {
                    write.call1((chunk.as_str(),))?;
                    chunk.clear();
                }
                continue;
            }
            if !chunk.is_empty() {
                write.call1((chunk.as_str(),))?;
                chunk.clear();
            }
            write_one.call1((record,))?;
        }
        Ok(())
    }

    /// Writes each field of a record into its text.
    fn pieces<'py>(
        &self,
        py: Python<'py>,
        record: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Piece<'py>>> {
        let mut pieces: Vec<Piece<'py>> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let value = record.getattr(field.name.bind(py))?;
            match field.out {
                Out::Many => {
                    for text in field.write.bind(py).call1((value,))?.try_iter()? {
                        pieces.push(Piece::Object(text?));
                    }
                }
                _ if value.is_none() => {
                    pieces.push(Piece::Object(field.none.bind(py).clone().into_any()));
                }
                Out::Call => pieces.push(Piece::Object(field.write.bind(py).call1((value,))?)),
                Out::Value => pieces.push(match self.native(py, &value)? {
                    Some(piece) => piece,
                    None => Piece::Object(field.write.bind(py).call1((value,))?),
                }),
            }
        }
        if let Some((name, check)) = &self.extra {
            let extra = record.getattr(name.bind(py))?;
            for text in check.bind(py).call1((extra,))?.try_iter()? {
                pieces.push(Piece::Object(text?));
            }
        }
        Ok(pieces)
    }

    /// Writes a value natively, as typeline does: exact strings as they are, integers, floats,
    /// and booleans as JSON, and any other value as its JSON text, or the string it encodes to.
    /// Returns None when the value cannot be encoded, for the field's writer to explain.
    fn native<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Option<Piece<'py>>> {
        if value.is_exact_instance_of::<PyString>() {
            return Ok(Some(Piece::Object(value.clone())));
        }
        if value.is_exact_instance_of::<PyBool>() {
            return Ok(Some(Piece::Static(if value.is_truthy()? {
                "true"
            } else {
                "false"
            })));
        }
        if value.is_exact_instance_of::<PyInt>() {
            return Ok(Some(match value.extract::<i64>() {
                Ok(number) => Piece::Owned(itoa::Buffer::new().format(number).to_string()),
                Err(_) => Piece::Object(value.str()?.into_any()),
            }));
        }
        if value.is_exact_instance_of::<PyFloat>() {
            let number = value.extract::<f64>()?;
            return Ok(Some(if number.is_finite() {
                Piece::Owned(json_float(number))
            } else {
                Piece::Object(value.repr()?.into_any())
            }));
        }
        let Ok(json) = self.encode.bind(py).call1((value,)) else {
            return Ok(None);
        };
        let json = json.cast::<PyBytes>()?.as_bytes();
        Ok(json_text(json).map(Piece::Owned))
    }

    /// Joins texts into a line when the csv module would write them as they are, else None.
    fn line(&self, pieces: &[Piece<'_>]) -> Option<String> {
        let mut line = String::new();
        for (index, piece) in pieces.iter().enumerate() {
            let text = piece.text()?;
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
