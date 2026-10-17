//! Writing records natively as lines of delimited text.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

use crate::text::json_float;

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
pub struct RecordFormatter {
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
    if value.is_exact_instance_of::<PyInt>() {
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
