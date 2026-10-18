//! Splitting delimited text into rows of fields as Python's `csv` module does, skipping the blank
//! lines and comment lines between rows.

use csv_core::{ReadRecordResult, Reader, ReaderBuilder};
use memchr::{memchr_iter, memchr2, memchr2_iter};

/// How many bytes of text are read from a source at a time.
pub const CHUNK: usize = 1 << 16;

/// The byte order mark that may start UTF-8 text.
const BOM: &[u8] = b"\xef\xbb\xbf";

/// Where text comes from, a chunk at a time.
pub trait Source {
    /// The error reading text may fail with.
    type Error;

    /// Appends the next chunk of text, returning false once there is none left.
    fn read(&mut self, text: &mut String) -> Result<bool, Self::Error>;
}

/// What a row reader finds next.
pub enum Event<'a> {
    /// A comment line, without its line ending.
    Comment {
        /// The line the comment is on, counting from 1.
        line: u64,
        /// The text of the comment, with its prefix.
        text: &'a str,
    },
    /// A row of fields.
    Row {
        /// The line the row starts on, counting from 1.
        line: u64,
        /// The fields of the row.
        row: Row<'a>,
    },
}

/// The fields of a row.
#[derive(Clone, Copy)]
pub struct Row<'a> {
    text: &'a str,
    ends: &'a [usize],
}

impl<'a> Row<'a> {
    /// The number of fields in the row.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// Whether the row has no fields.
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The field at an index, if the row has one there.
    pub fn get(&self, index: usize) -> Option<&'a str> {
        let end = *self.ends.get(index)?;
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        self.text.get(start..end)
    }

    /// The fields of the row, in order.
    pub fn iter(&self) -> impl Iterator<Item = &'a str> + '_ {
        (0..self.len()).filter_map(|index| self.get(index))
    }
}

/// How rows are written in the text.
#[derive(Clone)]
pub struct Options {
    /// The ASCII byte between fields.
    pub delimiter: u8,
    /// Whether `"` quotes fields, or is ordinary text.
    pub quoting: bool,
    /// The prefixes of comment lines, which are skipped between rows.
    pub comment_prefixes: Vec<String>,
}

/// Reads rows of fields from text, one at a time.
///
/// Between rows, blank lines (holding only whitespace and no delimiter) are skipped, and lines
/// starting with a comment prefix are returned as comments. A byte order mark that starts the text
/// is dropped. Lines end at `\n`, `\r`, or `\r\n`, and a quoted field may hold any of them.
pub struct Rows<S> {
    source: S,
    options: Options,
    reader: Reader,
    input: Vec<u8>,
    at: usize,
    exhausted: bool,
    chunk: String,
    output: Vec<u8>,
    ends: Vec<usize>,
    lines: u64,
    after_cr: bool,
    started: bool,
}

impl<S: Source> Rows<S> {
    /// Reads rows from a source of text.
    pub fn new(source: S, options: Options) -> Self {
        let reader = ReaderBuilder::new()
            .delimiter(options.delimiter)
            .quoting(options.quoting)
            .quote(b'"')
            .double_quote(true)
            .build();
        let mut rows = Self {
            source,
            options,
            reader,
            input: Vec::new(),
            at: 0,
            exhausted: false,
            chunk: String::new(),
            output: vec![0; 1024],
            ends: vec![0; 64],
            lines: 0,
            after_cr: false,
            started: false,
        };
        rows.restart();
        rows
    }

    /// Reads the source from its start again, e.g. once it holds new text, forgetting the text
    /// read so far and keeping the buffers, which building a reader anew would allocate again.
    pub fn restart(&mut self) {
        self.reader.reset();
        // csv-core drops a byte order mark that starts the first row it reads, wherever that row
        // is, so a blank line is read first; `read` itself drops one only on the first line.
        let _ = self.reader.read_record(b"\n", &mut [0], &mut [0]);
        self.input.clear();
        self.at = 0;
        self.exhausted = false;
        self.lines = 0;
        self.after_cr = false;
        self.started = false;
    }

    /// The source of text.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// The source of text, to give it more text.
    pub fn source_mut(&mut self) -> &mut S {
        &mut self.source
    }

    /// How rows are written in the text.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Reads the next comment or row, or None at the end of the text.
    pub fn read(&mut self) -> Result<Option<Event<'_>>, S::Error> {
        loop {
            if !self.fill_line()? {
                return Ok(None);
            }
            if self.after_cr && self.input[self.at] == b'\n' {
                self.at += 1;
                self.after_cr = false;
                continue;
            }
            self.after_cr = false;
            if !self.started {
                self.started = true;
                if self.input[self.at..].starts_with(BOM) {
                    self.at += BOM.len();
                    continue;
                }
            }
            let start = self.at;
            let end = self.line_end();
            let line = &self.input[start..end];
            let number = self.lines + 1;
            if self.is_blank(line) {
                self.skip_line(end);
                continue;
            }
            if self.is_comment(line) {
                self.skip_line(end);
                let text = text_of(&self.input[start..end]).trim_end_matches(['\r', '\n']);
                return Ok(Some(Event::Comment { line: number, text }));
            }
            return self.read_row(number);
        }
    }

    /// Makes sure a whole line, with its line ending, is buffered, or returns false at the end.
    fn fill_line(&mut self) -> Result<bool, S::Error> {
        loop {
            let rest = &self.input[self.at..];
            let complete = match memchr2(b'\r', b'\n', rest) {
                Some(found) => rest[found] == b'\n' || found + 1 < rest.len(),
                None => false,
            };
            if complete || self.exhausted {
                return Ok(!rest.is_empty());
            }
            self.refill()?;
        }
    }

    /// Returns where the buffered line at the read position ends, after its line ending.
    fn line_end(&self) -> usize {
        let rest = &self.input[self.at..];
        match memchr2(b'\r', b'\n', rest) {
            Some(found) if rest[found] == b'\r' && rest.get(found + 1) == Some(&b'\n') => {
                self.at + found + 2
            }
            Some(found) => self.at + found + 1,
            None => self.input.len(),
        }
    }

    /// Skips a line that is not part of a row.
    fn skip_line(&mut self, end: usize) {
        self.lines += u64::from(matches!(self.input[end - 1], b'\r' | b'\n'));
        self.at = end;
    }

    /// Returns whether a line holds only whitespace and no delimiter, as Python's `str.isspace`.
    fn is_blank(&self, line: &[u8]) -> bool {
        match line.first() {
            Some(&byte) if byte.is_ascii() && !is_ascii_python_space(byte) => false,
            _ => {
                !line.contains(&self.options.delimiter)
                    && !line.is_empty()
                    && text_of(line).chars().all(crate::text::is_python_space)
            }
        }
    }

    /// Returns whether a line starts with a comment prefix.
    fn is_comment(&self, line: &[u8]) -> bool {
        self.options
            .comment_prefixes
            .iter()
            .any(|prefix| line.starts_with(prefix.as_bytes()))
    }

    /// Reads the row that starts at the read position.
    fn read_row(&mut self, line: u64) -> Result<Option<Event<'_>>, S::Error> {
        let (mut out, mut ends) = (0, 0);
        loop {
            let input = &self.input[self.at..];
            let (result, read, written, ended) =
                self.reader
                    .read_record(input, &mut self.output[out..], &mut self.ends[ends..]);
            self.count_lines(self.at, self.at + read);
            self.at += read;
            out += written;
            ends += ended;
            match result {
                ReadRecordResult::InputEmpty => {
                    if !self.exhausted {
                        self.refill()?;
                    }
                }
                ReadRecordResult::OutputFull => self.output.resize(self.output.len() * 2, 0),
                ReadRecordResult::OutputEndsFull => self.ends.resize(self.ends.len() * 2, 0),
                ReadRecordResult::Record => {
                    let text = text_of(&self.output[..out]);
                    let row = Row {
                        text,
                        ends: &self.ends[..ends],
                    };
                    return Ok(Some(Event::Row { line, row }));
                }
                ReadRecordResult::End => return Ok(None),
            }
        }
    }

    /// Counts the lines that end in a span of the input, where `\r\n` ends one line.
    fn count_lines(&mut self, start: usize, end: usize) {
        let span = &self.input[start..end];
        if span.is_empty() {
            return;
        }
        let breaks = memchr2_iter(b'\r', b'\n', span).count() as u64;
        let mut pairs = memchr_iter(b'\n', span)
            .filter(|&found| found > 0 && span[found - 1] == b'\r')
            .count() as u64;
        if self.after_cr && span[0] == b'\n' {
            pairs += 1;
        }
        self.lines += breaks - pairs;
        self.after_cr = span[span.len() - 1] == b'\r';
    }

    /// Reads another chunk of text, first dropping the input already read, and passing over any
    /// empty chunk a source hands over before its end, which would read as the end of the text.
    fn refill(&mut self) -> Result<(), S::Error> {
        self.input.drain(..self.at);
        self.at = 0;
        loop {
            self.chunk.clear();
            if !self.source.read(&mut self.chunk)? {
                self.exhausted = true;
                return Ok(());
            }
            if !self.chunk.is_empty() {
                self.input.extend_from_slice(self.chunk.as_bytes());
                return Ok(());
            }
        }
    }
}

/// Where text stands in the quoting of its fields.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Quotes {
    /// At the start of a field, where a quote opens a quoted field.
    FieldStart,
    /// In a field that is not quoted, where a quote is text.
    Unquoted,
    /// In a quoted field, which a quote closes.
    Quoted,
    /// Just after a quote closed a quoted field, where another quote reopens it as text.
    Closed,
}

/// Returns whether text, read as rows, ends inside a quoted field that nothing closes.
pub fn ends_in_quotes(text: &str, delimiter: u8) -> bool {
    let mut quotes = Quotes::FieldStart;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    for &byte in text.as_bytes() {
        quotes = match (quotes, byte) {
            (Quotes::Quoted, b'"') => Quotes::Closed,
            (Quotes::Quoted, _) | (Quotes::FieldStart | Quotes::Closed, b'"') => Quotes::Quoted,
            (_, b'\r' | b'\n') => Quotes::FieldStart,
            (_, byte) if byte == delimiter => Quotes::FieldStart,
            _ => Quotes::Unquoted,
        };
    }
    quotes == Quotes::Quoted
}

/// Returns whether an ASCII byte is whitespace to Python's `str.isspace`.
fn is_ascii_python_space(byte: u8) -> bool {
    matches!(byte, b'\t'..=b'\r' | 0x1c..=b' ')
}

/// Returns bytes read from UTF-8 text, split only around ASCII bytes, as text.
fn text_of(bytes: &[u8]) -> &str {
    debug_assert!(std::str::from_utf8(bytes).is_ok());
    // SAFETY: the input is UTF-8, since sources append whole strings, and lines and fields are
    // only ever split at, or stripped of, ASCII bytes, which never fall inside a UTF-8 character.
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use proptest::prelude::*;

    use super::*;

    /// Text served in chunks of a fixed number of characters.
    struct Chunks {
        chars: Vec<char>,
        size: usize,
        gaps: bool,
        gap: bool,
    }

    impl Source for Chunks {
        type Error = Infallible;

        fn read(&mut self, text: &mut String) -> Result<bool, Infallible> {
            if self.chars.is_empty() {
                return Ok(false);
            }
            self.gap = !self.gap;
            if self.gaps && self.gap {
                return Ok(true);
            }
            let size = self.size.min(self.chars.len());
            text.extend(self.chars.drain(..size));
            Ok(true)
        }
    }

    /// Text served as the given pieces, in order, some of which may be empty.
    struct Pieces(Vec<&'static str>);

    impl Source for Pieces {
        type Error = Infallible;

        fn read(&mut self, text: &mut String) -> Result<bool, Infallible> {
            if self.0.is_empty() {
                return Ok(false);
            }
            text.push_str(self.0.remove(0));
            Ok(true)
        }
    }

    #[test]
    fn rows_read_on_past_an_empty_piece_inside_a_quoted_field() {
        let options = Options {
            delimiter: b'\t',
            quoting: true,
            comment_prefixes: vec!["#".to_string()],
        };
        let mut rows = Rows::new(Pieces(vec!["\"a\n", "", "", "b\"\tx\n"]), options);
        let Ok(Some(Event::Row { line, row })) = rows.read() else {
            panic!("expected a row");
        };
        assert_eq!(
            (line, row.iter().collect::<Vec<_>>()),
            (1, vec!["a\nb", "x"])
        );
        assert!(matches!(rows.read(), Ok(None)));
    }

    /// Reads every event left in rows of text, as owned values.
    fn drain(rows: &mut Rows<Chunks>) -> Vec<(u64, Result<Vec<String>, String>)> {
        let mut found = Vec::new();
        while let Some(event) = rows.read().unwrap() {
            found.push(match event {
                Event::Comment { line, text } => (line, Err(text.to_string())),
                Event::Row { line, row } => (line, Ok(row.iter().map(str::to_string).collect())),
            });
        }
        found
    }

    /// Reads every event of a text, served in chunks of a size, as owned values.
    fn events(text: &str, size: usize, quoting: bool) -> Vec<(u64, Result<Vec<String>, String>)> {
        events_with_gaps(text, size, quoting, false)
    }

    /// Reads every event of a text, served in chunks of a size, with an empty chunk before each
    /// chunk of text when there are gaps, as owned values.
    fn events_with_gaps(
        text: &str,
        size: usize,
        quoting: bool,
        gaps: bool,
    ) -> Vec<(u64, Result<Vec<String>, String>)> {
        let source = Chunks {
            chars: text.chars().collect(),
            size,
            gaps,
            gap: false,
        };
        let options = Options {
            delimiter: b'\t',
            quoting,
            comment_prefixes: vec!["#".to_string()],
        };
        drain(&mut Rows::new(source, options))
    }

    fn row(line: u64, fields: &[&str]) -> (u64, Result<Vec<String>, String>) {
        (
            line,
            Ok(fields.iter().map(|field| (*field).to_string()).collect()),
        )
    }

    fn comment(line: u64, text: &str) -> (u64, Result<Vec<String>, String>) {
        (line, Err(text.to_string()))
    }

    #[test]
    fn rows_skip_blank_lines_and_report_comments_with_their_lines() {
        let text = "\u{feff}# head\r\na\tb\n\n  \r\n\t\n# mid\nc\t\"d\ne\"\n\"x\"\"y\"z\t\n";
        for size in [1, 2, 3, 7, 1000] {
            assert_eq!(
                events(text, size, true),
                vec![
                    comment(1, "# head"),
                    row(2, &["a", "b"]),
                    row(5, &["", ""]),
                    comment(6, "# mid"),
                    row(7, &["c", "d\ne"]),
                    row(9, &["x\"yz", ""]),
                ],
                "chunks of {size}"
            );
        }
    }

    #[test]
    fn rows_end_lines_at_cr_lf_and_crlf() {
        for size in [1, 2, 5, 1000] {
            assert_eq!(
                events("a\rb\r\nc\nd", size, true),
                vec![
                    row(1, &["a"]),
                    row(2, &["b"]),
                    row(3, &["c"]),
                    row(4, &["d"])
                ],
                "chunks of {size}"
            );
        }
    }

    #[test]
    fn rows_without_quoting_keep_quotes_as_text() {
        assert_eq!(
            events("\"a\tb\"\n", 1000, false),
            vec![row(1, &["\"a", "b\""])]
        );
    }

    #[test]
    fn ends_in_quotes_tracks_quotes_as_rows_read_them() {
        for text in [
            "a\t\"b",
            "a\t\"b\nc\td\n",
            "\"a\"\"",
            "\"a\"\"b\"\"\"\"",
            "\u{feff}\"a",
        ] {
            assert!(ends_in_quotes(text, b'\t'), "{text:?}");
        }
        for text in [
            "",
            "a\tb\n",
            "\"a\"\tb",
            "a\"b\n",
            "\"a\"\"b\"\n",
            "\"a\"c\"\n",
            "a\"\n",
            "\u{feff}\u{feff}\"",
        ] {
            assert!(!ends_in_quotes(text, b'\t'), "{text:?}");
        }
    }

    #[test]
    fn rows_restarted_on_new_text_read_it_as_a_new_reader_would() {
        let text = "\u{feff}# head\r\na\tb\n\n\"c\n";
        let options = Options {
            delimiter: b'\t',
            quoting: true,
            comment_prefixes: vec!["#".to_string()],
        };
        let mut rows = Rows::new(
            Chunks {
                chars: text.chars().collect(),
                size: 2,
                gaps: false,
                gap: false,
            },
            options,
        );
        assert_eq!(drain(&mut rows), events(text, 2, true));
        for text in ["x\ty\r", "\u{feff}", text, ""] {
            rows.source_mut().chars = text.chars().collect();
            rows.restart();
            assert_eq!(drain(&mut rows), events(text, 2, true), "{text:?}");
        }
    }

    #[test]
    fn rows_only_skip_a_byte_order_mark_that_starts_the_text() {
        assert_eq!(
            events("a\n\u{feff}b\n", 1000, true),
            vec![row(1, &["a"]), row(2, &["\u{feff}b"])]
        );
        assert_eq!(
            events("\n\u{feff}\"b\"\n", 1000, true),
            vec![row(2, &["\u{feff}\"b\""])]
        );
        assert_eq!(events("\u{feff}\"b\"\n", 1000, true), vec![row(1, &["b"])]);
    }

    proptest! {
        #[test]
        fn rows_are_the_same_in_chunks_of_any_size(
            text in "[a\"\t\r\n# ]{0,40}",
            size in 1usize..8,
            gaps in any::<bool>(),
        ) {
            for quoting in [true, false] {
                prop_assert_eq!(
                    events_with_gaps(&text, size, quoting, gaps),
                    events(&text, 1 << 20, quoting)
                );
            }
        }

        #[test]
        fn rows_read_back_quoted_fields(fields in prop::collection::vec("[a-z\"\t\r\n ]{0,6}", 1..5)) {
            let line: Vec<String> = fields
                .iter()
                .map(|field| format!("\"{}\"", field.replace('"', "\"\"")))
                .collect();
            let text = format!("x\n{}\n", line.join("\t"));
            let found = events(&text, 3, true);
            prop_assert_eq!(found.len(), 2);
            prop_assert_eq!(&found[1].1, &Ok(fields.clone()));
        }
    }
}
