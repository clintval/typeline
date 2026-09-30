from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from typeline import Comment
from typeline import CsvReader
from typeline import CsvWriter
from typeline import TsvReader
from typeline import TsvWriter


@dataclass(frozen=True)
class Point:
    """A record for testing comments."""

    x: int
    y: int


def test_reader_sends_comments_with_their_line_numbers(tmp_path: Path) -> None:
    """Test that the reader sends each comment line, with its line number, to on_comment."""
    path = tmp_path / "test.csv"
    _ = path.write_text("# made by a tool\nx,y\n# first point\n1,2\n\n# last point\n3,4\n")

    comments: list[Comment] = []
    with CsvReader.from_path[Point](
        path, comment_prefixes={"#"}, on_comment=comments.append
    ) as reader:
        assert comments == [Comment(line_number=1, text="# made by a tool")]
        assert list(reader) == [Point(1, 2), Point(3, 4)]

    assert comments == [
        Comment(line_number=1, text="# made by a tool"),
        Comment(line_number=3, text="# first point"),
        Comment(line_number=6, text="# last point"),
    ]


def test_reader_drops_comments_by_default(tmp_path: Path) -> None:
    """Test that the reader skips comments when no on_comment is given."""
    path = tmp_path / "test.csv"
    _ = path.write_text("# a comment\nx,y\n1,2\n")

    with CsvReader.from_path[Point](path, comment_prefixes={"#"}) as reader:
        assert list(reader) == [Point(1, 2)]


def test_writer_prefixes_comment_lines_that_do_not_have_a_prefix() -> None:
    """Test that the writer adds its first prefix to each comment line that lacks one."""
    stream = StringIO()
    writer = CsvWriter[Point](stream)
    writer.write_comment("made by a tool\n## already a comment")
    writer.write_header()
    writer.write(Point(1, 2))

    assert stream.getvalue() == "# made by a tool\n## already a comment\nx,y\n1,2\n"


def test_writer_keeps_comment_lines_that_start_with_any_of_its_prefixes() -> None:
    """Test that the writer keeps comment lines that start with any of its prefixes."""
    stream = StringIO()
    writer = TsvWriter[Point](stream, comment_prefixes=("#", "browser", "track"))
    writer.write_comment("track name=points\nbrowser position chr1\nsome notes")

    assert stream.getvalue() == "track name=points\nbrowser position chr1\n# some notes\n"


def test_writer_writes_a_comment_that_was_read_as_it_was() -> None:
    """Test that the writer writes a Comment exactly as it was read, whatever its prefix."""
    stream = StringIO()
    writer = CsvWriter[Point](stream)
    writer.write_comment(Comment(line_number=1, text="browser position chr1"))

    assert stream.getvalue() == "browser position chr1\n"


def test_comments_stream_from_a_reader_into_a_writer(tmp_path: Path) -> None:
    """Test that on_comment can hand comments straight to a writer, keeping their places."""
    text = "##fileformat=points\nx\ty\n1\t2\n# between\n3\t4\n"
    _ = (tmp_path / "in.tsv").write_text(text)

    with (
        TsvWriter.from_path[Point](tmp_path / "out.tsv") as writer,
        TsvReader.from_path[Point](
            tmp_path / "in.tsv", comment_prefixes={"#"}, on_comment=writer.write_comment
        ) as reader,
    ):
        writer.write_header()
        for record in reader:
            writer.write(record)

    assert (tmp_path / "out.tsv").read_text() == text


@dataclass(frozen=True)
class Note:
    """A record of two optional text fields."""

    a: str | None
    b: str | None


def test_reader_keeps_a_row_of_empty_fields() -> None:
    """Test that a row whose fields are all empty is a record, not a blank line."""
    handle = StringIO()
    writer = TsvWriter[Note](handle)
    writer.write(Note(None, None))
    writer.write(Note("x", "y"))

    with TsvReader[Note](StringIO(handle.getvalue()), header=False) as reader:
        assert list(reader) == [Note(None, None), Note("x", "y")]


def test_reader_skips_whitespace_lines_but_keeps_rows_of_whitespace_fields() -> None:
    """Test that a line of only whitespace is blank, but one holding a delimiter is a record."""
    with CsvReader[Note](StringIO("  \n , \n\t\n"), header=False) as reader:
        assert list(reader) == [Note(" ", " ")]


def test_reader_keeps_a_row_that_starts_with_a_prefix_after_other_text() -> None:
    """Test that only lines starting with a comment prefix are comments."""
    text = "\t#x\n  #y\tz\n"
    with TsvReader[Note](StringIO(text), header=False, comment_prefixes={"#"}) as reader:
        assert list(reader) == [Note(None, "#x"), Note("  #y", "z")]


def test_reader_keeps_blank_and_comment_like_lines_inside_quoted_fields() -> None:
    """Test that lines inside a quoted field are text, never blank lines or comments."""
    comments: list[Comment] = []
    text = '# top\n"one\n\n#two\n",x\n\n# bottom\ny,z\n'

    with CsvReader[Note](
        StringIO(text), header=False, comment_prefixes={"#"}, on_comment=comments.append
    ) as reader:
        assert list(reader) == [Note("one\n\n#two\n", "x"), Note("y", "z")]

    assert comments == [Comment(1, "# top"), Comment(7, "# bottom")]


def test_writer_output_with_tricky_text_reads_back() -> None:
    """Test that records whose text looks like blank lines or comments read back unchanged."""
    records = [Note("a\n\nb", "#c"), Note("\n#d", None), Note(None, None)]
    handle = StringIO()
    writer = CsvWriter[Note](handle)
    for record in records:
        writer.write(record)

    with CsvReader[Note](
        StringIO(handle.getvalue()), header=False, comment_prefixes={"#"}
    ) as reader:
        assert list(reader) == records


@pytest.mark.parametrize("kind", [TsvReader, TsvWriter])
def test_a_string_of_comment_prefixes_is_refused(kind: Any) -> None:
    """Test that one string given as comment_prefixes is refused, not split into characters."""
    with pytest.raises(TypeError, match=r"^comment_prefixes must be a collection of strings"):
        _ = kind[Point](StringIO(), comment_prefixes="//")


def test_writer_writes_an_empty_comment_as_a_bare_prefix() -> None:
    """Test that an empty comment is written as a line holding only the first prefix."""
    handle = StringIO()
    TsvWriter[Point](handle).write_comment("")

    assert handle.getvalue() == "#\n"


def test_writer_splits_a_comment_only_at_line_breaks_the_reader_knows() -> None:
    """Test that a comment is split only at the line breaks a reader knows, keeping blank lines."""
    handle = StringIO()
    TsvWriter[Point](handle).write_comment("a\x1cb\r\nc\rd\n\ne\n")

    assert handle.getvalue() == "# a\x1cb\n# c\n# d\n#\n# e\n"


def test_writer_refuses_a_comment_object_holding_a_line_break() -> None:
    """Test that a Comment, written as it was read, may not hold a line break."""
    with pytest.raises(
        ValueError, match=r"^A Comment is one line, but this one holds a line break"
    ):
        TsvWriter[Point](StringIO()).write_comment(Comment(1, "# a\nb"))


def test_a_default_reader_reads_what_a_default_writer_writes_with_comments() -> None:
    """Test that readers and writers both treat lines starting with # as comments by default."""
    handle = StringIO()
    writer = TsvWriter[Point](handle)
    writer.write_comment("made by a tool")
    writer.write_header()
    writer.write(Point(1, 2))

    comments: list[Comment] = []
    with TsvReader[Point](StringIO(handle.getvalue()), on_comment=comments.append) as reader:
        assert list(reader) == [Point(1, 2)]
    assert comments == [Comment(1, "# made by a tool")]
