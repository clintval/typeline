from dataclasses import dataclass
from io import StringIO
from pathlib import Path

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
