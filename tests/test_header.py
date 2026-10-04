from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from time import perf_counter
from typing import Any
from typing import TextIO

import pytest
from typing_extensions import Unpack
from typing_extensions import override

from typeline import CsvReader
from typeline import ExtraColumns
from typeline import RecordType
from typeline import TsvReader
from typeline import TsvWriter
from typeline import WriterOptions

from .records import SimpleMetric


@dataclass(frozen=True)
class Region:
    """A record with two named fields and any number of extra columns."""

    name: str
    start: int
    extra: ExtraColumns = ()


@dataclass(frozen=True)
class Tagged:
    """A record with one named field and any number of extra columns."""

    name: str
    extra: ExtraColumns = ()


def test_reader_matches_columns_to_fields_by_header_name(tmp_path: Path) -> None:
    """Test that a header's columns may come in any order."""
    _ = (tmp_path / "test.tsv").write_text("field3\tfield1\tfield2\n0.2\t1\tname\n")

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv") as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_keeps_unnamed_columns_in_extra_columns_in_file_order(tmp_path: Path) -> None:
    """Test that columns a record doesn't name go to its ExtraColumns field, in file order."""
    _ = (tmp_path / "test.tsv").write_text("score\tstart\tcall\tname\n0.9\t10\tHIGH\texon1\n")

    with TsvReader.from_path[Region](tmp_path / "test.tsv") as reader:
        assert list(reader) == [Region("exon1", 10, ("0.9", "HIGH"))]


def test_reader_refuses_a_header_that_repeats_a_column(tmp_path: Path) -> None:
    """Test that a header naming a column twice is refused, since the column would be ambiguous."""
    _ = (tmp_path / "test.tsv").write_text("field1\tfield2\tfield3\tfield1\n")

    message = r"^Columns of header repeat a name on line 1! .* Repeated in header: \['field1'\]\.$"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv")


def test_reader_still_counts_fields_on_each_row_of_a_reordered_file(tmp_path: Path) -> None:
    """Test that a row of a reordered file with the wrong number of fields is still reported."""
    _ = (tmp_path / "test.tsv").write_text("field3\tfield1\tfield2\n0.2\t1\n")

    message = r"^Expected 3 columns but found 2 on line 2 for record type: SimpleMetric\.$"
    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv") as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)


def test_extra_columns_may_repeat_a_name() -> None:
    """Test that columns no field takes may share a name, since they are kept by position."""
    with TsvReader[Tagged](StringIO("info\tname\tinfo\na\tx\tb\n")) as reader:
        assert list(reader) == [Tagged("x", ("a", "b"))]


def test_a_repeated_field_name_is_refused() -> None:
    """Test that a header naming a field twice is refused."""
    with pytest.raises(ValueError, match=r"Repeated in header: \['name'\]\.$"):
        _ = TsvReader[Tagged](StringIO("name\tinfo\tname\n"))


def test_a_wide_header_is_matched_quickly() -> None:
    """Test that matching a header of many extra columns takes linear time."""
    names = [f"sample{index}" for index in range(50_000)]
    header = "\t".join(["name", *names])

    start = perf_counter()
    _ = TsvReader[Tagged](StringIO(f"{header}\n"))

    assert perf_counter() - start < 1.0


@pytest.mark.parametrize(
    "header,detail",
    [
        pytest.param(
            "field1\tfield2",
            "Header: ['field1', 'field2']. Columns of SimpleMetric: ['field1', 'field2', 'field3']."
            + " Missing from header: ['field3'].",
            id="missing",
        ),
        pytest.param(
            "field1\tfield2\tfield3\tfield4",
            "Unexpected in header: ['field4'].",
            id="unexpected",
        ),
        pytest.param(
            "field1\tfield2\tfield4",
            "Missing from header: ['field3']. Unexpected in header: ['field4'].",
            id="missing-and-unexpected",
        ),
    ],
)
def test_reader_names_how_the_header_differs_from_the_dataclass(
    tmp_path: Path, header: str, detail: str
) -> None:
    """Test the header mismatch error shows both headers and how they differ."""
    _ = (tmp_path / "test.txt").write_text(f"{header}\n")

    with pytest.raises(ValueError) as exception:
        _ = TsvReader.from_path[SimpleMetric](tmp_path / "test.txt")

    assert str(exception.value).startswith("Columns of header do not match fields of ")
    assert detail in str(exception.value)


def test_a_header_mismatch_names_columns_fields_and_line() -> None:
    """Test that a header that does not match is reported in columns and fields, with its line."""
    message = r"^Columns of header do not match fields of SimpleMetric on line 2! Header: "
    with pytest.raises(ValueError, match=message):
        _ = CsvReader[SimpleMetric](StringIO("# a comment\nfield1,field2\n"))


@dataclass(frozen=True)
class Point:
    """A record of two integers."""

    x: int
    y: int


def test_writer_with_header_writes_it_once_before_the_first_record() -> None:
    """Test that a writer with header=True writes its header before its first record, once."""
    stream = StringIO()
    writer = TsvWriter[Point](stream, header=True)
    assert stream.getvalue() == ""

    writer.write(Point(1, 2))
    writer.write(Point(3, 4))
    writer.write_all([Point(5, 6)])

    assert stream.getvalue() == "x\ty\n1\t2\n3\t4\n5\t6\n"


def test_writer_with_header_writes_earlier_comments_above_it() -> None:
    """Test that comments written before the first record come before the header."""
    stream = StringIO()
    writer = TsvWriter[Point](stream, header=True)
    writer.write_comment("made by a tool")
    writer.write(Point(1, 2))
    writer.write_comment("between")
    writer.write(Point(3, 4))

    assert stream.getvalue() == "# made by a tool\nx\ty\n1\t2\n# between\n3\t4\n"


def test_writer_with_header_keeps_the_comments_of_a_file_in_place(tmp_path: Path) -> None:
    """Test that comments a reader streams into write_all keep their places around the header."""
    text = "# top\nx\ty\n# under the header\n1\t2\n# between\n3\t4\n"
    _ = (tmp_path / "in.tsv").write_text(text)

    with (
        TsvWriter.from_path[Point](tmp_path / "out.tsv", header=True) as writer,
        TsvReader.from_path[Point](tmp_path / "in.tsv", on_comment=writer.write_comment) as reader,
    ):
        writer.write_all(reader)

    assert (tmp_path / "out.tsv").read_text() == text


def test_writer_with_header_writes_the_header_alone_when_no_record_is_written(
    tmp_path: Path,
) -> None:
    """Test that a writer with header=True writes a header-only file when it writes no records."""
    with TsvWriter.from_path[Point](tmp_path / "closed.tsv", header=True):
        pass
    with TsvWriter.from_path[Point](tmp_path / "write_all.tsv", header=True) as writer:
        writer.write_all([])
    with TsvWriter.from_path[Point](tmp_path / "commented.tsv", header=True) as writer:
        writer.write_comment("no points")

    assert (tmp_path / "closed.tsv").read_text() == "x\ty\n"
    assert (tmp_path / "write_all.tsv").read_text() == "x\ty\n"
    assert (tmp_path / "commented.tsv").read_text() == "# no points\nx\ty\n"
    with TsvReader.from_path[Point](tmp_path / "closed.tsv") as reader:
        assert list(reader) == []


def test_write_all_with_header_writes_the_header_to_an_open_stream() -> None:
    """Test that write_all writes the header as it starts, before any record is read."""
    stream = StringIO()
    TsvWriter[Point](stream, header=True).write_all([])

    assert stream.getvalue() == "x\ty\n"


def test_writer_without_header_writes_an_empty_file(tmp_path: Path) -> None:
    """Test that a writer writes no header by default, so writing no records leaves a file empty."""
    with TsvWriter.from_path[Point](tmp_path / "test.tsv") as writer:
        writer.write_all([])

    assert (tmp_path / "test.tsv").read_text() == ""


def test_write_header_with_header_writes_the_header_only_once() -> None:
    """Test that write_header on a writer with header=True writes the header now, or not at all."""
    stream = StringIO()
    writer = TsvWriter[Point](stream, header=True)
    writer.write_header()
    writer.write_comment("after the header")
    writer.write_header()
    writer.write(Point(1, 2))
    writer.write_header()

    assert stream.getvalue() == "x\ty\n# after the header\n1\t2\n"


def test_write_header_without_header_writes_the_header_each_time() -> None:
    """Test that write_header on a writer without header=True writes the header when called."""
    stream = StringIO()
    writer = TsvWriter[Point](stream)
    writer.write_header()
    writer.write(Point(1, 2))
    writer.write_header()

    assert stream.getvalue() == "x\ty\n1\t2\nx\ty\n"


@pytest.mark.parametrize(
    "record,quoting,message",
    [
        pytest.param(Point(1, 2), True, r"^Expected Region but found Point!$", id="wrong-type"),
        pytest.param(
            Region("a\tb", 1),
            False,
            r"^Cannot write field 'name' of Region without quoting",
            id="unquotable",
        ),
    ],
)
def test_writer_with_header_writes_nothing_for_a_refused_record(
    record: Any, quoting: bool, message: str
) -> None:
    """Test that a record a writer refuses writes nothing, not even the header."""
    stream = StringIO()
    writer = TsvWriter[Region](stream, header=True, quoting=quoting)

    with pytest.raises(ValueError, match=message):
        writer.write(record)

    assert stream.getvalue() == ""
    writer.write(Region("a", 1))
    assert stream.getvalue() == "name\tstart\na\t1\n"


def test_writer_with_header_writes_it_once_when_closed_twice(tmp_path: Path) -> None:
    """Test that closing a writer with header=True again writes nothing more."""
    writer = TsvWriter.from_path[Point](tmp_path / "test.tsv", header=True)
    writer.close()
    writer.close()

    assert (tmp_path / "test.tsv").read_text() == "x\ty\n"


def test_writer_with_header_and_without_quoting_refuses_an_unwritable_header(
    tmp_path: Path,
) -> None:
    """Test that a header that cannot be written without quoting is refused when built."""
    path = tmp_path / "test.tsv"
    _ = path.write_text("keep me\n")
    rename = {"x": "#x"}

    with pytest.raises(ValueError, match=r"^Cannot write field 'x' in column '#x' of Point"):
        _ = TsvWriter.from_path[Point](path, header=True, quoting=False, rename=rename)

    assert path.read_text() == "keep me\n"
    _ = TsvWriter[Point](StringIO(), quoting=False, rename=rename)


def test_a_format_may_write_a_header_by_default(tmp_path: Path) -> None:
    """Test that a writer subclass can make header=True its default, which callers may turn off."""

    class HeaderTsvWriter(TsvWriter[RecordType]):
        """A tab-delimited writer that writes its header by default."""

        @override
        def __init__(self, handle: TextIO, /, **options: Unpack[WriterOptions]) -> None:
            _ = options.setdefault("header", True)
            super().__init__(handle, **options)

    with HeaderTsvWriter.from_path[Point](tmp_path / "with.tsv") as writer:
        writer.write(Point(1, 2))
    with HeaderTsvWriter.from_path[Point](tmp_path / "without.tsv", header=False) as writer:
        writer.write(Point(1, 2))

    assert (tmp_path / "with.tsv").read_text() == "x\ty\n1\t2\n"
    assert (tmp_path / "without.tsv").read_text() == "1\t2\n"
