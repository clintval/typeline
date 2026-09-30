import csv
import math
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Optional  # pyright: ignore[reportDeprecated]
from typing import cast

import pytest

from typeline import CounterColumns
from typeline import CsvReader
from typeline import CsvWriter
from typeline import ExtraColumns
from typeline import TsvReader
from typeline import TsvWriter

from .records import SimpleMetric


def test_csv_writer_is_set_to_use_comma(tmp_path: Path) -> None:
    """Test that the CSV writer is set to use a comma."""
    with CsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"

    with CsvWriter[SimpleMetric](open(tmp_path / "test.txt", "w")) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"


def test_tsv_writer_is_set_to_use_tab(tmp_path: Path) -> None:
    """Test that the TSV writer is set to use a tab."""
    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"

    with TsvWriter[SimpleMetric](open(tmp_path / "test.txt", "w")) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"


def test_writer_can_write_old_style_optional_types(tmp_path: Path) -> None:
    """Test that the writer can write old style optional types."""

    @dataclass
    class MyMetric:
        field1: float
        field2: Optional[int]  # pyright: ignore[reportDeprecated]
        field3: Optional[list[int]]  # pyright: ignore[reportDeprecated]

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt") as writer:
        writer.write(MyMetric(0.1, 1, None))
        writer.write(MyMetric(0.2, None, [1, 2, 3]))

    assert (tmp_path / "test.txt").read_text() == '0.1,1,\n0.2,,"[1,2,3]"\n'


def test_writer_keeps_quotes_that_are_part_of_a_string(tmp_path: Path) -> None:
    """Test that the writer does not strip quote characters that belong to a string value."""

    @dataclass
    class MyMetric:
        field1: str
        field2: list[str]

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt") as writer:
        writer.write(MyMetric('"quoted"', ['a"b']))

    assert (tmp_path / "test.txt").read_text() == '"""quoted""","[""a\\""b""]"\n'

    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [MyMetric('"quoted"', ['a"b'])]


@pytest.mark.parametrize("none_field", ["", "null", "NA"])
def test_writer_writes_none_as_the_none_field(tmp_path: Path, none_field: str) -> None:
    """Test that the writer writes None as the none field."""

    @dataclass
    class MyMetric:
        field1: int | None
        field2: int | None

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt", none_field=none_field) as writer:
        writer.write(MyMetric(None, 1))

    assert (tmp_path / "test.txt").read_text() == f"{none_field},1\n"


def test_writer_and_reader_round_trip_none_with_their_defaults(tmp_path: Path) -> None:
    """Test that None is written as an empty field by default and read back as None."""

    @dataclass
    class MyMetric:
        name: str | None
        count: int | None
        values: list[int] | None

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt") as writer:
        writer.write(MyMetric(None, None, None))

    assert (tmp_path / "test.txt").read_text() == ",,\n"

    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [MyMetric(None, None, None)]


def test_empty_text_reads_back_as_none_only_in_optional_text_fields(tmp_path: Path) -> None:
    """Test that an empty field is None when the field allows None, and "" when it is a str."""

    @dataclass
    class MyMetric:
        required: str
        optional: str | None

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt") as writer:
        writer.write(MyMetric("", ""))
        writer.write(MyMetric("", None))

    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [MyMetric("", None), MyMetric("", None)]

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt", none_field="NA") as writer:
        writer.write(MyMetric("", ""))
        writer.write(MyMetric("", None))

    with CsvReader.from_path[MyMetric](
        tmp_path / "test.txt", header=False, none_field="NA"
    ) as reader:
        assert list(reader) == [MyMetric("", ""), MyMetric("", None)]


def test_writer_writes_standard_csv_quoting(tmp_path: Path) -> None:
    """Test that the writer quotes with double quotes, so other CSV tools read its output."""

    @dataclass
    class Contact:
        name: str
        notes: str
        tags: list[str]

    records = [Contact("Doe, Jane", 'said "hi"', ["a", "b"]), Contact("O'Brien", "it's fine", [])]
    with CsvWriter.from_path[Contact](tmp_path / "test.csv") as writer:
        for record in records:
            writer.write(record)

    assert (tmp_path / "test.csv").read_text() == (
        '"Doe, Jane","said ""hi""","[""a"",""b""]"\nO\'Brien,it\'s fine,[]\n'
    )

    with (tmp_path / "test.csv").open(newline="") as handle:
        assert list(csv.reader(handle)) == [
            ["Doe, Jane", 'said "hi"', '["a","b"]'],
            ["O'Brien", "it's fine", "[]"],
        ]


@dataclass
class Feature:
    """A record of free text written without quoting."""

    name: str
    notes: str
    extra: ExtraColumns = ()


def test_writer_writes_quotes_as_text_without_quoting(tmp_path: Path) -> None:
    """Test that without quoting the writer writes quotes as-is, and they read back as text."""
    records = [Feature('"quoted', 'a"b'), Feature("x", '"', ('say "hi"',))]
    with TsvWriter.from_path[Feature](tmp_path / "test.tsv", quoting=False) as writer:
        writer.write_header()
        for record in records:
            writer.write(record)

    assert (tmp_path / "test.tsv").read_text() == 'name\tnotes\n"quoted\ta"b\nx\t"\tsay "hi"\n'

    with TsvReader.from_path[Feature](tmp_path / "test.tsv", quoting=False) as reader:
        assert list(reader) == records


@pytest.mark.parametrize("text", ["a\tb", "a\nb", "a\rb"])
def test_writer_refuses_text_it_cannot_write_without_quoting(text: str) -> None:
    """Test that without quoting a field holding the delimiter or a line break is refused."""
    stream = StringIO()
    writer = TsvWriter[Feature](stream, quoting=False)

    with pytest.raises(ValueError, match="field 'notes' of Feature"):
        writer.write(Feature("x", text))

    with pytest.raises(ValueError, match="field 'extra' of Feature"):
        writer.write(Feature("x", "y", ("z", text)))

    assert stream.getvalue() == ""


def test_writer_refuses_a_lone_empty_field_without_quoting() -> None:
    """Test that without quoting a record of one empty field, which must be quoted, is refused."""

    @dataclass
    class Name:
        name: str

    with pytest.raises(ValueError, match="field 'name' of Name .* one empty field"):
        TsvWriter[Name](StringIO(), quoting=False).write(Name(""))


def test_writer_writes_the_delimiter_of_another_format_without_quoting(tmp_path: Path) -> None:
    """Test that without quoting only the writer's own delimiter is refused."""
    with CsvWriter.from_path[Feature](tmp_path / "test.csv", quoting=False) as writer:
        writer.write(Feature("a\tb", '"'))

    assert (tmp_path / "test.csv").read_text() == 'a\tb,"\n'


@dataclass
class Tagged:
    """A record whose first field may look like a comment."""

    tag: str
    value: int


def test_writer_quotes_a_row_that_would_read_as_a_comment() -> None:
    """Test that a row whose first field starts with a comment prefix is quoted, to read back."""
    handle = StringIO()
    writer = TsvWriter[Tagged](handle, comment_prefixes=("#", "track"))
    writer.write(Tagged("#1", 1))
    writer.write(Tagged("tracking", 2))
    writer.write(Tagged("plain", 3))

    assert handle.getvalue() == '"#1"\t"1"\n"tracking"\t"2"\nplain\t3\n'
    comment_prefixes = {"#", "track"}
    with TsvReader[Tagged](
        StringIO(handle.getvalue()), header=False, comment_prefixes=comment_prefixes
    ) as reader:
        assert list(reader) == [Tagged("#1", 1), Tagged("tracking", 2), Tagged("plain", 3)]


def test_writer_refuses_a_row_that_would_read_as_a_comment_without_quoting() -> None:
    """Test that without quoting a row whose first field starts with a comment prefix is refused."""
    writer = TsvWriter[Tagged](StringIO(), quoting=False)

    with pytest.raises(ValueError, match=r"field 'tag' of Tagged .* starts with a comment prefix"):
        writer.write(Tagged("#1", 1))


def test_writer_quotes_a_header_that_would_read_as_a_comment() -> None:
    """Test that a header whose first name starts with a comment prefix is quoted."""

    @dataclass
    class Region:
        _chrom: str

    handle = StringIO()
    TsvWriter[Region](handle, comment_prefixes=("_",)).write_header()

    assert handle.getvalue() == '"_chrom"\n'


def test_writer_header_refuses_unquotable_names_without_quoting() -> None:
    """Test that without quoting a header name holding the delimiter is refused."""

    class Letter(Enum):
        A = "A\tB"

    @dataclass
    class Counts:
        counts: CounterColumns[Letter]

    with pytest.raises(ValueError, match=r"Cannot write field 'A\tB' of Counts without quoting"):
        TsvWriter[Counts](StringIO(), quoting=False).write_header()


def test_writer_refuses_extra_columns_that_are_not_text() -> None:
    """Test that the values of an ExtraColumns field must be text."""
    writer = TsvWriter[Feature](StringIO())

    with pytest.raises(
        ValueError, match=r"^The ExtraColumns field 'extra' of Feature must hold text"
    ):
        writer.write(Feature("x", "y", cast(tuple[str, ...], (5,))))


@dataclass
class Measure:
    """A record of a float that may not be finite, and an optional one."""

    value: float
    maybe: float | None


def test_writer_writes_floats_that_are_not_finite_so_they_read_back() -> None:
    """Test that NaN and infinities are written as text that reads back as the same floats."""
    records = [Measure(float("inf"), float("-inf")), Measure(float("nan"), None)]
    handle = StringIO()
    writer = TsvWriter[Measure](handle)
    for record in records:
        writer.write(record)

    assert handle.getvalue() == "inf\t-inf\nnan\t\n"
    with TsvReader[Measure](StringIO(handle.getvalue()), header=False) as reader:
        first, second = list(reader)
    assert first == records[0]
    assert math.isnan(second.value)
    assert second.maybe is None
