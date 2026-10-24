from dataclasses import dataclass
from dataclasses import field
from io import StringIO
from math import isnan
from pathlib import Path
from re import escape

import pytest
from typing_extensions import assert_type

from typeline import ExtraColumns
from typeline import NamedExtraColumns
from typeline import TsvReader
from typeline import TsvWriter
from typeline.codecs import nan


@dataclass
class Bin:
    """A record with one named field and any number of counts in columns named by a header."""

    size: int
    counts: NamedExtraColumns[int] = field(default_factory=dict)


@dataclass
class Ratios:
    """A record with any number of ratios, which may be missing, in columns named by a header."""

    name: str
    ratios: NamedExtraColumns[float | None] = field(default_factory=dict)


@dataclass
class Notes:
    """A record with any number of notes in columns named by a header."""

    name: str
    notes: NamedExtraColumns[str] = field(default_factory=dict)


@dataclass
class Misplaced:
    """A record whose named extra columns field is not its last field."""

    counts: NamedExtraColumns[int]
    size: int


@dataclass
class Twice:
    """A record with two fields that would each hold the columns no other field takes."""

    size: int
    extra: ExtraColumns
    counts: NamedExtraColumns[int]


def test_reader_keeps_the_other_columns_by_their_names_in_file_order(tmp_path: Path) -> None:
    """Test that the reader keeps columns no field takes, keyed by name, wherever they sit."""
    path = tmp_path / "bins.tsv"
    _ = path.write_text("b.rf\tsize\ta.fr\n12\t187\t4123\n0\t188\t4100\n")

    with TsvReader.from_path[Bin](path) as reader:
        records = list(reader)

    assert records == [Bin(187, {"b.rf": 12, "a.fr": 4123}), Bin(188, {"b.rf": 0, "a.fr": 4100})]
    assert list(records[0].counts) == ["b.rf", "a.fr"]
    _ = assert_type(records[0].counts, dict[str, int])


def test_reader_reads_each_column_as_the_value_type_with_its_codec() -> None:
    """Test that each named extra column is read as the value type, codecs and None included."""
    reader = TsvReader[Ratios](StringIO("name\tx\ty\tz\na\t0.5\t?\t\n"), codecs={float: nan("?")})

    (record,) = list(reader)

    assert record.ratios["x"] == 0.5
    assert isnan(record.ratios["y"] or 0.0)
    assert record.ratios["z"] is None


def test_reader_names_the_field_and_column_when_a_value_cannot_be_read(tmp_path: Path) -> None:
    """Test that a value that cannot be read is named by its field, type, column, and line."""
    path = tmp_path / "bins.tsv"
    _ = path.write_text("size\ta.fr\n187\tmany\n")

    message = (
        r"^Could not read field 'counts' of type int from text 'many' in column 'a\.fr'"
        + rf" on line 2 of '{escape(str(path))}'!"
    )
    with TsvReader.from_path[Bin](path) as reader, pytest.raises(ValueError, match=message):
        _ = list(reader)


def test_reader_without_a_header_refuses_named_extra_columns() -> None:
    """Test that a reader refuses a record with named extra columns when there is no header."""
    message = (
        r"^The NamedExtraColumns field 'counts' of Bin names its columns after a header,"
        + r" so it cannot be read with header=False!$"
    )
    with pytest.raises(ValueError, match=message):
        _ = TsvReader[Bin](StringIO("187\t12\n"), header=False)


def test_reader_refuses_a_row_longer_than_the_header() -> None:
    """Test that a row with a column the header does not name is refused."""
    reader = TsvReader[Bin](StringIO("size\ta.fr\n187\t12\t3\n"))

    with pytest.raises(ValueError, match=r"^Expected 2 columns but found 3 on line 2"):
        _ = list(reader)


def test_writer_names_the_columns_of_the_first_record_in_the_header(tmp_path: Path) -> None:
    """Test that the header names the first record's columns, and each record follows them."""
    path = tmp_path / "bins.tsv"
    records = [Bin(187, {"a.fr": 4123, "b.rf": 12}), Bin(188, {"b.rf": 0, "a.fr": 4100})]

    with TsvWriter.from_path[Bin](path, header=True) as writer:
        for record in records:
            writer.write(record)

    assert path.read_text() == "size\ta.fr\tb.rf\n187\t4123\t12\n188\t4100\t0\n"

    with TsvReader.from_path[Bin](path) as reader:
        assert list(reader) == records


def test_writer_writes_each_value_as_the_value_type_with_its_codec() -> None:
    """Test that each named extra column is written as the value type, codecs and None included."""
    handle = StringIO()
    writer = TsvWriter[Ratios](handle, codecs={float: nan("?")})

    writer.write(Ratios("a", {"x": 0.5, "y": float("nan"), "z": None}))

    assert handle.getvalue() == "a\t0.5\t?\t\n"


def test_writer_refuses_a_record_whose_columns_differ_from_the_first() -> None:
    """Test that every record must name the same columns as the first record written."""
    writer = TsvWriter[Bin](StringIO())
    writer.write(Bin(187, {"a.fr": 1, "b.rf": 2}))

    message = (
        r"^The NamedExtraColumns field 'counts' of Bin must name the columns"
        + r" \['a\.fr', 'b\.rf'\] of the first record written, but names \['a\.fr', 'c\.tandem'\]!$"
    )
    with pytest.raises(ValueError, match=message):
        writer.write(Bin(188, {"a.fr": 1, "c.tandem": 2}))


def test_writer_takes_the_columns_from_a_record_that_was_written() -> None:
    """Test that a record that could not be written does not choose the columns."""
    handle = StringIO()
    writer = TsvWriter[Bin](handle)

    with pytest.raises(ValueError, match=r"^Could not write field 'counts' of type int"):
        writer.write(Bin(187, {"a.fr": object()}))  # type: ignore[dict-item]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]

    writer.write(Bin(188, {"b.rf": 2}))

    assert handle.getvalue() == "188\t2\n"


def test_writer_refuses_to_write_the_header_before_the_first_record() -> None:
    """Test that the header cannot be written before a record names the extra columns."""
    writer = TsvWriter[Bin](StringIO())

    message = (
        r"^Cannot write the header of Bin before its first record, which names the columns"
        + r" of its NamedExtraColumns field 'counts'!$"
    )
    with pytest.raises(ValueError, match=message):
        writer.write_header()


def test_writer_writes_all_records_after_a_header_named_by_the_first() -> None:
    """Test that write_all names the extra columns in the header after the first record."""
    handle = StringIO()
    writer = TsvWriter[Bin](handle, header=True)

    writer.write_all([Bin(187, {"a.fr": 4123}), Bin(188, {"a.fr": 4100})])

    assert handle.getvalue() == "size\ta.fr\n187\t4123\n188\t4100\n"


@pytest.mark.parametrize("write_all", [True, False])
def test_writer_without_records_writes_a_header_of_the_other_fields(
    tmp_path: Path, write_all: bool
) -> None:
    """Test that a header written without records names only the fields of the record."""
    path = tmp_path / "bins.tsv"

    with TsvWriter.from_path[Bin](path, header=True) as writer:
        if write_all:
            writer.write_all([])

    assert path.read_text() == "size\n"


def test_writer_names_the_field_when_a_value_cannot_be_written_without_quoting() -> None:
    """Test that a named extra column refused without quoting is named by its field."""
    writer = TsvWriter[Notes](StringIO(), quoting=False)

    message = r"^Cannot write field 'notes' of Notes without quoting, because its text holds"
    with pytest.raises(ValueError, match=message):
        writer.write(Notes("a", {"x": "tab\there"}))


def test_named_extra_columns_must_be_the_last_field() -> None:
    """Test that readers and writers refuse a record whose named extra columns are not last."""
    message = (
        r"^The NamedExtraColumns field of Misplaced must be its last field, but 'counts' is not!$"
    )
    with pytest.raises(TypeError, match=message):
        _ = TsvReader[Misplaced](StringIO(""))
    with pytest.raises(TypeError, match=message):
        _ = TsvWriter[Misplaced](StringIO())


def test_a_record_holds_the_other_columns_in_one_field_only() -> None:
    """Test that readers and writers refuse a record with two fields for the other columns."""
    message = (
        r"^Twice holds the columns no other field takes in more than one field:"
        + r" \['extra', 'counts'\]!$"
    )
    with pytest.raises(TypeError, match=message):
        _ = TsvReader[Twice](StringIO(""))
    with pytest.raises(TypeError, match=message):
        _ = TsvWriter[Twice](StringIO())
