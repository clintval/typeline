from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pytest
from typing_extensions import assert_type

from typeline import ExtraColumns
from typeline import TsvReader
from typeline import TsvWriter


@dataclass(frozen=True)
class Region:
    """A record with two named fields and any number of extra columns."""

    name: str
    start: int
    extra: ExtraColumns = ()


@dataclass(frozen=True)
class Misplaced:
    """A record whose extra columns field is not its last field."""

    extra: ExtraColumns
    name: str


def test_reader_collects_columns_past_the_named_fields(tmp_path: Path) -> None:
    """Test that the reader keeps any columns past the named fields as text."""
    path = tmp_path / "regions.tsv"
    _ = path.write_text("a\t1\n\nb\t2\tx\t.\t\nc\t3\t[1]\n")

    with TsvReader.from_path[Region](path, header=False, none_field=".") as reader:
        records = list(reader)

    assert records == [Region("a", 1), Region("b", 2, ("x", ".", "")), Region("c", 3, ("[1]",))]
    _ = assert_type(records[1].extra, tuple[str, ...])


def test_reader_refuses_a_line_without_the_named_fields(tmp_path: Path) -> None:
    """Test that a line with fewer columns than the named fields is refused."""
    path = tmp_path / "regions.tsv"
    _ = path.write_text("a\n")

    message = r"^Expected at least 2 columns but found 1 on line 1 for record type: Region\.$"
    with (
        TsvReader.from_path[Region](path, header=False) as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)


def test_reader_checks_only_the_named_fields_in_a_header(tmp_path: Path) -> None:
    """Test that a header may name the extra columns after the named fields."""
    path = tmp_path / "regions.tsv"
    _ = path.write_text("name\tstart\tscore\na\t1\t0.5\n")

    with TsvReader.from_path[Region](path) as reader:
        assert list(reader) == [Region("a", 1, ("0.5",))]


def test_writer_writes_the_extra_columns_after_the_named_fields(tmp_path: Path) -> None:
    """Test that the writer writes extra columns back, and a header of the named fields."""
    path = tmp_path / "regions.tsv"
    records = [Region("a", 1), Region("b", 2, ("x", "", "y z"))]

    with TsvWriter.from_path[Region](path) as writer:
        writer.write_header()
        for record in records:
            writer.write(record)

    assert path.read_text() == "name\tstart\na\t1\nb\t2\tx\t\ty z\n"

    with TsvReader.from_path[Region](path) as reader:
        assert list(reader) == records


def test_extra_columns_must_be_the_last_field() -> None:
    """Test that readers and writers refuse a record whose extra columns field is not last."""
    message = r"^The ExtraColumns field of Misplaced must be its last field, but 'extra' is not!$"
    with pytest.raises(TypeError, match=message):
        _ = TsvReader[Misplaced](StringIO(""), header=False)
    with pytest.raises(TypeError, match=message):
        _ = TsvWriter[Misplaced](StringIO())
