from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from enum import Enum
from enum import StrEnum
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from typeline import Codecs
from typeline import CounterColumns
from typeline import CsvReader
from typeline import CsvWriter
from typeline import ExtraColumns
from typeline import TsvReader
from typeline import TsvWriter
from typeline.codecs import delimited


class Color(Enum):
    """A color, written as its value."""

    RED = "red"
    BLUE = "blue"


@dataclass(frozen=True)
class Sample:
    """A record of many kinds of field."""

    name: str
    count: int
    ratio: float | None
    passed: bool
    seen: date
    color: Color
    lanes: tuple[int, ...]
    scores: list[float]
    meta: dict[str, int]


@dataclass(frozen=True)
class Point:
    """A record of two integers."""

    x: int
    y: int


SAMPLES: list[Sample] = [
    Sample("s1", 3, 0.25, True, date(2026, 10, 4), Color.RED, (1, 2), [0.5, 1.0], {"a": 1}),
    Sample("s,2", 0, None, False, date(2026, 1, 1), Color.BLUE, (), [], {}),
]


@pytest.mark.parametrize("codecs", [{}, {tuple[int, ...]: delimited(int, container=tuple)}])
def test_write_all_writes_what_write_writes_for_each_record(codecs: Codecs) -> None:
    """Test that write_all writes every record in order, just as write writes each one."""
    one_by_one = StringIO()
    writer = CsvWriter[Sample](one_by_one, codecs=codecs)
    for record in SAMPLES:
        writer.write(record)

    all_at_once = StringIO()
    CsvWriter[Sample](all_at_once, codecs=codecs).write_all(SAMPLES)

    assert all_at_once.getvalue() == one_by_one.getvalue()
    with CsvReader[Sample](StringIO(all_at_once.getvalue()), header=False, codecs=codecs) as reader:
        assert list(reader) == SAMPLES


def test_write_all_of_no_records_writes_nothing() -> None:
    """Test that write_all of an empty iterable writes nothing when the writer writes no header."""
    stream = StringIO()
    TsvWriter[Sample](stream).write_all([])

    assert stream.getvalue() == ""


def test_write_all_reads_a_generator_once_in_order() -> None:
    """Test that write_all takes any iterable, like a generator, and reads it once."""
    seen: list[int] = []

    def points() -> Iterator[Point]:
        for x in range(3):
            seen.append(x)
            yield Point(x, x * x)

    stream = StringIO()
    TsvWriter[Point](stream).write_all(points())

    assert stream.getvalue() == "0\t0\n1\t1\n2\t4\n"
    assert seen == [0, 1, 2]


def test_write_all_appends_to_what_was_written() -> None:
    """Test that write_all may be mixed with write, each adding records after the last."""
    stream = StringIO()
    writer = TsvWriter[Point](stream)
    writer.write(Point(0, 0))
    writer.write_all([Point(1, 1), Point(2, 4)])
    writer.write_all(Point(x, -x) for x in (3, 4))

    assert stream.getvalue() == "0\t0\n1\t1\n2\t4\n3\t-3\n4\t-4\n"


def test_write_all_stops_at_a_record_it_cannot_write() -> None:
    """Test that write_all raises at the first refused record, after writing the ones before it."""
    stream = StringIO()
    records: list[Any] = [Point(0, 0), Coverage("s1", 41.2), Point(1, 1)]

    with pytest.raises(ValueError, match=r"^Expected Point but found Coverage!$"):
        TsvWriter[Point](stream).write_all(records)

    assert stream.getvalue() == "0\t0\n"


@dataclass(frozen=True)
class Coverage:
    """A record whose columns are renamed."""

    sample: str
    gc: float


def test_write_all_with_renamed_columns(tmp_path: Path) -> None:
    """Test that write_all with a header writes the renamed columns, which read back."""
    path = tmp_path / "coverage.tsv"
    rename = {"gc": "%GC"}
    records = [Coverage("s1", 41.2), Coverage("s2", 38.0)]

    with TsvWriter.from_path[Coverage](path, header=True, rename=rename) as writer:
        writer.write_all(records)

    assert path.read_text() == "sample\t%GC\ns1\t41.2\ns2\t38.0\n"
    with TsvReader.from_path[Coverage](path, rename=rename) as reader:
        assert list(reader) == records


@dataclass(frozen=True)
class Region:
    """A record with two named fields and any number of extra columns."""

    name: str
    start: int
    extra: ExtraColumns = ()


def test_write_all_with_extra_columns(tmp_path: Path) -> None:
    """Test that write_all writes each record's extra columns after a header of its named fields."""
    path = tmp_path / "regions.tsv"
    records = [Region("a", 1), Region("b", 2, ("x", "", "y z"))]

    with TsvWriter.from_path[Region](path, header=True) as writer:
        writer.write_all(records)

    assert path.read_text() == "name\tstart\na\t1\nb\t2\tx\t\ty z\n"
    with TsvReader.from_path[Region](path) as reader:
        assert list(reader) == records


class Base(StrEnum):
    """The four DNA bases."""

    A = "A"
    C = "C"
    G = "G"
    T = "T"


@dataclass(frozen=True)
class Pileup:
    """A record with fields on both sides of its base counts."""

    contig: str
    counts: CounterColumns[Base]
    depth: int | None


def test_write_all_with_counter_columns(tmp_path: Path) -> None:
    """Test that write_all writes each member's count where the field sits, under its header."""
    path = tmp_path / "pileup.tsv"
    records = [
        Pileup("chr1", Counter({Base.A: 12, Base.G: 3}), 15),
        Pileup("chr2", Counter(), None),
    ]

    with TsvWriter.from_path[Pileup](path, header=True) as writer:
        writer.write_all(records)

    assert path.read_text() == (
        "contig\tA\tC\tG\tT\tdepth\nchr1\t12\t0\t3\t0\t15\nchr2\t0\t0\t0\t0\t\n"
    )
    with TsvReader.from_path[Pileup](path) as reader:
        assert [record.counts.total() for record in reader] == [15, 0]
