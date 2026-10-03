from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from io import StringIO
from pathlib import Path
from typing import Any
from typing import TextIO

import pytest
from typing_extensions import Unpack
from typing_extensions import override

from typeline import CounterColumns
from typeline import CsvReader
from typeline import CsvWriter
from typeline import ExtraColumns
from typeline import FieldCodec
from typeline import FixedRecordType
from typeline import ReaderOptions
from typeline import TsvReader
from typeline import TsvWriter
from typeline import WriterOptions


@dataclass(frozen=True)
class Bin:
    """A record whose columns are named with text that is not a Python identifier."""

    contig: str
    gc: float
    mean_depth: float | None
    bin_1_4: int
    p_value: float


COLUMNS: dict[str, str] = {
    "gc": "%GC",
    "mean_depth": "mean depth",
    "bin_1_4": "1-4",
    "p_value": "p-value",
}

RECORDS: list[Bin] = [Bin("chr1", 41.5, 30.25, 7, 0.01), Bin("chr2", 38.0, None, 0, 1.0)]

TEXT: str = (
    "contig\t%GC\tmean depth\t1-4\tp-value\nchr1\t41.5\t30.25\t7\t0.01\nchr2\t38.0\t\t0\t1.0\n"
)


@dataclass(frozen=True)
class Depth:
    """A record with a column named after a threshold chosen at runtime."""

    sample: str
    frac_below_min: float


@dataclass(frozen=True)
class Interval:
    """A record whose first column may be named like a comment."""

    chrom: str
    start: int
    end: int


@dataclass(frozen=True)
class Pair:
    """A record of two fields of the same type."""

    first: str
    second: str


@dataclass(frozen=True)
class Scored:
    """A record with a named field and any number of extra columns."""

    name: str
    score: float
    extra: ExtraColumns = ()


class Base(StrEnum):
    """The four DNA bases."""

    A = "A"
    C = "C"
    G = "G"
    T = "T"


@dataclass(frozen=True)
class Pileup:
    """A record with fields on both sides of its base counts."""

    position: int
    counts: CounterColumns[Base]
    depth: int


def test_writer_writes_the_named_columns_in_the_header() -> None:
    """Test that the header names each field's column, and the records are written as before."""
    handle = StringIO()
    writer = TsvWriter[Bin](handle, columns=COLUMNS)
    writer.write_header()
    for record in RECORDS:
        writer.write(record)

    assert handle.getvalue() == TEXT


@pytest.mark.parametrize(
    "writer_type,reader_type",
    [pytest.param(TsvWriter, TsvReader, id="tsv"), pytest.param(CsvWriter, CsvReader, id="csv")],
)
def test_round_trip_with_named_columns(tmp_path: Path, writer_type: Any, reader_type: Any) -> None:
    """Test that records written with named columns read back the same with the same names."""
    path = tmp_path / "bins.txt"

    with writer_type.from_path[Bin](path, columns=COLUMNS) as writer:
        writer.write_header()
        for record in RECORDS:
            writer.write(record)

    with reader_type.from_path[Bin](path, columns=COLUMNS) as reader:
        assert list(reader) == RECORDS


def test_reader_matches_named_columns_in_any_order() -> None:
    """Test that the named columns of a header may come in any order."""
    text = "p-value\t1-4\tcontig\tmean depth\t%GC\n0.01\t7\tchr1\t30.25\t41.5\n"

    with TsvReader[Bin](StringIO(text), columns=COLUMNS) as reader:
        assert list(reader) == [RECORDS[0]]


def test_a_name_may_hold_the_delimiter_and_quotes() -> None:
    """Test that a name is written quoted when it must be, and the header reads back."""
    columns = {"gc": "GC, %", "p_value": 'p "two-sided"'}
    handle = StringIO()
    writer = CsvWriter[Bin](handle, columns=columns)
    writer.write_header()
    writer.write(RECORDS[0])

    assert handle.getvalue().splitlines()[0] == (
        'contig,"GC, %",mean_depth,bin_1_4,"p ""two-sided"""'
    )
    with CsvReader[Bin](StringIO(handle.getvalue()), columns=columns) as reader:
        assert list(reader) == [RECORDS[0]]


@pytest.mark.parametrize("min_depth", [10, 20])
def test_columns_named_at_runtime(tmp_path: Path, min_depth: int) -> None:
    """Test that a column may be named after a value known only at runtime."""
    path = tmp_path / "depth.tsv"
    columns = {"frac_below_min": f"frac_below_{min_depth}x"}

    with TsvWriter.from_path[Depth](path, columns=columns) as writer:
        writer.write_header()
        writer.write(Depth("s1", 0.25))

    assert path.read_text() == f"sample\tfrac_below_{min_depth}x\ns1\t0.25\n"

    with TsvReader.from_path[Depth](path, columns=columns) as reader:
        assert list(reader) == [Depth("s1", 0.25)]


def test_reader_refuses_a_header_named_for_another_runtime_value() -> None:
    """Test that a header naming a column after another value is refused, naming both names."""
    text = "sample\tfrac_below_10x\ns1\t0.25\n"

    message = (
        r"^Columns of header do not match fields of Depth on line 1!"
        + r" Header: \['sample', 'frac_below_10x'\]\."
        + r" Columns of Depth: \['sample', 'frac_below_20x'\]\."
        + r" Missing from header: \['frac_below_20x'\]\."
        + r" Unexpected in header: \['frac_below_10x'\]\.$"
    )
    with pytest.raises(ValueError, match=message):
        _ = TsvReader[Depth](StringIO(text), columns={"frac_below_min": "frac_below_20x"})


def test_reader_refuses_a_field_by_its_own_name_once_its_column_is_named() -> None:
    """Test that a field has exactly one column, so its own name no longer matches it."""
    text = "contig\tgc\tmean depth\t1-4\tp-value\n"

    message = r"Missing from header: \['%GC'\]\. Unexpected in header: \['gc'\]\.$"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader[Bin](StringIO(text), columns=COLUMNS)


def test_names_have_no_effect_without_a_header() -> None:
    """Test that without a header columns are read and written in field order, names aside."""
    line = "chr1\t41.5\t30.25\t7\t0.01\n"

    with TsvReader[Bin](StringIO(line), header=False, columns=COLUMNS) as reader:
        assert list(reader) == [RECORDS[0]]

    assert TsvReader[Bin](StringIO(), columns=COLUMNS).decode(line) == RECORDS[0]

    handle = StringIO()
    TsvWriter[Bin](handle, columns=COLUMNS).write(RECORDS[0])
    assert handle.getvalue() == line


def test_fields_may_swap_names() -> None:
    """Test that a field may take another field's name, as long as no two columns share one."""
    columns = {"first": "second", "second": "first"}
    handle = StringIO()
    writer = TsvWriter[Pair](handle, columns=columns)
    writer.write_header()
    writer.write(Pair("a", "b"))

    assert handle.getvalue() == "second\tfirst\na\tb\n"

    with TsvReader[Pair](StringIO("first\tsecond\nb\ta\n"), columns=columns) as reader:
        assert list(reader) == [Pair("a", "b")]


def test_a_column_named_like_a_comment_is_quoted_at_the_start_of_a_header() -> None:
    """Test that a header starting with a name like a comment is quoted, so it reads back."""
    columns = {"chrom": "#chrom"}
    handle = StringIO()
    writer = TsvWriter[Interval](handle, columns=columns)
    writer.write_header()
    writer.write(Interval("chr1", 10, 20))

    assert handle.getvalue() == '"#chrom"\t"start"\t"end"\nchr1\t10\t20\n'

    with TsvReader[Interval](StringIO(handle.getvalue()), columns=columns) as reader:
        assert list(reader) == [Interval("chr1", 10, 20)]


def test_a_column_named_like_a_comment_with_other_comment_prefixes() -> None:
    """Test that comment prefixes the name does not start with write and read it unquoted."""
    columns = {"chrom": "#chrom"}
    handle = StringIO()
    writer = TsvWriter[Interval](handle, columns=columns, comment_prefixes=["##"])
    writer.write_comment("source=tool")
    writer.write_header()
    writer.write(Interval("chr1", 10, 20))

    assert handle.getvalue() == "## source=tool\n#chrom\tstart\tend\nchr1\t10\t20\n"

    with TsvReader[Interval](
        StringIO(handle.getvalue()), columns=columns, comment_prefixes=["##"]
    ) as reader:
        assert list(reader) == [Interval("chr1", 10, 20)]


def test_extra_columns_keep_the_columns_no_field_takes(tmp_path: Path) -> None:
    """Test that unnamed columns, a field's own name among them, are kept as extra columns."""
    path = tmp_path / "scored.tsv"
    columns = {"score": "Score"}
    _ = path.write_text("score\tname\tScore\tnote\n0.1\tx\t0.9\thi\n")

    with TsvReader.from_path[Scored](path, columns=columns) as reader:
        records = list(reader)

    assert records == [Scored("x", 0.9, ("0.1", "hi"))]

    with TsvWriter.from_path[Scored](path, columns=columns) as writer:
        writer.write_header()
        writer.write(records[0])

    assert path.read_text() == "name\tScore\nx\t0.9\t0.1\thi\n"


def test_named_columns_beside_counter_columns(tmp_path: Path) -> None:
    """Test that fields beside a Counter are written and found under their named columns."""
    path = tmp_path / "pileup.tsv"
    columns = {"position": "pos", "depth": "DP"}
    record = Pileup(100, Counter({Base.A: 12, Base.G: 3}), 15)

    with TsvWriter.from_path[Pileup](path, columns=columns) as writer:
        writer.write_header()
        writer.write(record)

    assert path.read_text() == "pos\tA\tC\tG\tT\tDP\n100\t12\t0\t3\t0\t15\n"

    _ = path.write_text("DP\tT\tpos\tG\tA\tC\n15\t0\t100\t3\t12\t0\n")
    with TsvReader.from_path[Pileup](path, columns=columns) as reader:
        assert list(reader) == [record]


class BinReader(TsvReader[Bin], FixedRecordType):
    """A reader of a format whose column names are fixed."""

    @override
    def __init__(self, handle: TextIO, /, **options: Unpack[ReaderOptions]) -> None:
        _ = options.setdefault("columns", COLUMNS)
        super().__init__(handle, **options)


class BinWriter(TsvWriter[Bin], FixedRecordType):
    """A writer of a format whose column names are fixed."""

    @override
    def __init__(self, handle: TextIO, /, **options: Unpack[WriterOptions]) -> None:
        _ = options.setdefault("columns", COLUMNS)
        super().__init__(handle, **options)


def test_a_format_may_fix_its_names_as_defaults(tmp_path: Path) -> None:
    """Test that a reader and writer of one format may name its columns once, as defaults."""
    path = tmp_path / "bins.tsv"

    with BinWriter.from_path(path) as writer:
        writer.write_header()
        for record in RECORDS:
            writer.write(record)

    assert path.read_text() == TEXT

    with BinReader.from_path(path) as reader:
        assert list(reader) == RECORDS


BUILDERS: list[Any] = [
    pytest.param(TsvReader, {}, id="reader"),
    pytest.param(TsvReader, {"header": False}, id="reader-without-header"),
    pytest.param(TsvWriter, {}, id="writer"),
]
"""Reader and writer classes and their options, typed loosely to bind records and pass options."""


@pytest.mark.parametrize("kind,options", BUILDERS)
@pytest.mark.parametrize(
    "record_type,columns,message",
    [
        pytest.param(
            Bin,
            {"gc_content": "%GC"},
            r"^Cannot name a column for 'gc_content', which is not a field of Bin!"
            + r" Fields of Bin: \['contig', 'gc', 'mean_depth', 'bin_1_4', 'p_value'\]\.$",
            id="not-a-field",
        ),
        pytest.param(
            Bin,
            {"%GC": "gc"},
            r"^Cannot name a column for '%GC', which is not a field of Bin!",
            id="column-to-field",
        ),
        pytest.param(
            Scored,
            {"extra": "rest"},
            r"^Cannot name a column for the ExtraColumns field 'extra' of Scored,"
            + r" which holds the columns no other field takes!$",
            id="extra-columns",
        ),
        pytest.param(
            Pileup,
            {"counts": "N"},
            r"^Cannot name a column for the CounterColumns field 'counts' of Pileup,"
            + r" whose columns are named after the values of its members!$",
            id="counter-columns",
        ),
        pytest.param(
            Bin,
            {"gc": "value", "p_value": "value"},
            r"^The fields 'gc' and 'p_value' of Bin both have a column 'value'!$",
            id="two-fields-one-name",
        ),
        pytest.param(
            Bin,
            {"gc": "contig"},
            r"^The fields 'contig' and 'gc' of Bin both have a column 'contig'!$",
            id="another-field-name",
        ),
        pytest.param(
            Pileup,
            {"depth": "A"},
            r"^The field 'depth' and the CounterColumns field 'counts' of Pileup"
            + r" both have a column 'A'!$",
            id="member-column",
        ),
    ],
)
def test_readers_and_writers_refuse_names_that_do_not_name_one_column_each(
    kind: Any, options: dict[str, Any], record_type: Any, columns: dict[str, str], message: str
) -> None:
    """Test that names are checked when a reader or writer is built, with or without a header."""
    with pytest.raises(ValueError, match=message):
        _ = kind[record_type](StringIO(), columns=columns, **options)


@pytest.mark.parametrize("kind,options", BUILDERS)
def test_readers_and_writers_refuse_a_name_that_is_not_a_string(
    kind: Any, options: dict[str, Any]
) -> None:
    """Test that a column must be named by a string."""
    message = r"^The column of field 'bin_1_4' of Bin must be named by a string, not 14!$"
    with pytest.raises(TypeError, match=message):
        _ = kind[Bin](StringIO(), columns={"bin_1_4": 14}, **options)


def test_writer_refuses_names_before_opening_the_file(tmp_path: Path) -> None:
    """Test that a writer refused for its names leaves its file alone."""
    path = tmp_path / "bins.tsv"

    with pytest.raises(ValueError, match=r"^Cannot name a column for 'gc_content'"):
        _ = TsvWriter.from_path[Bin](path, columns={"gc_content": "%GC"})

    assert not path.exists()


def test_reader_error_names_the_field_and_its_column() -> None:
    """Test that a field that cannot be read is named along with its column."""
    codecs = {int: FieldCodec(from_text=int, into_text=str)}
    text = "contig\t%GC\tmean depth\t1-4\tp-value\nchr1\t41.5\t30.25\tseven\t0.01\n"

    message = r"^Could not read field 'bin_1_4' of type int from text 'seven' in column '1-4'"
    with (
        TsvReader[Bin](StringIO(text), columns=COLUMNS, codecs=codecs) as reader,
        pytest.raises(ValueError, match=rf"{message} on line 2!$"),
    ):
        _ = list(reader)


def test_writer_error_names_the_field_and_its_column() -> None:
    """Test that a field that cannot be written is named along with its column."""

    def refuse(value: int) -> str:
        raise ValueError(f"Cannot write {value}")

    codecs = {int: FieldCodec(from_text=int, into_text=refuse)}
    writer = TsvWriter[Bin](StringIO(), columns=COLUMNS, codecs=codecs)

    message = r"^Could not write field 'bin_1_4' of type int in column '1-4'!$"
    with pytest.raises(ValueError, match=message):
        writer.write(RECORDS[0])


def test_writer_without_quoting_names_the_field_and_its_column() -> None:
    """Test that text a writer cannot write without quoting names the field and its column."""
    writer = TsvWriter[Interval](StringIO(), columns={"chrom": "#chrom"}, quoting=False)

    message = r"^Cannot write field 'chrom' in column '#chrom' of Interval without quoting"
    with pytest.raises(ValueError, match=rf"{message}, because its text starts with a comment"):
        writer.write_header()
    with pytest.raises(ValueError, match=rf"{message}, because its text holds the delimiter"):
        writer.write(Interval("chr\t1", 10, 20))


def test_columns_must_map_names_to_names_for_type_checkers() -> None:
    """Test that type checkers refuse columns that are not named by strings."""
    with pytest.raises(TypeError, match=r"^The column of field 'gc' of Bin must be named"):
        _ = TsvWriter[Bin](StringIO(), columns={"gc": 1})  # type: ignore[dict-item]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]
