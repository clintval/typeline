from collections import Counter
from dataclasses import dataclass
from dataclasses import make_dataclass
from enum import Enum
from enum import StrEnum
from io import StringIO
from pathlib import Path
from re import escape
from typing import Any
from typing import cast

import pytest
from typing_extensions import assert_type

from typeline import Comment
from typeline import CounterColumns
from typeline import CsvReader
from typeline import CsvWriter
from typeline import ExtraColumns
from typeline import FieldCodec
from typeline import TsvReader
from typeline import TsvWriter

READERS_AND_WRITERS: list[Any] = [TsvReader, TsvWriter]
"""Reader and writer classes, typed loosely so they can be bound to records built at runtime."""


class Base(StrEnum):
    """The four DNA bases."""

    A = "A"
    C = "C"
    G = "G"
    T = "T"


@dataclass(frozen=True)
class Pileup:
    """A record with a name and a count of each base."""

    name: str
    counts: CounterColumns[Base]


@dataclass(frozen=True)
class Position:
    """A record with fields on both sides of its base counts."""

    contig: str
    counts: CounterColumns[Base]
    depth: int | None


@dataclass(frozen=True)
class Scored:
    """A record with base counts followed by any extra columns."""

    name: str
    counts: CounterColumns[Base]
    extra: ExtraColumns = ()


def test_reader_gathers_member_columns_into_a_counter(tmp_path: Path) -> None:
    """Test that each member's column is read into that member's count."""
    path = tmp_path / "pileup.tsv"
    _ = path.write_text("name\tA\tC\tG\tT\nsite1\t1\t2\t0\t4\n")

    with TsvReader.from_path[Pileup](path) as reader:
        records = list(reader)

    assert records == [Pileup("site1", Counter({Base.A: 1, Base.C: 2, Base.G: 0, Base.T: 4}))]
    _ = assert_type(records[0].counts, Counter[Base])
    assert list(records[0].counts) == [Base.A, Base.C, Base.G, Base.T]
    assert all(type(member) is Base for member in records[0].counts)


def test_reader_refuses_a_header_missing_member_columns(tmp_path: Path) -> None:
    """Test that every member needs a column, so a misnamed column is never read as a zero count."""
    path = tmp_path / "pileup.tsv"
    _ = path.write_text("name\tT\tA\nsite1\t4\t1\n")

    message = (
        r"^Fields of header do not match fields of dataclass!"
        + r" Header: \['name', 'T', 'A'\]\."
        + r" Fields of Pileup: \['name', 'A', 'C', 'G', 'T'\]\."
        + r" Missing from header: \['C', 'G'\]\.$"
    )
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[Pileup](path)


def test_reader_refuses_a_header_without_member_columns(tmp_path: Path) -> None:
    """Test that a header without any member columns is refused."""
    path = tmp_path / "positions.tsv"
    _ = path.write_text("contig\tdepth\nchr1\t7\n")

    message = r"Missing from header: \['A', 'C', 'G', 'T'\]\.$"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[Position](path)


def test_reader_refuses_a_misnamed_member_column_even_with_extra_columns(tmp_path: Path) -> None:
    """Test that a misnamed member column is refused rather than kept as extra text."""
    path = tmp_path / "annotated.tsv"
    _ = path.write_text("name\ta\tC\tG\tT\n")

    with pytest.raises(ValueError, match=r"Missing from header: \['A'\]\. Unexpected"):
        _ = TsvReader.from_path[Scored](path)


def test_reader_reads_fields_after_the_member_columns(tmp_path: Path) -> None:
    """Test that fields after a Counter are read from the columns after its member columns."""
    path = tmp_path / "positions.csv"
    _ = path.write_text("# made by a tool\ncontig,A,C,G,T,depth\nchr1,1,2,3,4,10\nchr2,0,0,0,0,\n")
    comments: list[Comment] = []

    with CsvReader.from_path[Position](
        path, comment_prefixes={"#"}, on_comment=comments.append
    ) as reader:
        records = list(reader)

    assert comments == [Comment(1, "# made by a tool")]

    assert records == [
        Position("chr1", Counter({Base.A: 1, Base.C: 2, Base.G: 3, Base.T: 4}), 10),
        Position("chr2", Counter(), None),
    ]


def test_reader_reads_member_columns_in_enum_order_without_a_header() -> None:
    """Test that without a header the member columns are read as the writer writes them."""
    with TsvReader[Position](StringIO("chr1\t1\t2\t3\t4\t10\n"), header=False) as reader:
        records = list(reader)
    with TsvReader[Scored](StringIO("site1\t1\t2\t3\t4\t0.5\n"), header=False) as reader:
        scored = list(reader)

    counts = Counter({Base.A: 1, Base.C: 2, Base.G: 3, Base.T: 4})
    assert records == [Position("chr1", counts, 10)]
    assert scored == [Scored("site1", counts, ("0.5",))]


def test_round_trip_without_a_header() -> None:
    """Test that records with a Counter written without a header are read back again."""
    handle = StringIO()
    record = Position("chr1", Counter({Base.G: 7}), None)
    TsvWriter[Position](handle).write(record)

    with TsvReader[Position](StringIO(handle.getvalue()), header=False) as reader:
        assert list(reader) == [record]


@pytest.mark.parametrize("text", ["", "x", "1.5", "true", "-1", "+1", "1_000", " 1", "\u0661"])
def test_reader_refuses_a_count_that_is_not_an_integer(tmp_path: Path, text: str) -> None:
    """Test that a member column must hold an integer count."""
    path = tmp_path / "pileup.tsv"
    _ = path.write_text(f"name\tA\tC\tG\tT\nsite1\t1\t2\t3\t4\nsite2\t3\t{text}\t0\t0\n")

    message = (
        r"^Could not read column 'C' of field 'counts' as a count"
        + rf" from '{escape(text)}' on line 3!$"
    )
    with (
        TsvReader.from_path[Pileup](path) as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)


def test_reader_finds_member_columns_anywhere_in_the_header(tmp_path: Path) -> None:
    """Test that member columns are found by name wherever they sit among the other columns."""
    path = tmp_path / "positions.tsv"
    _ = path.write_text("G\tdepth\tA\tT\tcontig\tC\n3\t10\t1\t0\tchr1\t2\n")

    with TsvReader.from_path[Position](path) as reader:
        records = list(reader)

    assert records == [Position("chr1", Counter({Base.A: 1, Base.C: 2, Base.G: 3}), 10)]


def test_reader_refuses_a_repeated_member_column(tmp_path: Path) -> None:
    """Test that a member's column may appear only once."""
    path = tmp_path / "pileup.tsv"
    _ = path.write_text("name\tA\tC\tA\n")

    message = r"^Fields of header repeat a name! .* Repeated in header: \['A'\].$"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[Pileup](path)


def test_reader_refuses_a_header_missing_a_field_beside_the_member_columns(tmp_path: Path) -> None:
    """Test that the fields beside a Counter are still checked against the header."""
    path = tmp_path / "positions.tsv"
    _ = path.write_text("depth\tA\n")

    message = r"^Fields of header do not match fields of dataclass!"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[Position](path)


def test_writer_writes_one_column_per_member_in_enum_order() -> None:
    """Test that a Counter is written as one column per member, in the order of the enum."""
    handle = StringIO()
    writer = TsvWriter[Position](handle)
    writer.write_header()
    writer.write(Position("chr1", Counter({Base.T: 4, Base.A: 1}), 5))
    writer.write(Position("chr2", Counter(), None))

    assert handle.getvalue() == (
        "contig\tA\tC\tG\tT\tdepth\nchr1\t1\t0\t0\t4\t5\nchr2\t0\t0\t0\t0\t\n"
    )


def test_writer_refuses_a_key_that_is_not_a_member() -> None:
    """Test that the writer refuses a Counter holding a key its enum has no column for."""
    writer = TsvWriter[Pileup](StringIO())
    counts = cast(Counter[Base], Counter({Base.A: 1, "N": 2}))

    message = r"^Could not write field 'counts', which counts 'N' that is not a member of Base!$"
    with pytest.raises(ValueError, match=message):
        writer.write(Pileup("site1", counts))


@pytest.mark.parametrize("count", [1.5, -2, True, "3"])
def test_writer_refuses_a_count_that_is_not_a_non_negative_integer(count: object) -> None:
    """Test that the writer refuses a count the reader could not read back."""
    writer = TsvWriter[Pileup](StringIO())
    counts: Counter[Base] = Counter()
    counts[Base.C] = cast(int, count)

    message = (
        rf"^Could not write field 'counts', which counts {count!r} of 'C'"
        + r"; counts must be non-negative integers!$"
    )
    with pytest.raises(ValueError, match=message):
        writer.write(Pileup("site1", counts))


def test_writer_accepts_member_values_as_keys() -> None:
    """Test that a Counter keyed by a member's text is written as that member's count."""
    handle = StringIO()
    counts = cast(Counter[Base], Counter({"G": 3}))

    TsvWriter[Pileup](handle).write(Pileup("site1", counts))

    assert handle.getvalue() == "site1\t0\t0\t3\t0\n"


def test_round_trip(tmp_path: Path) -> None:
    """Test that records with a Counter are written and read back again."""
    path = tmp_path / "positions.csv"
    records = [
        Position("chr1", Counter({Base.A: 1, Base.C: 2, Base.G: 3, Base.T: 4}), 10),
        Position("chr2", Counter({Base.G: 7}), None),
    ]

    with CsvWriter.from_path[Position](path) as writer:
        writer.write_header()
        for record in records:
            writer.write(record)

    with CsvReader.from_path[Position](path) as reader:
        assert list(reader) == records


def test_extra_columns_are_the_columns_that_are_neither_fields_nor_members(tmp_path: Path) -> None:
    """Test that the columns that are neither fields nor member columns are extra columns."""
    path = tmp_path / "annotated.tsv"
    _ = path.write_text("name\tC\tA\tG\tT\tscore\tflag\nsite1\t2\t1\t0\t0\t0.5\tPASS\n")

    with TsvReader.from_path[Scored](path) as reader:
        records = list(reader)

    assert records == [Scored("site1", Counter({Base.A: 1, Base.C: 2}), ("0.5", "PASS"))]


def test_extra_columns_never_hold_a_member_column(tmp_path: Path) -> None:
    """Test that a member column past an extra column is counted, not kept as extra text."""
    path = tmp_path / "annotated.tsv"
    _ = path.write_text("name\tA\tscore\tC\tG\tT\nsite1\t1\t0.5\t2\t0\t0\n")

    with TsvReader.from_path[Scored](path) as reader:
        records = list(reader)

    assert records == [Scored("site1", Counter({Base.A: 1, Base.C: 2}), ("0.5",))]


def test_extra_columns_are_written_after_the_member_columns() -> None:
    """Test that the writer writes extra columns after the member columns."""
    handle = StringIO()
    writer = TsvWriter[Scored](handle)
    writer.write_header()
    writer.write(Scored("site1", Counter({Base.C: 2}), ("0.5",)))

    assert handle.getvalue() == "name\tA\tC\tG\tT\nsite1\t0\t2\t0\t0\t0.5\n"


def test_other_fields_keep_their_codecs_hooks_and_none_field(tmp_path: Path) -> None:
    """Test that codecs and none_field apply to other fields but not to the member columns."""
    path = tmp_path / "positions.tsv"
    codecs = {int: FieldCodec(from_text=lambda text: int(text) * 10, into_text=str)}
    _ = path.write_text("contig\tA\tC\tG\tT\tdepth\nchr1\t1\t2\t3\t4\t5\nchr2\t0\t0\t0\t0\t.\n")

    with TsvReader.from_path[Position](path, codecs=codecs, none_field=".") as reader:
        records = list(reader)

    assert records == [
        Position("chr1", Counter({Base.A: 1, Base.C: 2, Base.G: 3, Base.T: 4}), 50),
        Position("chr2", Counter(), None),
    ]


@pytest.mark.parametrize(
    "counted", [int, str, Enum("Numbered", {"A": 1}), Enum("Mixed", {"A": "A", "B": 2})]
)
def test_readers_and_writers_refuse_a_counter_of_anything_but_a_text_enum(counted: Any) -> None:
    """Test that a CounterColumns field must count the members of an Enum whose values are text."""
    record_type = make_dataclass("Bad", [("counts", cast(Any, CounterColumns)[counted])])

    message = (
        r"^The CounterColumns field 'counts' of Bad must count members of an Enum"
        + r" whose values are text!$"
    )
    for kind in READERS_AND_WRITERS:
        with pytest.raises(TypeError, match=message):
            _ = kind[record_type](StringIO("A\n"))


def test_readers_and_writers_refuse_two_counter_fields_sharing_a_column() -> None:
    """Test that two CounterColumns fields may not both have a column of the same name."""
    record_type = make_dataclass(
        "Bad", [("first", CounterColumns[Base]), ("second", CounterColumns[Base])]
    )

    message = r"^The CounterColumns fields 'first' and 'second' of Bad both have a column 'A'!$"
    for kind in READERS_AND_WRITERS:
        with pytest.raises(TypeError, match=message):
            _ = kind[record_type](StringIO("A\n"))


class Quality(Enum):
    """Base qualities in bins, a plain Enum whose values are text."""

    LOW = "low"
    HIGH = "high"


class Strand(str, Enum):
    """Strands, a text Enum mixin."""

    PLUS = "plus"
    MINUS = "minus"


@dataclass(frozen=True)
class Tally:
    """A record with three Counter fields over different kinds of text enums."""

    name: str
    bases: CounterColumns[Base]
    qualities: CounterColumns[Quality]
    strands: CounterColumns[Strand]


def test_records_may_have_several_counter_fields_over_any_text_enum(tmp_path: Path) -> None:
    """Test that several Counter fields over plain, mixin and StrEnum enums round-trip."""
    path = tmp_path / "tally.tsv"
    record = Tally(
        "site1",
        Counter({Base.A: 1, Base.T: 2}),
        Counter({Quality.HIGH: 3}),
        Counter({Strand.PLUS: 1, Strand.MINUS: 2}),
    )

    with TsvWriter.from_path[Tally](path) as writer:
        writer.write_header()
        writer.write(record)

    assert path.read_text() == (
        "name\tA\tC\tG\tT\tlow\thigh\tplus\tminus\nsite1\t1\t0\t0\t2\t0\t3\t1\t2\n"
    )
    with TsvReader.from_path[Tally](path) as reader:
        assert list(reader) == [record]
    with TsvReader.from_path[Tally](path) as reader:
        (read,) = list(reader)
        assert all(type(member) is Quality for member in read.qualities)


def test_several_counter_fields_are_found_by_name_in_any_order(tmp_path: Path) -> None:
    """Test that the columns of several Counter fields may be mixed in any order in a header."""
    path = tmp_path / "tally.tsv"
    _ = path.write_text("high\tT\tminus\tname\tA\tlow\tC\tplus\tG\n3\t2\t2\tsite1\t1\t0\t0\t1\t0\n")

    with TsvReader.from_path[Tally](path) as reader:
        assert list(reader) == [
            Tally(
                "site1",
                Counter({Base.A: 1, Base.T: 2}),
                Counter({Quality.HIGH: 3}),
                Counter({Strand.PLUS: 1, Strand.MINUS: 2}),
            )
        ]


def test_several_counter_fields_are_read_without_a_header() -> None:
    """Test that without a header each Counter field's columns are read at its place."""
    with TsvReader[Tally](StringIO("site1\t1\t0\t0\t2\t0\t3\t1\t2\n"), header=False) as reader:
        (record,) = list(reader)

    assert record.qualities == Counter({Quality.HIGH: 3})
    assert record.strands == Counter({Strand.PLUS: 1, Strand.MINUS: 2})


def test_writer_accepts_the_text_of_a_plain_enum_member_as_a_key() -> None:
    """Test that a plain Enum member's text counts as that member when writing."""
    handle = StringIO()
    record = Tally("site1", Counter(), cast(Counter[Quality], Counter({"high": 4})), Counter())

    TsvWriter[Tally](handle).write(record)

    assert handle.getvalue() == "site1\t0\t0\t0\t0\t0\t4\t0\t0\n"


def test_writer_refuses_a_member_counted_twice() -> None:
    """Test that a Counter holding both a plain Enum member and its text is refused."""
    counts = cast(Counter[Quality], Counter({Quality.HIGH: 1, "high": 2}))
    record = Tally("site1", Counter(), counts, Counter())

    message = r"^Could not write field 'qualities', which counts 'high' twice!$"
    with pytest.raises(ValueError, match=message):
        TsvWriter[Tally](StringIO()).write(record)


def test_readers_and_writers_refuse_a_member_named_like_a_field() -> None:
    """Test that a member's column may not share its name with another field."""

    class Letter(StrEnum):
        A = "A"
        NAME = "name"

    record_type = make_dataclass("Bad", [("name", str), ("counts", CounterColumns[Letter])])

    message = r"^The CounterColumns field 'counts' of Bad has a column 'name' named like a field!$"
    for kind in READERS_AND_WRITERS:
        with pytest.raises(TypeError, match=message):
            _ = kind[record_type](StringIO("name\n"))


def test_readers_and_writers_refuse_an_optional_counter_field() -> None:
    """Test that an optional CounterColumns field is refused rather than read as one column."""
    record_type = make_dataclass("Bad", [("counts", CounterColumns[Base] | None)])

    message = r"^The CounterColumns field 'counts' of Bad may not be optional!$"
    for kind in READERS_AND_WRITERS:
        with pytest.raises(TypeError, match=message):
            _ = kind[record_type](StringIO("A\n"))
