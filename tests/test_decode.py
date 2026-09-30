from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from msgspec import ValidationError
from typing_extensions import assert_type

from typeline import Comment
from typeline import CounterColumns
from typeline import CsvReader
from typeline import ExtraColumns
from typeline import TsvReader
from typeline.codecs import boolean

from .records import SimpleMetric


class Vote(StrEnum):
    """The answers a ballot counts."""

    YES = "yes"
    NO = "no"


@dataclass(frozen=True)
class Tally:
    """A record with a count of each vote beside its other fields."""

    precinct: str
    votes: CounterColumns[Vote]
    turnout: int | None


@dataclass(frozen=True)
class Tagged:
    """A record with two named fields and any number of extra columns."""

    name: str
    size: int
    extra: ExtraColumns = ()


@dataclass(frozen=True)
class Note:
    """A record of free text."""

    title: str
    body: str


@pytest.mark.parametrize("ending", ["", "\n", "\r\n"])
def test_decode_reads_one_record_from_a_line(ending: str) -> None:
    """Test that decode reads a record from a line, with or without its line break."""
    reader = TsvReader[SimpleMetric](StringIO(), header=False)

    record = reader.decode(f"1\tname\t0.2{ending}")

    _ = assert_type(record, SimpleMetric)
    assert record == SimpleMetric(field1=1, field2="name", field3=0.2)


def test_decode_reads_columns_in_the_order_of_the_header() -> None:
    """Test that decode matches columns to fields by the header the reader was built with."""
    reader = CsvReader[SimpleMetric](StringIO("field3,field1,field2\n"))

    assert reader.decode("0.2,1,name\n") == SimpleMetric(field1=1, field2="name", field3=0.2)


def test_decode_reads_fields_with_the_options_of_the_reader() -> None:
    """Test that decode reads fields with the reader's none field, codecs, and dec_hook."""

    @dataclass
    class Visit:
        patient: str | None
        consented: bool
        room: complex

    def dec_hook(kind: type, obj: Any) -> Any:
        if kind is complex:
            return complex(obj)
        raise NotImplementedError

    reader = TsvReader[Visit](
        StringIO(),
        header=False,
        none_field=".",
        codecs={bool: boolean(true="Y", false="N")},
        dec_hook=dec_hook,
    )

    assert reader.decode(".\tY\t1+2j\n") == Visit(None, True, complex(1, 2))


def test_decode_keeps_extra_columns_in_file_order() -> None:
    """Test that decode keeps columns no field takes as extra columns, in the order they come."""
    without_header = TsvReader[Tagged](StringIO(), header=False)
    with_header = TsvReader[Tagged](StringIO("color\tsize\tshape\tname\n"))

    assert without_header.decode("a\t1\tx\ty\n") == Tagged("a", 1, ("x", "y"))
    assert with_header.decode("red\t2\tround\tb\n") == Tagged("b", 2, ("red", "round"))


def test_decode_reads_counter_columns_found_by_the_header() -> None:
    """Test that decode reads each member's count from the column the header names for it."""
    reader = TsvReader[Tally](StringIO("no\tturnout\tprecinct\tyes\n"))

    assert reader.decode("3\t10\tnorth\t7\n") == Tally(
        "north", Counter({Vote.YES: 7, Vote.NO: 3}), 10
    )


def test_decode_reads_quotes_as_text_without_quoting() -> None:
    """Test that decode reads a quote as text, which joins no lines, when quoting is off."""
    reader = TsvReader[Note](StringIO(), header=False, quoting=False)

    assert reader.decode('"quoted\ta"b\n') == Note('"quoted', 'a"b')
    with pytest.raises(ValueError, match=r", which holds more than one record!$"):
        _ = reader.decode('title\t"one\ntwo"\n')


@pytest.mark.parametrize(
    "line,body",
    [
        pytest.param('title,"one\ntwo"\n', "one\ntwo", id="newline"),
        pytest.param('title,"one\r\ntwo"\r\n', "one\r\ntwo", id="carriage-return-newline"),
        pytest.param('title,"one\rtwo"', "one\rtwo", id="carriage-return"),
        pytest.param('title,"one\n\ntwo"', "one\n\ntwo", id="blank-line"),
    ],
)
def test_decode_reads_a_quoted_field_holding_line_breaks(line: str, body: str) -> None:
    """Test that decode reads one record whose quoted field holds line breaks."""
    reader = CsvReader[Note](StringIO(), header=False)

    assert reader.decode(line) == Note("title", body)


@pytest.mark.parametrize("line", ['title,"one', 'title,"one\n', 'title,"one\ntwo\n'])
def test_decode_refuses_a_line_that_ends_inside_a_quoted_field(line: str) -> None:
    """Test that decode refuses text that ends before a quoted field is closed."""
    reader = CsvReader[Note](StringIO(), header=False)

    message = r"^Could not decode Note from .*, which ends inside a quoted field!$"
    with pytest.raises(ValueError, match=message):
        _ = reader.decode(line)


@pytest.mark.parametrize("line", ["", "\n", "\r\n", " \t \n"])
def test_decode_refuses_a_blank_line(line: str) -> None:
    """Test that decode refuses a line that holds only whitespace, which readers skip as blank."""
    reader = CsvReader[Note](StringIO(), header=False)

    with pytest.raises(ValueError, match=r"^Could not decode Note from blank line .*!$"):
        _ = reader.decode(line)


def test_decode_reads_a_line_of_empty_fields() -> None:
    """Test that a line of only whitespace and delimiters is a record, as readers read it."""
    reader = TsvReader[Note](StringIO(), header=False)

    assert reader.decode(" \t\n") == Note(" ", "")


def test_decode_refuses_a_comment_line() -> None:
    """Test that decode refuses a comment line, without sending it to on_comment."""
    comments: list[Comment] = []
    reader = TsvReader[Note](
        StringIO(), header=False, comment_prefixes=("#", "//"), on_comment=comments.append
    )

    for line in ["# a note\n", "//\tanother\n"]:
        with pytest.raises(ValueError, match=r"^Could not decode Note from comment line .*!$"):
            _ = reader.decode(line)

    assert comments == []


def test_decode_reads_a_line_with_a_prefix_that_is_not_a_comment_prefix() -> None:
    """Test that decode reads a line as a record when its prefix is not one of the reader's."""
    reader = TsvReader[Note](StringIO(), header=False, comment_prefixes=())

    assert reader.decode("#title\tbody\n") == Note("#title", "body")


@pytest.mark.parametrize(
    "line",
    [
        pytest.param("a,b\nc,d\n", id="two-records"),
        pytest.param("a,b\r\nc,d", id="two-records-crlf"),
        pytest.param("a,b\rc,d", id="two-records-cr"),
        pytest.param('a,"b"\nc,d', id="after-a-quoted-field"),
        pytest.param("a,b\n\n", id="trailing-blank-line"),
        pytest.param("\na,b\n", id="leading-blank-line"),
    ],
)
def test_decode_refuses_text_holding_more_than_one_record(line: str) -> None:
    """Test that decode refuses text holding more than one record, counting a blank line as one."""
    reader = CsvReader[Note](StringIO(), header=False)

    message = r"^Could not decode Note from .*, which holds more than one record!$"
    with pytest.raises(ValueError, match=message):
        _ = reader.decode(line)


def test_decode_keeps_line_breaks_that_do_not_end_a_line() -> None:
    """Test that decode splits text only at the line breaks readers split files at."""
    reader = TsvReader[Note](StringIO(), header=False)

    assert reader.decode("one\x0ctwo\tthree\x85four\u2028five\n") == Note(
        "one\x0ctwo", "three\x85four\u2028five"
    )


def test_decode_does_not_read_from_the_handle() -> None:
    """Test that decode leaves the handle where it was and records still read from it."""
    handle = StringIO("field1,field2,field3\n1,a,0.1\n2,b,0.2\n")
    reader = CsvReader[SimpleMetric](handle)
    position = handle.tell()

    assert reader.decode("9,z,0.9") == SimpleMetric(field1=9, field2="z", field3=0.9)
    assert handle.tell() == position
    assert list(reader) == [
        SimpleMetric(field1=1, field2="a", field3=0.1),
        SimpleMetric(field1=2, field2="b", field3=0.2),
    ]


def test_decode_may_be_interleaved_with_iteration() -> None:
    """Test that decoding between records leaves iteration, and the lines it reports, unchanged."""
    reader = CsvReader[SimpleMetric](StringIO("field1,field2,field3\n1,a,0.1\n# a note\n\n2,b\n"))
    records = iter(reader)

    assert next(records) == SimpleMetric(field1=1, field2="a", field3=0.1)
    assert reader.decode("9,z,\n") == SimpleMetric(field1=9, field2="z", field3=None)
    with pytest.raises(ValueError, match=r"^Expected 3 columns but found 1 for record type"):
        _ = reader.decode("oops\n")
    message = r"^Expected 3 columns but found 2 on line 5 for record type: SimpleMetric\.$"
    with pytest.raises(ValueError, match=message):
        _ = next(records)


def test_decode_does_not_close_a_reader_from_a_path(tmp_path: Path) -> None:
    """Test that decode leaves a reader from from_path open, so it may still be read."""
    path = tmp_path / "test.tsv"
    _ = path.write_text("field1\tfield2\tfield3\n1\ta\t0.1\n")

    with TsvReader.from_path[SimpleMetric](path) as reader:
        assert reader.decode("2\tb\t0.2\n") == SimpleMetric(field1=2, field2="b", field3=0.2)
        assert list(reader) == [SimpleMetric(field1=1, field2="a", field3=0.1)]


@pytest.mark.parametrize(
    "line,found",
    [
        pytest.param("1\tname", 2, id="too-few-columns"),
        pytest.param("1\tname\t0.2\textra", 4, id="too-many-columns"),
    ],
)
def test_decode_names_the_columns_of_a_line_of_the_wrong_width(line: str, found: int) -> None:
    """Test that decode reports a line of the wrong width in columns, without a line number."""
    reader = TsvReader[SimpleMetric](StringIO(), header=False)

    message = rf"^Expected 3 columns but found {found} for record type: SimpleMetric\.$"
    with pytest.raises(ValueError, match=message):
        _ = reader.decode(line)


def test_decode_names_a_field_its_codec_could_not_read() -> None:
    """Test that decode reports the field a codec could not read, without a line number."""

    @dataclass
    class Visit:
        patient: str
        consented: bool

    reader = TsvReader[Visit](StringIO(), header=False, codecs={bool: boolean()})

    message = r"^Could not read field 'consented' of type bool from text 'yes'!$"
    with pytest.raises(ValueError, match=message) as error:
        _ = reader.decode("P-001\tyes\n")
    assert isinstance(error.value.__cause__, ValueError)


def test_decode_names_a_column_that_does_not_hold_a_count() -> None:
    """Test that decode reports a member column that holds no count, without a line number."""
    reader = TsvReader[Tally](StringIO(), header=False)

    message = r"^Could not read column 'no' of field 'votes' as a count from 'many'!$"
    with pytest.raises(ValueError, match=message):
        _ = reader.decode("north\t7\tmany\t10\n")


def test_decode_names_the_record_it_could_not_build() -> None:
    """Test that decode reports a record msgspec could not build, without a line number."""
    reader = TsvReader[SimpleMetric](StringIO(), header=False)

    message = (
        r"^Could not build SimpleMetric from"
        + r" \{'field1': '1', 'field2': 'name', 'field3': 'BOMB'\}! Expected `float \| null`"
    )
    with pytest.raises(ValidationError, match=message):
        _ = reader.decode("1\tname\tBOMB\n")
