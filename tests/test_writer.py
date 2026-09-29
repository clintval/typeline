from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Optional

import pytest

from typeline import CsvReader
from typeline import CsvWriter
from typeline import TsvWriter

from .conftest import ComplexMetric
from .conftest import SimpleMetric


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


def test_writer_will_write_a_header(tmp_path: Path) -> None:
    """Test that the writer will write a header when asked to."""
    with CsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"


def test_writer_will_allow_a_custom_delimiter(tmp_path: Path) -> None:
    """Test that the writer will write with a tab delimiter."""
    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"


def test_writer_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the writer will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "1\t'my\tname'\t0.2\n"


def test_writer_will_write_a_complicated_record(tmp_path: Path) -> None:
    """Test that the writer will write a complicated record with nested fields."""
    metric = ComplexMetric(
        field1=1,
        field2="my\tname",
        field3=0.2,
        field4=[1, 2, 3],
        field5=set([3, 4, 5]),
        field6=(5, 6, 7),
        field7={"field1": 1, "field2": 2},
        field8=SimpleMetric(field1=10, field2="hi-mom", field3=None),
        field9={
            "first": SimpleMetric(field1=2, field2="hi-dad", field3=0.2),
            "second": SimpleMetric(field1=3, field2="hi-all", field3=0.3),
        },
        field10=True,
        field11=None,
        field12=0.2,
    )
    with TsvWriter.from_path[ComplexMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    expected: str = (
        "1"
        + "\t'my\tname'"
        + "\t0.2"
        + "\t[1,2,3]"
        + "\t[3,4,5]"
        + "\t[5,6,7]"
        + '\t{"field1":1,"field2":2}'
        + '\t{"field1":10,"field2":"hi-mom","field3":null}'
        + '\t{"first":{"field1":2,"field2":"hi-dad","field3":0.2}'
        + ',"second":{"field1":3,"field2":"hi-all","field3":0.3}}'
        + "\ttrue"
        + "\t"
        + "\t0.2\n"
    )
    assert (tmp_path / "test.txt").read_text() == expected


def test_writer_can_write_with_a_custom_callback(tmp_path: Path) -> None:
    """Test we can implement a writer with a custom encode callback."""

    class MyCustomType:
        """A custom class to test encoding."""

        def __init__(self, value: str) -> None:
            self.value = value

        def __repr__(self) -> str:
            return f"{self.value}!"

    @dataclass
    class MyMetric:
        field1: float
        field2: MyCustomType

    def enc_hook(value: Any) -> Any:
        """A custom encoding hook for the writer."""
        if isinstance(value, MyCustomType):
            return repr(value)
        return value

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt", enc_hook=enc_hook) as writer:
        writer.write(MyMetric(0.1, MyCustomType("hello")))

    assert (tmp_path / "test.txt").read_text() == "0.1,hello!\n"


def test_writer_can_write_old_style_optional_types(tmp_path: Path) -> None:
    """Test that the writer can write old style optional types."""

    @dataclass
    class MyMetric:
        field1: float
        field2: Optional[int]
        field3: Optional[list[int]]

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt") as writer:
        writer.write(MyMetric(0.1, 1, None))
        writer.write(MyMetric(0.2, None, [1, 2, 3]))

    assert (tmp_path / "test.txt").read_text() == "0.1,1,\n0.2,,'[1,2,3]'\n"


def test_writer_keeps_quotes_that_are_part_of_a_string(tmp_path: Path) -> None:
    """Test that the writer does not strip quote characters that belong to a string value."""

    @dataclass
    class MyMetric:
        field1: str
        field2: list[str]

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt") as writer:
        writer.write(MyMetric('"quoted"', ['a"b']))

    assert (tmp_path / "test.txt").read_text() == '"quoted",["a\\"b"]\n'

    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [MyMetric('"quoted"', ['a"b'])]


@pytest.mark.parametrize("none_field,expected", [("", ""), ("null", "null"), ("NA", "NA")])
def test_writer_writes_none_as_the_none_field(
    tmp_path: Path, none_field: str, expected: str
) -> None:
    """Test that the writer writes None as the none field."""

    @dataclass
    class MyMetric:
        field1: int | None
        field2: int | None

    with CsvWriter.from_path[MyMetric](tmp_path / "test.txt", none_field=none_field) as writer:
        writer.write(MyMetric(None, 1))

    assert (tmp_path / "test.txt").read_text() == f"{expected},1\n"


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
