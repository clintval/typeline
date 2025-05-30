from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Optional

import pytest
from typing_extensions import override

from typeline import CsvWriter
from typeline import RecordType
from typeline import TsvWriter

from .conftest import ComplexMetric
from .conftest import SimpleMetric


def test_writer_raises_exception_on_non_dataclass(tmp_path: Path) -> None:
    """Test that the writer will raise an exception for non-dataclasses."""

    class MyTest:
        """A test metric."""

    with pytest.raises(ValueError, match="record_type is not a dataclass but must be!"):
        CsvWriter.from_path(tmp_path / "test.txt", MyTest)  # type: ignore[type-var]


def test_csv_writer_is_set_to_use_comma(tmp_path: Path) -> None:
    """Test that the CSV writer is set to use a comma."""
    with CsvWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"

    with CsvWriter(open(tmp_path / "test.txt", "w"), SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"


def test_tsv_writer_is_set_to_use_tab(tmp_path: Path) -> None:
    """Test that the TSV writer is set to use a tab."""
    with TsvWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"

    with TsvWriter(open(tmp_path / "test.txt", "w"), SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"


def test_writer_will_write_a_header(tmp_path: Path) -> None:
    """Test that the writer will write a header when asked to."""
    with CsvWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"


def test_writer_will_allow_a_custom_delimiter(tmp_path: Path) -> None:
    """Test that the writer will write with a tab delimiter."""
    with TsvWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"


def test_writer_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the writer will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
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
    with TsvWriter.from_path(tmp_path / "test.txt", ComplexMetric) as writer:
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
        + "\tnull"
        + "\t0.2\n"
    )
    assert (tmp_path / "test.txt").read_text() == expected


def test_writer_can_write_with_a_custom_callback(tmp_path: Path) -> None:
    """Test we can implement a writer with a custom encode callback."""

    @dataclass
    class MyMetric:
        field1: float
        field2: list[int]

    class SimpleListWriter(CsvWriter[RecordType]):
        @override
        def _encode(self, item: Any) -> Any:
            """A callback for overriding the encoding of builtin types and custom types."""
            if isinstance(item, list):
                return ",".join(map(str, item))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            return item

    with SimpleListWriter.from_path(tmp_path / "test.txt", MyMetric) as writer:
        writer.write(MyMetric(0.1, [1, 2, 3]))

    assert (tmp_path / "test.txt").read_text() == "0.1,'1,2,3'\n"


def test_writer_can_write_old_style_optional_types(tmp_path: Path) -> None:
    """Test that the writer can write old style optional types."""

    @dataclass
    class MyMetric:
        field1: float
        field2: Optional[int]
        field3: Optional[list[int]]

    with CsvWriter.from_path(tmp_path / "test.txt", MyMetric) as writer:
        writer.write(MyMetric(0.1, 1, None))
        writer.write(MyMetric(0.2, None, [1, 2, 3]))

    assert (tmp_path / "test.txt").read_text() == "0.1,1,null\n0.2,null,'[1,2,3]'\n"
