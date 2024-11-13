from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Optional
from typing import get_origin

import pytest
from msgspec import DecodeError
from msgspec import ValidationError
from typing_extensions import override

from typeline import CsvRecordReader
from typeline import CsvRecordWriter
from typeline import RecordType
from typeline import TsvRecordReader
from typeline import TsvRecordWriter

from .conftest import ComplexMetric
from .conftest import SimpleMetric


def test_csv_reader_is_set_to_use_comma(tmp_path: Path) -> None:
    """Test that the CSV reader is set to use a comma."""
    with CsvRecordWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "1,name,0.2\n",
    ])

    with CsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]

    with CsvRecordWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "1,name,0.2\n",
    ])

    with CsvRecordReader(open(tmp_path / "test.txt", "r"), SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_tsv_reader_is_set_to_use_tab(tmp_path: Path) -> None:
    """Test that the TSV reader is set to use a tab."""
    with TsvRecordWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1\tfield2\tfield3",
        "1\tname\t0.2\n",
    ])

    with TsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]

    with TsvRecordWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1\tfield2\tfield3",
        "1\tname\t0.2\n",
    ])

    with TsvRecordReader(open(tmp_path / "test.txt", "r"), SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_raises_exception_on_non_dataclass(tmp_path: Path) -> None:
    """Test that the reader will raise an exception for non-dataclasses."""

    class MyTest:
        """A test metric."""

    (tmp_path / "test.txt").touch()

    with pytest.raises(ValueError, match="record_type is not a dataclass but must be!"):
        CsvRecordReader.from_path(tmp_path / "test.txt", MyTest)  # type: ignore[type-var]


def test_reader_raises_exception_when_header_is_wrong(tmp_path: Path) -> None:
    """Test that the reader will raise an exception when the header is wrong."""
    (tmp_path / "test.txt").write_text("field10,field11,field13\n")

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        CsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric)


def test_reader_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the reader will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvRecordWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "1\t'my\tname'\t0.2\n"

    with TsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric, header=False) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="my\tname", field3=0.2)]


def test_reader_will_write_a_complicated_record(tmp_path: Path) -> None:
    """Test that the reader will write a complicated record with nested fields."""
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
    with TsvRecordWriter.from_path(tmp_path / "test.txt", ComplexMetric) as writer:
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

    with TsvRecordReader.from_path(tmp_path / "test.txt", ComplexMetric, header=False) as reader:
        assert list(reader) == [metric]


def test_csv_reader_ignores_comments_and_blank_lines(tmp_path: Path) -> None:
    """Test that the CSV reader is set to use a comma."""
    with CsvRecordWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer._handle.write("# this is a comment\n")
        writer._handle.write("#and this is a comment too!\n")
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
        writer._handle.write("\n")
        writer._handle.write("  \n")
        writer.write(SimpleMetric(field1=2, field2="name2", field3=0.3))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "# this is a comment",
        "#and this is a comment too!",
        "1,name,0.2",
        "",
        "  ",
        "2,name2,0.3\n",
    ])

    with CsvRecordReader.from_path(
        tmp_path / "test.txt", SimpleMetric, comment_prefixes={"#"}
    ) as reader:
        assert list(reader) == [
            SimpleMetric(field1=1, field2="name", field3=0.2),
            SimpleMetric(field1=2, field2="name2", field3=0.3),
        ]


def test_reader_raises_exception_for_missing_fields(tmp_path: Path) -> None:
    """Test the reader raises an exception for missing fields."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\n",
            "1\tname\t0.2\n",
        ])
    )

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        TsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric)


def test_reader_raises_exception_for_extra_fields(tmp_path: Path) -> None:
    """Test the reader raises an exception for extra fields."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\tfield3\tfield4\n",
            "1\tname\t0.2\thi-five\n",
        ])
    )

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        TsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric)


def test_reader_raises_exception_for_failed_type_coercion(tmp_path: Path) -> None:
    """Test the reader raises an exception for failed type coercion."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\tfield3\n",
            "1\tname\tBOMB\n",
        ])
    )

    with (
        TsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader,
        pytest.raises(
            DecodeError,
            match=(
                r"Could not load delimited data line into JSON\-like format\."
                + r" Built improperly formatted JSON\:"
                + r" \{\"field1\"\:1\,\"field2\"\:\"name\"\,\"field3\"\:BOMB\}\."
                + r" Original exception\: JSON is malformed\:"
                + r" invalid character \(byte \d\d\)\."
            ),
        ),
    ):
        list(reader)


def test_reader_can_read_empty_file_ok(tmp_path: Path) -> None:
    """Test the reader can read an empty file if asked to."""
    (tmp_path / "test.txt").touch()

    with (
        TsvRecordReader.from_path(tmp_path / "test.txt", SimpleMetric, header=False) as reader,
    ):
        assert list(reader) == []


def test_reader_can_read_with_a_custom_callback(tmp_path: Path) -> None:
    """Test we can implement a reader with a custom decode callback."""

    @dataclass
    class MyMetric:
        field1: float
        field2: list[int]

    (tmp_path / "test.txt").write_text("field1,field2\n0.1,'1,2,3,'\n")

    class SimpleListReader(CsvRecordReader[RecordType]):
        @override
        def _decode(self, field_type: type[Any] | str | Any, item: Any) -> Any:
            """A callback for overriding the decoding of builtin types and custom types."""
            if get_origin(field_type) is list:
                stripped: str = item.rstrip(",")
                return f"[{stripped}]"
            return super()._decode(field_type, item=item)

    with SimpleListReader.from_path(tmp_path / "test.txt", MyMetric) as reader:
        assert list(reader) == [MyMetric(0.1, [1, 2, 3])]


def test_reader_msgspec_validation_exception(tmp_path: Path) -> None:
    """Test that we clarify when msgspec cannot decode a structure of builtins."""

    @dataclass
    class MyData:
        field1: str
        field2: list[int]

    (tmp_path / "test.txt").write_text("field1,field2\nmy-name,null\n")

    with CsvRecordReader.from_path(tmp_path / "test.txt", MyData) as reader:
        with pytest.raises(
            ValidationError,
            match=(
                r"Could not parse JSON\-like object into requested structure\:"
                + r" \[\(\'field1\'\, \'my-name\'\)\, \(\'field2\'\, None\)\]\."
                + r" Requested structure\: MyData. Original exception\:"
                + r" Expected \`array\`\, got \`null\` \- at \`\$\.field2\`"
            ),
        ):
            list(reader)


def test_reader_can_read_old_style_optional_types(tmp_path: Path) -> None:
    """Test that the reader can read old style optional types."""

    @dataclass
    class MyMetric:
        field1: float
        field2: Optional[int]
        field3: Optional[str]
        field4: Optional[list[int]]

    (tmp_path / "test.txt").write_text("0.1,1,hello,null\n0.2,null,null,'[1,2,3]'\n")

    with CsvRecordReader.from_path(tmp_path / "test.txt", MyMetric, header=False) as reader:
        record1, record2 = list(iter(reader))

    assert record1 == MyMetric(0.1, 1, "hello", None)
    assert record2 == MyMetric(0.2, None, None, [1, 2, 3])
