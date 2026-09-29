from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest
from msgspec import ValidationError

from typeline import CsvReader
from typeline import CsvWriter
from typeline import TsvReader
from typeline import TsvWriter
from typeline.codecs import delimited

from .conftest import ComplexMetric
from .conftest import SimpleMetric


def test_csv_reader_is_set_to_use_comma(tmp_path: Path) -> None:
    """Test that the CSV reader is set to use a comma."""
    with CsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "1,name,0.2\n",
    ])

    with CsvReader.from_path[SimpleMetric](tmp_path / "test.txt") as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]

    with CsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "1,name,0.2\n",
    ])

    with CsvReader[SimpleMetric](open(tmp_path / "test.txt", "r")) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_tsv_reader_is_set_to_use_tab(tmp_path: Path) -> None:
    """Test that the TSV reader is set to use a tab."""
    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1\tfield2\tfield3",
        "1\tname\t0.2\n",
    ])

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.txt") as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]

    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write_header()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1\tfield2\tfield3",
        "1\tname\t0.2\n",
    ])

    with TsvReader[SimpleMetric](open(tmp_path / "test.txt", "r")) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_raises_exception_when_header_is_wrong(tmp_path: Path) -> None:
    """Test that the reader will raise an exception when the header is wrong."""
    (tmp_path / "test.txt").write_text("field10,field11,field13\n")

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        CsvReader.from_path[SimpleMetric](tmp_path / "test.txt")


def test_reader_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the reader will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "1\t'my\tname'\t0.2\n"

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.txt", header=False) as reader:
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

    with TsvReader.from_path[ComplexMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [metric]


def test_csv_reader_ignores_comments_and_blank_lines(tmp_path: Path) -> None:
    """Test that the CSV reader is set to use a comma."""
    with CsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
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

    with CsvReader.from_path[SimpleMetric](tmp_path / "test.txt", comment_prefixes={"#"}) as reader:
        assert list(reader) == [
            SimpleMetric(field1=1, field2="name", field3=0.2),
            SimpleMetric(field1=2, field2="name2", field3=0.3),
        ]


@pytest.mark.parametrize(
    "header,detail",
    [
        pytest.param(
            "field1\tfield2",
            "Header: ['field1', 'field2']. Fields of SimpleMetric: ['field1', 'field2', 'field3']."
            + " Missing from header: ['field3'].",
            id="missing",
        ),
        pytest.param(
            "field1\tfield2\tfield3\tfield4",
            "Unexpected in header: ['field4'].",
            id="unexpected",
        ),
        pytest.param(
            "field1\tfield2\tfield4",
            "Missing from header: ['field3']. Unexpected in header: ['field4'].",
            id="missing-and-unexpected",
        ),
        pytest.param("field3\tfield2\tfield1", "The fields are out of order.", id="out-of-order"),
    ],
)
def test_reader_names_how_the_header_differs_from_the_dataclass(
    tmp_path: Path, header: str, detail: str
) -> None:
    """Test the header mismatch error shows both headers and how they differ."""
    (tmp_path / "test.txt").write_text(f"{header}\n")

    with pytest.raises(ValueError) as exception:
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt")

    assert str(exception.value).startswith("Fields of header do not match fields of dataclass!")
    assert detail in str(exception.value)


def test_reader_raises_exception_for_missing_fields(tmp_path: Path) -> None:
    """Test the reader raises an exception for missing fields."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\n",
            "1\tname\t0.2\n",
        ])
    )

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt")


def test_reader_raises_exception_for_extra_fields(tmp_path: Path) -> None:
    """Test the reader raises an exception for extra fields."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\tfield3\tfield4\n",
            "1\tname\t0.2\thi-five\n",
        ])
    )

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt")


@pytest.mark.parametrize(
    "line,found",
    [
        pytest.param("1\tname", 2, id="too-few-fields"),
        pytest.param("1\tname\t0.2\thi-five", 4, id="too-many-fields"),
    ],
)
def test_reader_raises_exception_for_a_record_with_the_wrong_number_of_fields(
    tmp_path: Path, line: str, found: int
) -> None:
    """Test the reader names the line and field counts when a record is too short or too long."""
    (tmp_path / "test.txt").write_text("\n".join(["field1\tfield2\tfield3", "1\tname\t0.2", line]))

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.txt") as reader:
        with pytest.raises(
            ValueError,
            match=f"Expected 3 fields but found {found} on line 3 for record type: SimpleMetric.",
        ):
            _ = list(reader)


def test_reader_raises_exception_for_failed_type_coercion(tmp_path: Path) -> None:
    """Test the reader raises an exception for failed type coercion."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\tfield3",
            "1\tname\tBOMB",
        ])
    )

    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt") as reader,
        pytest.raises(
            ValidationError,
            match=(
                r"Could not parse JSON\-like object into requested structure\:"
                + r" \{\'field1\'\: \'1\', \'field2\'\: \'name\', \'field3\'\: \'BOMB\'\}\."
            ),
        ),
    ):
        list(reader)


def test_reader_can_read_empty_file_ok(tmp_path: Path) -> None:
    """Test the reader can read an empty file if asked to."""
    (tmp_path / "test.txt").touch()

    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt", header=False) as reader,
    ):
        assert list(reader) == []


def test_reader_can_read_with_a_field_codec(tmp_path: Path) -> None:
    """Test we can read a field in a custom text format with a codec for its type."""

    @dataclass
    class MyMetric:
        field1: float
        field2: list[int]

    (tmp_path / "test.txt").write_text("field1,field2\n0.1,'1|2|3|'\n")

    codecs = {list[int]: delimited(int, sep="|", trailing_sep=True)}
    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", codecs=codecs) as reader:
        assert list(reader) == [MyMetric(0.1, [1, 2, 3])]


def test_reader_msgspec_validation_exception(tmp_path: Path) -> None:
    """Test that we clarify when msgspec cannot decode a structure of builtins."""

    @dataclass
    class MyData:
        field1: str
        field2: list[int]

    (tmp_path / "test.txt").write_text("field1,field2\nmy-name,null\n")

    with CsvReader.from_path[MyData](tmp_path / "test.txt") as reader:
        with pytest.raises(
            ValidationError,
            match=(
                r"Could not parse JSON\-like object into requested structure\:"
                + r" \{\'field1\'\: \'my\-name\'"
                + r".*\'field2\'\: None\}"
                + r".*Requested structure\: MyData\."
                + r".*Expected \`array\`\, got \`null\`"
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

    (tmp_path / "test.txt").write_text("0.1,1,hello,\n0.2,,,'[1,2,3]'\n")

    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", header=False) as reader:
        record1, record2 = list(iter(reader))

    assert record1 == MyMetric(0.1, 1, "hello", None)
    assert record2 == MyMetric(0.2, None, None, [1, 2, 3])


def test_reader_should_be_usable_right_after_file_handle_open(tmp_path: Path) -> None:
    """Test that the reader should be usable right after file handle open."""
    (tmp_path / "test.txt").write_text("field1,field2,field3\n1,name,\n")

    with open(tmp_path / "test.txt", "r") as handle, CsvReader[SimpleMetric](handle) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=None)]


@dataclass
class TextFields:
    """A record with a required and an optional text field."""

    required: str
    optional: str | None


@pytest.mark.parametrize(
    "line,expected",
    [
        pytest.param(",", TextFields("", None), id="empty"),
        pytest.param("null,null", TextFields("null", "null"), id="null"),
        pytest.param("true,false", TextFields("true", "false"), id="booleans"),
        pytest.param("'[1]','{2}'", TextFields("[1]", "{2}"), id="json-looking"),
    ],
)
def test_reader_keeps_text_in_str_fields(tmp_path: Path, line: str, expected: TextFields) -> None:
    """Test that text fields are not parsed as JSON, and are only None when optional."""
    (tmp_path / "test.csv").write_text(f"required,optional\n{line}\n")

    with CsvReader.from_path[TextFields](tmp_path / "test.csv") as reader:
        assert list(reader) == [expected]
