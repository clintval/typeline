from dataclasses import dataclass
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Literal
from typing import NewType
from typing import Optional  # pyright: ignore[reportDeprecated]

import pytest
from msgspec import ValidationError

from typeline import CsvReader
from typeline import CsvWriter
from typeline import TsvReader
from typeline import TsvWriter

from .records import ComplexMetric
from .records import SimpleMetric


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

    with TsvReader[SimpleMetric](open(tmp_path / "test.txt", "r")) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the reader will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvWriter.from_path[SimpleMetric](tmp_path / "test.txt") as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == '1\t"my\tname"\t0.2\n'

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="my\tname", field3=0.2)]


def test_a_complicated_record_round_trips(tmp_path: Path) -> None:
    """Test that a record with nested and collection fields is written and read back."""
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
        + '\t"my\tname"'
        + "\t0.2"
        + "\t[1,2,3]"
        + "\t[3,4,5]"
        + "\t[5,6,7]"
        + '\t"{""field1"":1,""field2"":2}"'
        + '\t"{""field1"":10,""field2"":""hi-mom"",""field3"":null}"'
        + '\t"{""first"":{""field1"":2,""field2"":""hi-dad"",""field3"":0.2}'
        + ',""second"":{""field1"":3,""field2"":""hi-all"",""field3"":0.3}}"'
        + "\ttrue"
        + "\t"
        + "\t0.2\n"
    )
    assert (tmp_path / "test.txt").read_text() == expected

    with TsvReader.from_path[ComplexMetric](tmp_path / "test.txt", header=False) as reader:
        assert list(reader) == [metric]


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
    _ = (tmp_path / "test.txt").write_text(
        "\n".join(["field1\tfield2\tfield3", "1\tname\t0.2", line])
    )

    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt") as reader,
        pytest.raises(
            ValueError,
            match=f"Expected 3 columns but found {found} on line 3 for record type: SimpleMetric.",
        ),
    ):
        _ = list(reader)


def test_reader_raises_exception_for_failed_type_coercion(tmp_path: Path) -> None:
    """Test the reader raises an exception for failed type coercion."""
    _ = (tmp_path / "test.txt").write_text(
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
                r"^Could not build SimpleMetric from"
                + r" \{\'field1\'\: \'1\', \'field2\'\: \'name\', \'field3\'\: \'BOMB\'\}"
            ),
        ),
    ):
        _ = list(reader)


def test_reader_can_read_empty_file_ok(tmp_path: Path) -> None:
    """Test the reader can read an empty file if asked to."""
    (tmp_path / "test.txt").touch()

    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.txt", header=False) as reader,
    ):
        assert list(reader) == []


def test_reader_msgspec_validation_exception(tmp_path: Path) -> None:
    """Test that we clarify when msgspec cannot decode a structure of builtins."""

    @dataclass
    class MyData:
        field1: str
        field2: list[int]

    _ = (tmp_path / "test.txt").write_text("field1,field2\nmy-name,null\n")

    with (
        CsvReader.from_path[MyData](tmp_path / "test.txt") as reader,
        pytest.raises(
            ValidationError,
            match=(
                r"^Could not build MyData from \{\'field1\'\: \'my\-name\'"
                + r".*\'field2\'\: None\} on line 2!"
                + r".*Expected \`array\`\, got \`null\`"
            ),
        ),
    ):
        _ = list(reader)


def test_reader_can_read_old_style_optional_types(tmp_path: Path) -> None:
    """Test that the reader can read old style optional types."""

    @dataclass
    class MyMetric:
        field1: float
        field2: Optional[int]  # pyright: ignore[reportDeprecated]
        field3: Optional[str]  # pyright: ignore[reportDeprecated]
        field4: Optional[list[int]]  # pyright: ignore[reportDeprecated]

    _ = (tmp_path / "test.txt").write_text('0.1,1,hello,\n0.2,,,"[1,2,3]"\n')

    with CsvReader.from_path[MyMetric](tmp_path / "test.txt", header=False) as reader:
        record1, record2 = list(iter(reader))

    assert record1 == MyMetric(0.1, 1, "hello", None)
    assert record2 == MyMetric(0.2, None, None, [1, 2, 3])


def test_reader_reads_from_an_open_handle(tmp_path: Path) -> None:
    """Test that a reader reads from a handle the caller opened."""
    _ = (tmp_path / "test.txt").write_text("field1,field2,field3\n1,name,\n")

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
        pytest.param("[1],{2}", TextFields("[1]", "{2}"), id="json-looking"),
    ],
)
def test_reader_keeps_text_in_str_fields(tmp_path: Path, line: str, expected: TextFields) -> None:
    """Test that text fields are not parsed as JSON, and are only None when optional."""
    _ = (tmp_path / "test.csv").write_text(f"required,optional\n{line}\n")

    with CsvReader.from_path[TextFields](tmp_path / "test.csv") as reader:
        assert list(reader) == [expected]


@dataclass
class Contact:
    """A record with free text that can hold delimiters and quotes."""

    name: str
    notes: str
    tags: list[str]


def test_reader_reads_standard_csv_quoting(tmp_path: Path) -> None:
    """Test that the reader reads fields quoted with double quotes, as other CSV tools write."""
    _ = (tmp_path / "test.csv").write_text(
        'name,notes,tags\n"Doe, Jane","said ""hi""","[""a"",""b""]"\nO\'Brien,it\'s fine,[]\n'
    )

    with CsvReader.from_path[Contact](tmp_path / "test.csv") as reader:
        assert list(reader) == [
            Contact("Doe, Jane", 'said "hi"', ["a", "b"]),
            Contact("O'Brien", "it's fine", []),
        ]


@dataclass
class Feature:
    """A record of free text read without quoting."""

    name: str
    notes: str


def test_reader_reads_quotes_as_text_without_quoting(tmp_path: Path) -> None:
    """Test that without quoting a quote is text, and one opening a field does not join lines."""
    _ = (tmp_path / "test.tsv").write_text('"quoted\ta"b\nnext\t"\n""\tlast"\n')

    with TsvReader.from_path[Feature](tmp_path / "test.tsv", header=False, quoting=False) as reader:
        assert list(reader) == [
            Feature('"quoted', 'a"b'),
            Feature("next", '"'),
            Feature('""', 'last"'),
        ]


def test_reader_reads_quotes_as_text_without_quoting_from_a_stream(tmp_path: Path) -> None:
    """Test that the reader constructor takes quoting, as from_path does."""
    _ = (tmp_path / "test.tsv").write_text('name\tnotes\n"quoted\ta"b\n')

    with TsvReader[Feature](open(tmp_path / "test.tsv"), quoting=False) as reader:
        assert list(reader) == [Feature('"quoted', 'a"b')]


SampleId = NewType("SampleId", str)


class Kind(str, Enum):
    """A text enum whose values look like JSON."""

    Null = "null"
    Listed = "[x]"


@dataclass
class TextLike:
    """A record whose fields are text, though not declared as plain `str`."""

    ident: SampleId
    flag: Literal["true", "false"]
    kind: Kind


def test_reader_keeps_text_in_text_like_fields(tmp_path: Path) -> None:
    """Test that NewTypes of str, Literals of strings, and str Enums are not parsed as JSON."""
    _ = (tmp_path / "test.csv").write_text("true,false,null\nnull,true,[x]\n")

    with CsvReader.from_path[TextLike](tmp_path / "test.csv", header=False) as reader:
        assert list(reader) == [
            TextLike(SampleId("true"), "false", Kind.Null),
            TextLike(SampleId("null"), "true", Kind.Listed),
        ]


def test_errors_name_the_line_a_record_starts_on() -> None:
    """Test that an error in a record spanning lines names the line where the record starts."""
    text = 'field1,field2,field3\n1,"a\nb",x\n'
    with (
        CsvReader[SimpleMetric](StringIO(text)) as reader,
        pytest.raises(ValidationError, match=r"^Could not build SimpleMetric from .* on line 2!"),
    ):
        _ = list(reader)


def test_a_row_of_the_wrong_width_names_its_columns_and_line() -> None:
    """Test that a row with too few columns is reported in columns, with its line."""
    text = "field1,field2,field3\n\n1,a\n"
    message = r"^Expected 3 columns but found 2 on line 3 for record type: SimpleMetric\.$"
    with (
        CsvReader[SimpleMetric](StringIO(text)) as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)
