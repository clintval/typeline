import csv
from abc import ABC
from abc import abstractmethod
from dataclasses import InitVar
from dataclasses import dataclass
from dataclasses import field
from dataclasses import make_dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from enum import Enum
from enum import IntEnum
from io import BytesIO
from io import StringIO
from pathlib import Path
from re import escape
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import NewType
from typing import Optional  # pyright: ignore[reportDeprecated]
from typing import cast
from uuid import UUID

import pytest
from msgspec import Meta
from typing_extensions import override

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
    """Test the reader names the file, line and field counts when a record has the wrong width."""
    path = tmp_path / "test.txt"
    _ = path.write_text("\n".join(["field1\tfield2\tfield3", "1\tname\t0.2", line]))

    message = (
        rf"^Expected 3 columns but found {found} on line 3 of '{escape(str(path))}'"
        + r" for record type: SimpleMetric\.$"
    )
    with (
        TsvReader.from_path[SimpleMetric](path) as reader,
        pytest.raises(ValueError, match=message),
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
            ValueError,
            match=r"^Could not read field 'field3' of type float from text 'BOMB' on line 2 of ",
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

    path = tmp_path / "test.txt"
    _ = path.write_text("field1,field2\nmy-name,null\n")

    with (
        CsvReader.from_path[MyData](path) as reader,
        pytest.raises(
            ValueError,
            match=(
                r"^Could not read field 'field2' of type list\[int\] from text 'null'"
                + rf" on line 2 of '{escape(str(path))}'! Expected `array`, got `null`$"
            ),
        ),
    ):
        _ = list(reader)


def test_reader_reads_text_that_only_looks_like_json_as_text() -> None:
    """Test that a field that may hold text keeps malformed JSON as text, but refuses other JSON."""

    @dataclass
    class Note:
        body: str | list[int]

    reader = TsvReader[Note](StringIO(), header=False)

    assert reader.decode("[1,2]\n") == Note([1, 2])
    assert reader.decode("[not json\n") == Note("[not json")
    with pytest.raises(
        ValueError, match=r"from text '\{\}'! Expected `str \| array`, got `object`$"
    ):
        _ = reader.decode("{}\n")


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


@dataclass
class AnnotatedText:
    """A record whose text fields are annotated, without a codec for the annotation."""

    note: Annotated[str, "note"]
    flag: Annotated[Literal["true", "false"], "flag"]
    count: Annotated[int, Meta(ge=0)]


def test_reader_keeps_text_in_annotated_text_fields() -> None:
    """Test that annotated text fields keep text that looks like JSON, as plain ones do."""
    reader = CsvReader[AnnotatedText](StringIO(), header=False)

    assert reader.decode("true,false,1\n") == AnnotatedText("true", "false", 1)
    assert reader.decode("[x],true,2\n") == AnnotatedText("[x]", "true", 2)
    assert reader.decode("null,false,0\n") == AnnotatedText("null", "false", 0)


def test_reader_checks_the_constraints_of_an_annotated_number() -> None:
    """Test that an annotated number is still checked against its msgspec constraints."""
    reader = CsvReader[AnnotatedText](StringIO(), header=False)

    message = r"^Could not read field 'count' of type .* from text '-1'! Expected `int` >= 0$"
    with pytest.raises(ValueError, match=message):
        _ = reader.decode("a,true,-1\n")


def test_errors_name_the_line_a_record_starts_on() -> None:
    """Test that an error in a record spanning lines names the line where the record starts."""
    text = 'field1,field2,field3\n1,"a\nb",x\n'
    with (
        CsvReader[SimpleMetric](StringIO(text)) as reader,
        pytest.raises(ValueError, match=r"^Could not read field 'field3' .* on line 2!$"),
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


def test_reader_reads_a_lone_carriage_return_as_a_line_ending() -> None:
    """Test that a line may end with a lone carriage return, as in old Mac files."""
    text = "field1,field2,field3\r1,a,0.5\r\n2,b,\n"
    with CsvReader[SimpleMetric](StringIO(text)) as reader:
        assert list(reader) == [
            SimpleMetric(field1=1, field2="a", field3=0.5),
            SimpleMetric(field1=2, field2="b", field3=None),
        ]


def test_reader_reads_only_a_byte_order_mark_as_no_records() -> None:
    """Test that text of only a byte order mark holds no records, like empty text."""
    with CsvReader[SimpleMetric](StringIO("\ufeff"), header=False) as reader:
        assert list(reader) == []


def test_reader_keeps_a_byte_order_mark_that_does_not_start_the_text() -> None:
    """Test that only a byte order mark that starts the text is dropped."""

    @dataclass
    class Named:
        name: str

    with TsvReader[Named](StringIO('\ufeffa\n\ufeff"b"\n'), header=False) as reader:
        assert list(reader) == [Named("a"), Named('\ufeff"b"')]


def test_reader_reads_lines_from_any_iterable_of_text() -> None:
    """Test that a reader reads from an iterable of lines, not only from a stream."""
    lines = iter(["field1,field2,field3\n", "1,a,0.5\n", "", "2,b,\n"])
    reader = CsvReader[SimpleMetric](lines)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]
    assert list(reader) == [
        SimpleMetric(field1=1, field2="a", field3=0.5),
        SimpleMetric(field1=2, field2="b", field3=None),
    ]


def test_reader_refuses_a_stream_of_bytes() -> None:
    """Test that a reader names the type of a stream that is not text."""
    with pytest.raises(TypeError, match=r"^Delimited data must be read as text, not bytes!$"):
        _ = CsvReader[SimpleMetric](BytesIO(b"field1,field2,field3\n"))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]


def test_reader_reads_negative_zero_as_a_negative_float() -> None:
    """Test that the text -0 reads as negative zero in a float field."""
    with CsvReader[SimpleMetric](StringIO("1,a,-0\n"), header=False) as reader:
        (record,) = list(reader)
    assert str(record.field3) == "-0.0"


def test_reader_reads_a_field_longer_than_the_csv_module_allows() -> None:
    """Test that a field is not limited in length, as the csv module limits it."""
    long = "x" * (csv.field_size_limit() + 1)
    with CsvReader[SimpleMetric](StringIO(f"1,{long},0.5\n"), header=False) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2=long, field3=0.5)]


@dataclass
class OptionalInt:
    """A record of an optional integer."""

    value: int | None


@dataclass
class OptionalFloat:
    """A record of an optional float."""

    value: float | None


@dataclass
class OptionalBool:
    """A record of an optional boolean."""

    value: bool | None


@pytest.mark.parametrize(
    ("record_type", "text"),
    [
        (OptionalInt, "1e3"),
        (OptionalInt, "1.0"),
        (OptionalInt, "+1"),
        (OptionalInt, "007"),
        (OptionalInt, "null"),
        (OptionalFloat, "NaN"),
        (OptionalFloat, "Infinity"),
        (OptionalFloat, ".5"),
        (OptionalBool, "1"),
        (OptionalBool, "TRUE"),
        (OptionalBool, "True"),
    ],
)
def test_reader_reads_plain_fields_only_as_they_are_written(
    record_type: type[OptionalInt | OptionalFloat | OptionalBool], text: str
) -> None:
    """Test that int, float, and bool fields read only the text a writer writes for them."""
    message = rf"^Could not read field 'value' of type \w+ from text '{escape(text)}'!$"
    with pytest.raises(ValueError, match=message):
        reader: TsvReader[Any] = cast(Any, TsvReader)[record_type](StringIO(""), header=False)
        _ = reader.decode(f"{text}\n")


def test_reader_reads_back_every_float_and_integer_a_writer_writes() -> None:
    """Test that the text a writer writes for floats and integers reads back as the same value."""

    @dataclass
    class Numbers:
        real: float
        whole: int

    values = [
        Numbers(float("nan"), 0),
        Numbers(float("inf"), -1),
        Numbers(float("-inf"), 2**70),
        Numbers(1e-6, -(2**70)),
        Numbers(-0.0, 7),
    ]
    stream = StringIO()
    TsvWriter[Numbers](stream).write_all(values)
    found = list(TsvReader[Numbers](StringIO(stream.getvalue()), header=False))

    assert [repr(record) for record in found] == [repr(record) for record in values]


def test_reader_builds_records_with_their_init() -> None:
    """Test that records are built by their __init__, whatever kinds of field they have."""

    @dataclass(frozen=True)
    class Mixed:
        name: str
        count: int = field(kw_only=True)
        label: str = field(init=False, default="unset")
        ratio: float | None = None

        def __post_init__(self) -> None:
            object.__setattr__(self, "name", self.name.upper())

    reader = TsvReader[Mixed](StringIO("a\t1\tset\t0.5\n"), header=False)
    (record,) = list(reader)

    assert (record.name, record.count, record.label, record.ratio) == ("A", 1, "set", 0.5)


def test_reader_raises_an_error_of_init_as_it_is_with_a_note_of_its_line() -> None:
    """Test that an error raised building a record is raised as it is, noting its line."""

    @dataclass
    class Checked:
        count: int

        def __post_init__(self) -> None:
            if self.count < 0:
                raise ValueError("count must not be negative!")

    reader = TsvReader[Checked](StringIO("count\n1\n-1\n"))
    with pytest.raises(ValueError, match=r"^count must not be negative!") as error:
        _ = list(reader)

    assert error.value.__notes__ == ["Raised while building Checked on line 3."]


def test_reader_calls_the_init_a_record_type_defines_itself() -> None:
    """Test that a record type with its own __init__ is built by calling it."""

    @dataclass
    class Shouted:
        name: str

        def __init__(self, name: str) -> None:
            self.name = name.upper()

    assert list(TsvReader[Shouted](StringIO("quiet\n"), header=False)) == [Shouted("QUIET")]


def test_reader_calls_the_new_a_record_type_inherits() -> None:
    """Test that a record type whose __new__ is not object's, e.g. an exception, is built by it."""

    @dataclass
    class FailureError(Exception):
        code: int
        message: str

    @dataclass
    class Counted:
        value: int
        made: ClassVar[int] = 0

        def __new__(cls, *_args: object, **_kwargs: object) -> "Counted":
            Counted.made += 1
            return super().__new__(cls)

    (failure,) = list(TsvReader[FailureError](StringIO("1\tboom\n"), header=False))
    counted = list(TsvReader[Counted](StringIO("1\n2\n"), header=False))

    assert (failure.code, failure.message, failure.args, str(failure)) == (1, "boom", (), "")
    assert Counted.made == 2
    assert counted == [Counted(1), Counted(2)]


def test_reader_refuses_to_build_an_abstract_record_type() -> None:
    """Test that an abstract record type is refused as calling it would refuse it."""

    @dataclass
    class Shape(ABC):
        name: str

        @abstractmethod
        def area(self) -> float: ...

    reader: TsvReader[Any] = cast(Any, TsvReader)[Shape](StringIO("square\n"), header=False)
    with pytest.raises(TypeError, match=r"^Can't instantiate abstract class Shape") as error:
        _ = list(reader)

    assert error.value.__notes__ == ["Raised while building Shape on line 1."]


def test_reader_builds_a_record_with_an_init_var_through_its_init() -> None:
    """Test that a record whose __post_init__ takes an InitVar is built by its __init__."""

    @dataclass
    class Scaled:
        value: int
        scale: InitVar[int] = 10

        def __post_init__(self, scale: int) -> None:
            self.value *= scale

    (record,) = list(TsvReader[Scaled](StringIO("4\n"), header=False))

    assert record.value == 40


class Level(IntEnum):
    """A level, written as its integer value."""

    LOW = 1
    HIGH = 2


Count = NewType("Count", int)
"""A count, which is an integer."""


@dataclass
class Scalars:
    """A record of the scalar types the reader reads natively."""

    day: date
    moment: datetime
    clock: time
    key: UUID
    amount: Decimal
    level: Level
    rank: Literal[1, 2]
    flag: Literal[True]
    either: int | str
    number: int | float | None
    count: Count


def test_reader_reads_back_every_scalar_a_writer_writes() -> None:
    """Test that dates, times, UUIDs, decimals, enums, literals, and unions read back as written."""
    records = [
        Scalars(
            day=date(2021, 3, 4),
            moment=datetime(2021, 3, 4, 5, 6, 7, 123456, tzinfo=timezone.utc),
            clock=time(5, 6, 7),
            key=UUID("3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
            amount=Decimal("1.50"),
            level=Level.HIGH,
            rank=2,
            flag=True,
            either=5,
            number=1.5,
            count=Count(3),
        ),
        Scalars(
            day=date(1, 1, 1),
            moment=datetime(2021, 3, 4, 5, 6, 7, tzinfo=timezone(timedelta(hours=-5, minutes=-30))),
            clock=time(23, 59, 59, 999999, tzinfo=timezone(timedelta(hours=1))),
            key=UUID(int=0),
            amount=Decimal("-0.001"),
            level=Level.LOW,
            rank=1,
            flag=True,
            either="007",
            number=None,
            count=Count(-4),
        ),
    ]
    stream = StringIO()
    TsvWriter[Scalars](stream).write_all(records)

    assert list(TsvReader[Scalars](StringIO(stream.getvalue()), header=False)) == records


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2021-03-04T05:06:07", datetime(2021, 3, 4, 5, 6, 7)),
        ("2021-03-04 05:06:07z", datetime(2021, 3, 4, 5, 6, 7, tzinfo=timezone.utc)),
        ("2021-03-04T05:06:07-00:00", datetime(2021, 3, 4, 5, 6, 7, tzinfo=timezone.utc)),
        ("2021-03-04T05:06:07.1234567", datetime(2021, 3, 4, 5, 6, 7, 123457)),
        ("2021-03-04T23:59:59.9999995", datetime(2021, 3, 5)),
        (
            "2021-03-04T05:06:07+0530",
            datetime(2021, 3, 4, 5, 6, 7, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        ),
    ],
)
def test_reader_reads_rfc_3339_datetimes(text: str, expected: datetime) -> None:
    """Test that datetimes are read to the microsecond, rounding, with or without an offset."""

    @dataclass
    class Moment:
        moment: datetime

    (record,) = list(TsvReader[Moment](StringIO(f"{text}\n"), header=False))

    assert record.moment == expected
    assert record.moment.utcoffset() == expected.utcoffset()


@pytest.mark.parametrize(
    ("record_type", "text"),
    [
        (date, "2021-02-30"),
        (date, "0000-01-01"),
        (datetime, "2021-03-04"),
        (datetime, "2021-03-04T05:06:07[America/New_York]"),
        (time, "25:00:00"),
        (UUID, "3f2504e0-4f89-11d3-9a0c-0305e82c330"),
        (Decimal, "one"),
        (Level, "3"),
        (int | float, "x"),
    ],
)
def test_reader_names_a_scalar_it_cannot_read(record_type: Any, text: str) -> None:
    """Test that a scalar the reader cannot read raises an error naming its field and text."""
    record: Any = make_record(record_type)
    reader: TsvReader[Any] = cast(Any, TsvReader)[record](StringIO(""), header=False)
    message = rf"^Could not read field 'value' of type .* from text '{escape(text)}'!"
    with pytest.raises(ValueError, match=message):
        _ = reader.decode(f"{text}\n")


def make_record(field_type: Any) -> type[Any]:
    """Make a record type of one field, named value, of a type."""
    return make_dataclass("Record", [("value", field_type)])


def test_reader_reads_a_union_of_plain_types_as_its_first_type_that_fits() -> None:
    """Test that a union of plain types reads a boolean, then an integer, a float, and text."""

    @dataclass
    class Mixed:
        value: bool | int | float | str

    texts = ["true", "12", "-0", "1.5", "nan", "007", "x"]
    reader = TsvReader[Mixed](StringIO("".join(f"{text}\n" for text in texts)), header=False)
    values = [record.value for record in reader]

    assert [type(value) for value in values] == [bool, int, int, float, float, str, str]
    assert values[:4] == [True, 12, 0, 1.5]


def test_reader_reads_on_past_an_empty_line_of_an_iterable_inside_a_quoted_field() -> None:
    """Test that an empty line from an iterable of lines does not end a quoted field."""

    @dataclass
    class Pair:
        text: str
        other: str

    lines = iter(['"a\n', "", "", 'b"\tx\n'])
    reader = TsvReader[Pair](lines, header=False)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]

    assert list(reader) == [Pair("a\nb", "x")]


def test_reader_builds_a_record_with_its_own_setattr_through_its_init() -> None:
    """Test that a record type with its own __setattr__ sees each field it is built with."""

    @dataclass
    class Tidy:
        name: str

        @override
        def __setattr__(self, name: str, value: object) -> None:
            super().__setattr__(name, value.strip() if isinstance(value, str) else value)

    assert list(TsvReader[Tidy](StringIO("  padded  \n"), header=False)) == [Tidy("padded")]
