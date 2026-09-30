from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Annotated
from typing import Any
from typing import Optional

import pytest

from typeline import Codecs
from typeline import CsvReader
from typeline import CsvWriter
from typeline import FieldCodec
from typeline import TsvReader
from typeline.codecs import boolean
from typeline.codecs import delimited
from typeline.codecs import key_value
from typeline.codecs import nullable


@dataclass(frozen=True)
class Color:
    """A custom type with its own text format, e.g. `101,2,32`."""

    r: int
    g: int
    b: int

    @classmethod
    def from_string(cls, text: str) -> "Color":
        """Read a color from its text, e.g. `101,2,32`."""
        r, g, b = text.split(",")
        return cls(int(r), int(g), int(b))

    def __str__(self) -> str:
        """Write a color into its text, e.g. `101,2,32`."""
        return f"{self.r},{self.g},{self.b}"


class Interval:
    """A custom type that msgspec cannot convert on its own."""

    def __init__(self, start: int, end: int) -> None:
        """Build an interval from its start and end."""
        self.start = start
        self.end = end

    def __eq__(self, other: object) -> bool:
        """Compare intervals by their start and end."""
        return isinstance(other, Interval) and (self.start, self.end) == (other.start, other.end)

    def __hash__(self) -> int:
        """Hash an interval by its start and end."""
        return hash((self.start, self.end))


COLOR: FieldCodec[Color] = FieldCodec(from_text=Color.from_string, into_text=str)


def dec_hook(type_: type, obj: Any) -> Any:
    """Decode an Interval from a two-element list."""
    if type_ is Interval:
        return Interval(*obj)
    raise NotImplementedError(type_)


def enc_hook(obj: Any) -> Any:
    """Encode an Interval into a two-element list."""
    if isinstance(obj, Interval):
        return [obj.start, obj.end]
    raise NotImplementedError(type(obj))


@pytest.mark.parametrize(
    "codec,text,value",
    [
        pytest.param(delimited(int), "1,2,3", [1, 2, 3], id="list"),
        pytest.param(delimited(int), "", [], id="empty"),
        pytest.param(delimited(int, trailing_sep=True), "1,2,", [1, 2], id="trailing-sep"),
        pytest.param(delimited(str, sep="|"), "a|b", ["a", "b"], id="custom-sep"),
        pytest.param(delimited(int, container=tuple), "1,2", (1, 2), id="tuple"),
        pytest.param(
            delimited(Color.from_string, sep=";"),
            "1,2,3;4,5,6",
            [Color(1, 2, 3), Color(4, 5, 6)],
            id="custom-items",
        ),
    ],
)
def test_delimited_codec_round_trips(codec: FieldCodec[Any], text: str, value: Any) -> None:
    """Test that a delimited codec reads text into a container and writes it back."""
    assert codec.from_text(text) == value
    assert codec.into_text(value) == text


@pytest.mark.parametrize("trailing_sep", [True, False])
def test_delimited_codec_reads_an_optional_trailing_separator(trailing_sep: bool) -> None:
    """Test that a delimited codec reads text whether or not it ends with a separator."""
    codec = delimited(int, trailing_sep=trailing_sep)
    assert codec.from_text("1,2,") == [1, 2]
    assert codec.from_text("1,2") == [1, 2]


@pytest.mark.parametrize(
    "codec,text,value",
    [
        pytest.param(key_value(), "ID=g1;Name=TP53", {"ID": "g1", "Name": "TP53"}, id="gff3"),
        pytest.param(key_value(), "", {}, id="empty"),
        pytest.param(
            key_value(value=float), "DP=10.0;AF=0.5", {"DP": 10.0, "AF": 0.5}, id="values"
        ),
        pytest.param(key_value(sep=",", assign=":"), "a:1,b:2", {"a": "1", "b": "2"}, id="custom"),
        pytest.param(boolean(), "Y", True, id="yes"),
        pytest.param(boolean(), "N", False, id="no"),
        pytest.param(boolean(true="1", false="0"), "1", True, id="one"),
        pytest.param(nullable(COLOR, missing="0"), "0", None, id="missing"),
        pytest.param(nullable(COLOR, missing="0"), "1,2,3", Color(1, 2, 3), id="present"),
    ],
)
def test_helper_codecs_round_trip(codec: FieldCodec[Any], text: str, value: Any) -> None:
    """Test that the helper codecs read text into a value and write it back."""
    assert codec.from_text(text) == value
    assert codec.into_text(value) == text


def test_key_value_codec_reads_a_trailing_separator() -> None:
    """Test that a key-value codec reads text that ends with a separator, as GFF3 allows."""
    assert key_value().from_text("ID=g1;Name=TP53;") == {"ID": "g1", "Name": "TP53"}


def test_key_value_codec_refuses_an_item_without_a_value() -> None:
    """Test that a key-value codec refuses an item with no assignment."""
    with pytest.raises(ValueError, match=r"^Expected key=value but found 'lonely'!"):
        _ = key_value().from_text("ID=g1;lonely")


def test_boolean_codec_refuses_other_text() -> None:
    """Test that a boolean codec refuses text that is neither of its two values."""
    with pytest.raises(ValueError, match=r"^Expected 'Y' or 'N' but found 'yes'!"):
        _ = boolean().from_text("yes")


def test_codecs_alias_types_a_mapping_of_different_codecs(tmp_path: Path) -> None:
    """Test that the Codecs alias holds codecs of different value types for one reader."""

    @dataclass
    class MyData:
        color: Color
        flag: bool

    codecs: Codecs = {Color: COLOR, bool: boolean()}
    path = tmp_path / "test.csv"
    _ = path.write_text('"1,2,3",Y\n')

    with CsvReader.from_path[MyData](path, header=False, codecs=codecs) as reader:
        assert list(reader) == [MyData(Color(1, 2, 3), True)]


def test_reader_uses_a_codec_registered_for_the_field_type(tmp_path: Path) -> None:
    """Test that the reader reads a field with the codec registered for its type."""

    @dataclass
    class MyData:
        color: Color
        blocks: list[int]

    path = tmp_path / "test.tsv"
    _ = path.write_text("color\tblocks\n101,2,32\t1|2|3|\n")

    codecs = {Color: COLOR, list[int]: delimited(int, sep="|", trailing_sep=True)}
    with TsvReader.from_path[MyData](path, codecs=codecs) as reader:
        assert list(reader) == [MyData(Color(101, 2, 32), [1, 2, 3])]


def test_reader_matches_codecs_by_exact_field_type(tmp_path: Path) -> None:
    """Test that a codec for one type is not used for a different field type."""

    @dataclass
    class MyData:
        numbers: list[int]
        names: list[str]

    path = tmp_path / "test.csv"
    _ = path.write_text('numbers,names\n1|2,"[""a"",""b""]"\n')

    with CsvReader.from_path[MyData](path, codecs={list[int]: delimited(int, sep="|")}) as reader:
        assert list(reader) == [MyData([1, 2], ["a", "b"])]


@dataclass
class NewStyleOptional:
    """A record with an optional field written with `|`."""

    name: str
    color: Color | None


@dataclass
class OldStyleOptional:
    """A record with an optional field written with `Optional`."""

    name: str
    color: Optional[Color]  # noqa: UP045


@pytest.mark.parametrize("record_type", [NewStyleOptional, OldStyleOptional])
def test_reader_uses_a_codec_for_an_optional_field(
    tmp_path: Path, record_type: type[NewStyleOptional] | type[OldStyleOptional]
) -> None:
    """Test that an optional field reads the none field as None and other text with the codec."""
    path = tmp_path / "test.csv"
    _ = path.write_text('name,color\nfoo,.\nbar,"1,2,3"\n')

    with CsvReader.from_path[record_type](path, codecs={Color: COLOR}, none_field=".") as reader:
        assert list(reader) == [record_type("foo", None), record_type("bar", Color(1, 2, 3))]


def test_reader_passes_nested_custom_types_to_the_dec_hook(tmp_path: Path) -> None:
    """Test that the reader decodes custom types nested in a field with the dec_hook."""

    @dataclass
    class MyData:
        intervals: list[Interval]

    path = tmp_path / "test.tsv"
    _ = path.write_text("intervals\n[[1,2],[3,4]]\n")

    with TsvReader.from_path[MyData](path, dec_hook=dec_hook) as reader:
        assert list(reader) == [MyData([Interval(1, 2), Interval(3, 4)])]


def test_reader_names_the_field_when_a_codec_fails(tmp_path: Path) -> None:
    """Test that a failing codec is reported with the line, field, type, and text."""

    @dataclass
    class MyData:
        name: str
        color: Color

    path = tmp_path / "test.csv"
    _ = path.write_text('name,color\nfoo,"1,2,3"\nbar,purple\n')

    with CsvReader.from_path[MyData](path, codecs={Color: COLOR}) as reader:
        message = r"^Could not read field 'color' of type Color from text 'purple' on line 3!"
        with pytest.raises(ValueError, match=message) as error:
            _ = list(reader)
    assert isinstance(error.value.__cause__, ValueError)


def test_writer_uses_a_codec_registered_for_the_field_type(tmp_path: Path) -> None:
    """Test that the writer writes a field with the codec registered for its declared type."""

    @dataclass
    class MyData:
        color: Color
        blocks: list[int]
        names: list[str]

    path = tmp_path / "test.csv"
    codecs = {Color: COLOR, list[int]: delimited(int, sep="|", trailing_sep=True)}
    with CsvWriter.from_path[MyData](path, codecs=codecs) as writer:
        writer.write(MyData(Color(101, 2, 32), [1, 2, 3], ["a"]))

    assert path.read_text() == '"101,2,32",1|2|3|,"[""a""]"\n'


def test_writer_writes_none_as_the_none_field_before_a_codec(tmp_path: Path) -> None:
    """Test that the writer writes None as the none field even when a codec is registered."""

    @dataclass
    class MyData:
        color: Color | None

    path = tmp_path / "test.csv"
    with CsvWriter.from_path[MyData](path, codecs={Color: COLOR}, none_field=".") as writer:
        writer.write(MyData(None))
        writer.write(MyData(Color(1, 2, 3)))

    assert path.read_text() == '.\n"1,2,3"\n'


def test_writer_passes_nested_custom_types_to_the_enc_hook(tmp_path: Path) -> None:
    """Test that the writer encodes custom types nested in a field with the enc_hook."""

    @dataclass
    class MyData:
        intervals: list[Interval]

    path = tmp_path / "test.csv"
    with CsvWriter.from_path[MyData](path, enc_hook=enc_hook) as writer:
        writer.write(MyData([Interval(1, 2), Interval(3, 4)]))

    assert path.read_text() == '"[[1,2],[3,4]]"\n'


def test_writer_names_the_field_when_a_codec_fails(tmp_path: Path) -> None:
    """Test that a failing codec is reported with the field and its type."""

    @dataclass
    class MyData:
        color: Color

    def broken(_: Color) -> str:
        raise RuntimeError("no!")

    path = tmp_path / "test.csv"
    codecs = {Color: FieldCodec(from_text=Color.from_string, into_text=broken)}
    with CsvWriter.from_path[MyData](path, codecs=codecs) as writer:
        message = r"^Could not write field 'color' of type Color!"
        with pytest.raises(ValueError, match=message) as error:
            writer.write(MyData(Color(1, 2, 3)))
    assert isinstance(error.value.__cause__, RuntimeError)


def test_codecs_round_trip_through_a_file(tmp_path: Path) -> None:
    """Test that records written with codecs are read back unchanged with the same codecs."""

    @dataclass
    class MyData:
        color: Color | None
        blocks: list[int]

    path = tmp_path / "test.csv"
    codecs = {Color: COLOR, list[int]: delimited(int, trailing_sep=True)}
    records = [MyData(Color(1, 2, 3), [1, 2]), MyData(None, [])]

    with CsvWriter.from_path[MyData](path, codecs=codecs) as writer:
        writer.write_header()
        for record in records:
            writer.write(record)

    with CsvReader.from_path[MyData](path, codecs=codecs) as reader:
        assert list(reader) == records


@dataclass
class PostponedColor:
    """A record whose field annotation is a string, as with postponed annotations."""

    color: "Color | None"  # noqa: UP037


def test_codecs_are_found_for_postponed_annotations(tmp_path: Path) -> None:
    """Test that codecs are found for fields whose annotations are strings."""
    path = tmp_path / "test.csv"
    with CsvWriter.from_path[PostponedColor](path, codecs={Color: COLOR}) as writer:
        writer.write(PostponedColor(Color(1, 2, 3)))

    assert path.read_text() == '"1,2,3"\n'

    with CsvReader.from_path[PostponedColor](path, header=False, codecs={Color: COLOR}) as reader:
        assert list(reader) == [PostponedColor(Color(1, 2, 3))]


@dataclass
class OptionalColor:
    """A record with an optional color, where a codec decides how a missing color looks."""

    name: str
    color: Color | None


def test_nullable_codec_writes_its_missing_marker_for_none(tmp_path: Path) -> None:
    """Test that a nullable codec writes None as its own missing marker, not the none field."""
    codecs: Codecs = {Color: nullable(COLOR, missing="0")}
    path = tmp_path / "test.csv"

    with CsvWriter.from_path[OptionalColor](path, codecs=codecs, none_field=".") as writer:
        writer.write(OptionalColor("foo", None))
        writer.write(OptionalColor("bar", Color(1, 2, 3)))

    assert path.read_text() == 'foo,0\nbar,"1,2,3"\n'

    with CsvReader.from_path[OptionalColor](path, header=False, codecs=codecs) as reader:
        assert list(reader) == [OptionalColor("foo", None), OptionalColor("bar", Color(1, 2, 3))]


UPPER: FieldCodec[str] = FieldCodec(from_text=str.lower, into_text=str.upper)


@dataclass
class Shouting:
    """A record where only the annotated fields are written in upper case."""

    loud: Annotated[str, "upper"]
    maybe_loud: Annotated[str, "upper"] | None
    quiet: str


def test_codecs_are_found_by_annotated_type(tmp_path: Path) -> None:
    """Test that a codec keyed on an Annotated type applies only to fields annotated that way."""
    codecs: Codecs = {Annotated[str, "upper"]: UPPER}
    path = tmp_path / "test.csv"

    with CsvWriter.from_path[Shouting](path, codecs=codecs) as writer:
        writer.write(Shouting("hi", "hey", "hello"))

    assert path.read_text() == "HI,HEY,hello\n"

    with CsvReader.from_path[Shouting](path, header=False, codecs=codecs) as reader:
        assert list(reader) == [Shouting("hi", "hey", "hello")]


def test_codecs_for_the_plain_type_apply_to_annotated_fields(tmp_path: Path) -> None:
    """Test that a codec keyed on a plain type still applies to fields that annotate that type."""
    path = tmp_path / "test.csv"
    _ = path.write_text("HI,HEY,HELLO\n")

    with CsvReader.from_path[Shouting](path, header=False, codecs={str: UPPER}) as reader:
        assert list(reader) == [Shouting("hi", "hey", "hello")]


@dataclass(frozen=True)
class Blocks:
    """A record with optional blocks, where a codec decides how missing blocks look."""

    blocks: list[int] | None


def test_a_codec_missing_marker_replaces_the_none_field_on_read() -> None:
    """Test that with a codec's missing marker, the none field is ordinary text for that codec."""
    codecs: Codecs = {list[int]: nullable(delimited(int), missing="NA")}
    handle = StringIO()
    writer = CsvWriter[Blocks](handle, codecs=codecs)
    for record in (Blocks([]), Blocks(None), Blocks([1, 2])):
        writer.write(record)

    assert handle.getvalue() == '""\nNA\n"1,2"\n'
    with CsvReader[Blocks](StringIO(handle.getvalue()), header=False, codecs=codecs) as reader:
        assert list(reader) == [Blocks([]), Blocks(None), Blocks([1, 2])]


@pytest.mark.parametrize("items", [["a", ""], [""]])
@pytest.mark.parametrize("trailing_sep", [False, True])
def test_delimited_refuses_an_empty_last_item(items: list[str], trailing_sep: bool) -> None:
    """Test that an empty last item, which would read back as no item, is refused."""
    codec = delimited(str, trailing_sep=trailing_sep)

    with pytest.raises(
        ValueError, match=r"^Cannot write an empty last item, which reads back as none!$"
    ):
        _ = codec.into_text(items)


def test_delimited_writes_empty_items_before_the_last() -> None:
    """Test that empty items before the last are written and read back."""
    codec = delimited(str)

    assert codec.into_text(["", "a"]) == ",a"
    assert codec.from_text(",a") == ["", "a"]
