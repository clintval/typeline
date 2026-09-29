from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Optional

import pytest

from typeline import CsvReader
from typeline import CsvWriter
from typeline import FieldCodec
from typeline import TsvReader
from typeline.codecs import delimited


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
    _ = path.write_text('numbers,names\n1|2,\'["a","b"]\'\n')

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
    _ = path.write_text("name,color\nfoo,.\nbar,'1,2,3'\n")

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
    _ = path.write_text("name,color\nfoo,'1,2,3'\nbar,purple\n")

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
    with CsvWriter.from_path(path, MyData, codecs=codecs) as writer:
        writer.write(MyData(Color(101, 2, 32), [1, 2, 3], ["a"]))

    assert path.read_text() == "'101,2,32',1|2|3|,[\"a\"]\n"


def test_writer_writes_none_as_the_none_field_before_a_codec(tmp_path: Path) -> None:
    """Test that the writer writes None as the none field even when a codec is registered."""

    @dataclass
    class MyData:
        color: Color | None

    path = tmp_path / "test.csv"
    with CsvWriter.from_path(path, MyData, codecs={Color: COLOR}, none_field=".") as writer:
        writer.write(MyData(None))
        writer.write(MyData(Color(1, 2, 3)))

    assert path.read_text() == ".\n'1,2,3'\n"


def test_writer_passes_nested_custom_types_to_the_enc_hook(tmp_path: Path) -> None:
    """Test that the writer encodes custom types nested in a field with the enc_hook."""

    @dataclass
    class MyData:
        intervals: list[Interval]

    path = tmp_path / "test.csv"
    with CsvWriter.from_path(path, MyData, enc_hook=enc_hook) as writer:
        writer.write(MyData([Interval(1, 2), Interval(3, 4)]))

    assert path.read_text() == "'[[1,2],[3,4]]'\n"


def test_writer_names_the_field_when_a_codec_fails(tmp_path: Path) -> None:
    """Test that a failing codec is reported with the field and its type."""

    @dataclass
    class MyData:
        color: Color

    def broken(_: Color) -> str:
        raise RuntimeError("no!")

    path = tmp_path / "test.csv"
    codecs = {Color: FieldCodec(from_text=Color.from_string, into_text=broken)}
    with CsvWriter.from_path(path, MyData, codecs=codecs) as writer:
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

    with CsvWriter.from_path(path, MyData, codecs=codecs) as writer:
        writer.write_header()
        for record in records:
            writer.write(record)

    with CsvReader.from_path[MyData](path, codecs=codecs, none_field="null") as reader:
        assert list(reader) == records
