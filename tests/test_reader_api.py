"""Tests for how a reader is given its record type, checked at runtime and by static type checkers.

Every expected static error carries an ignore comment for mypy, pyright, and ty. Each checker is
configured to fail on unused ignore comments, so these comments assert the error is reported.
"""

from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
from typing import Any

import pytest
from typing_extensions import assert_type

from typeline import CsvReader
from typeline import RecordType
from typeline import TsvReader


@dataclass
class MyData:
    """A record type for testing."""

    field1: int
    field2: str


class MyCsvReader(CsvReader[RecordType]):
    """A custom comma-delimited reader."""


EXPECTED: list[MyData] = [MyData(field1=1, field2="name")]


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    """A comma-delimited file holding one MyData record."""
    path = tmp_path / "test.csv"
    _ = path.write_text("field1,field2\n1,name\n")
    return path


@pytest.fixture
def tsv_path(tmp_path: Path) -> Path:
    """A tab-delimited file holding one MyData record."""
    path = tmp_path / "test.tsv"
    _ = path.write_text("field1\tfield2\n1\tname\n")
    return path


def test_from_path_subscripted_with_a_dataclass(csv_path: Path, tsv_path: Path) -> None:
    """Test that from_path subscripted with a dataclass reads records of that type."""
    with TsvReader.from_path[MyData](tsv_path) as tsv_reader:
        _ = assert_type(tsv_reader, TsvReader[MyData])
        assert type(tsv_reader) is TsvReader[MyData]
        assert list(tsv_reader) == EXPECTED

    with CsvReader.from_path[MyData](csv_path, header=True) as csv_reader:
        _ = assert_type(csv_reader, CsvReader[MyData])
        assert type(csv_reader) is CsvReader[MyData]
        for record in csv_reader:
            _ = assert_type(record, MyData)
            assert record == EXPECTED[0]


def test_from_path_on_a_generic_subclass(csv_path: Path) -> None:
    """Test that from_path on a generic subclass builds that subclass, typed as its parent."""
    with MyCsvReader.from_path[MyData](csv_path) as reader:
        _ = assert_type(reader, CsvReader[MyData])
        assert type(reader) is MyCsvReader[MyData]
        assert isinstance(reader, MyCsvReader)
        assert list(reader) == EXPECTED


def test_constructor_subscripted_with_a_dataclass(csv_path: Path) -> None:
    """Test that the reader class subscripted with a dataclass constructs from a handle."""
    with CsvReader[MyData](csv_path.open()) as reader:
        _ = assert_type(reader, CsvReader[MyData])
        assert list(reader) == EXPECTED


def test_subscripted_reader_classes_are_cached_and_keep_their_delimiter() -> None:
    """Test that a subscripted reader class is created once and keeps its delimiter."""
    assert CsvReader[MyData] is CsvReader[MyData]
    assert TsvReader[MyData].delimiter == "\t"
    assert CsvReader[MyData].__name__ == "CsvReader[MyData]"


def test_subclass_of_a_subscripted_reader_keeps_its_record_type(csv_path: Path) -> None:
    """Test that a subclass of a subscripted reader constructs with the bound record type."""

    class MyDataReader(CsvReader[MyData]):
        """A reader dedicated to MyData records."""

    with MyDataReader(csv_path.open()) as reader:
        assert list(reader) == EXPECTED


def test_from_path_keeps_the_name_and_docs_of_the_classmethod() -> None:
    """Test that the subscriptable from_path still looks like the classmethod it wraps."""
    assert TsvReader.from_path.__name__ == "from_path"
    assert TsvReader.from_path.__doc__ is not None
    assert TsvReader.from_path.__doc__.startswith(
        "Construct a delimited data reader from a file path."
    )


def test_from_path_without_a_subscript(csv_path: Path) -> None:
    """Test that from_path without a record type is refused."""
    with pytest.raises(
        TypeError, match=r"^CsvReader.from_path must be subscripted with a dataclass"
    ):
        CsvReader.from_path(csv_path)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]


def test_from_path_on_a_subscripted_reader(csv_path: Path) -> None:
    """Test that from_path through a reader class that already has a record type is refused."""
    message = r"^CsvReader\[MyData\] already has a record type! Use CsvReader.from_path\[MyData\]"
    with pytest.raises(TypeError, match=message):
        CsvReader[MyData].from_path(csv_path)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]


def test_from_path_subscripted_twice(csv_path: Path) -> None:
    """Test that subscripting both the reader class and from_path is refused, only at runtime."""
    message = r"^CsvReader\[MyData\] already has a record type! Use CsvReader.from_path\[MyData\]"
    with pytest.raises(TypeError, match=message):
        _ = CsvReader[MyData].from_path[MyData](csv_path)


def test_from_path_with_an_unknown_keyword(csv_path: Path) -> None:
    """Test that from_path keeps the signature of the classmethod it wraps."""
    with pytest.raises(TypeError, match=r"unexpected keyword argument 'headr'"):
        CsvReader.from_path[MyData](csv_path, headr=True)  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # ty: ignore[unknown-argument]


def test_from_path_subscripted_with_a_non_dataclass(tmp_path: Path) -> None:
    """Test that from_path subscripted with a non-dataclass is refused before opening the file."""
    missing = tmp_path / "does-not-exist.csv"
    message = r"^CsvReader.from_path must be subscripted with a dataclass, not <class 'int'>!"
    with pytest.raises(TypeError, match=message):
        CsvReader.from_path[int](missing)  # type: ignore[type-var]  # pyright: ignore[reportCallIssue, reportArgumentType]  # ty: ignore[invalid-argument-type]


def test_constructor_without_a_subscript(csv_path: Path) -> None:
    """Test that constructing a reader without a record type is refused."""
    with (
        csv_path.open() as handle,
        pytest.raises(TypeError, match=r"^CsvReader must be subscripted"),
    ):
        _ = CsvReader(handle)


def test_from_path_closes_the_file_when_the_reader_cannot_be_built(
    csv_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that from_path closes the file it opened when constructing the reader fails."""
    opened: list[TextIOWrapper] = []
    original_open = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original_open(self, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", recording_open)

    @dataclass
    class OtherData:
        other: int

    with pytest.raises(ValueError, match=r"^Fields of header do not match fields of dataclass!"):
        _ = CsvReader.from_path[OtherData](csv_path)

    assert len(opened) == 1
    assert opened[0].closed
