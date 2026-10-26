from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any
from typing import TextIO

import pytest
from typing_extensions import Unpack
from typing_extensions import assert_type
from typing_extensions import override

from typeline import CsvReader
from typeline import CsvWriter
from typeline import DelimitedDataReader
from typeline import DelimitedDataWriter
from typeline import ReaderOptions
from typeline import RecordType
from typeline import TsvReader
from typeline import TsvWriter
from typeline import WriterOptions


@dataclass
class MyData:
    """A record type for testing."""

    field1: int
    field2: str


class RecordTypeAtStart(TsvReader[MyData]):
    """A reader that reads its record type before its parent is initialized."""

    @override
    def __init__(self, handle: TextIO, /, **options: Unpack[ReaderOptions]) -> None:
        self.seen: type[MyData] = self.record_type
        super().__init__(handle, **options)


class WriterRecordTypeAtStart(TsvWriter[MyData]):
    """A writer that reads its record type before its parent is initialized."""

    @override
    def __init__(self, handle: TextIO, /, **options: Unpack[WriterOptions]) -> None:
        self.seen: type[MyData] = self.record_type
        super().__init__(handle, **options)


class UnboundReader(TsvReader[RecordType]):
    """A generic reader subclass left without a record type."""


def test_reader_record_type_when_subscripted() -> None:
    """Test that a reader subscripted directly reports its record type."""
    with TsvReader[MyData](StringIO()) as reader:
        _ = assert_type(reader.record_type, type[MyData])
        assert reader.record_type is MyData

    with CsvReader[MyData](StringIO()) as reader:
        assert reader.record_type is MyData


def test_writer_record_type_when_subscripted() -> None:
    """Test that a writer subscripted directly reports its record type."""
    with TsvWriter[MyData](StringIO()) as writer:
        _ = assert_type(writer.record_type, type[MyData])
        assert writer.record_type is MyData

    with CsvWriter[MyData](StringIO()) as writer:
        assert writer.record_type is MyData


def test_record_type_from_path(tmp_path: Path) -> None:
    """Test that a reader and a writer made by from_path report their record type."""
    path = tmp_path / "test.tsv"

    with TsvWriter.from_path[MyData](path) as writer:
        _ = assert_type(writer.record_type, type[MyData])
        assert writer.record_type is MyData

    with TsvReader.from_path[MyData](path) as reader:
        _ = assert_type(reader.record_type, type[MyData])
        assert reader.record_type is MyData


def test_record_type_through_the_base_classes() -> None:
    """Test that the base reader and writer classes report their record type."""
    reader: DelimitedDataReader[MyData] = TsvReader[MyData](StringIO())
    writer: DelimitedDataWriter[MyData] = TsvWriter[MyData](StringIO())

    _ = assert_type(reader.record_type, type[MyData])
    _ = assert_type(writer.record_type, type[MyData])
    assert reader.record_type is writer.record_type is MyData


def test_record_type_in_a_subclass_before_its_parent_is_initialized() -> None:
    """Test that a subclass reads its record type in its own __init__ before super().__init__."""
    assert RecordTypeAtStart(StringIO()).seen is MyData
    assert WriterRecordTypeAtStart(StringIO()).seen is MyData


def test_record_type_of_a_user_subclass() -> None:
    """Test that a user subclass of a reader reports the record type it was subscripted with."""
    assert RecordTypeAtStart(StringIO()).record_type is MyData


def test_an_unsubscripted_subclass_still_refuses_to_be_built() -> None:
    """Test that a subclass without a record type raises the existing error when built."""
    unbound: Any = UnboundReader
    with pytest.raises(TypeError, match=r"^UnboundReader must be subscripted with a dataclass"):
        _ = unbound(StringIO())
