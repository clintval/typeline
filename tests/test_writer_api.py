"""Tests for how a writer is given its record type, checked at runtime and by static type checkers.

Every expected static error carries an ignore comment for mypy, pyright, and ty. Each checker is
configured to fail on unused ignore comments, so these comments assert the error is reported.
"""

from collections.abc import Callable
from dataclasses import dataclass
from io import StringIO
from os import linesep
from pathlib import Path
from typing import Any
from typing import TextIO

import pytest
from typing_extensions import Unpack
from typing_extensions import assert_type
from typing_extensions import override

from typeline import CsvWriter
from typeline import DelimitedDataWriter
from typeline import ExtraColumns
from typeline import FixedRecordType
from typeline import RecordType
from typeline import TsvWriter
from typeline import WriterOptions


@dataclass
class MyData:
    """A record type for testing."""

    field1: int
    field2: str | None


class MyCsvWriter(CsvWriter[RecordType]):
    """A custom comma-delimited writer."""


RECORD: MyData = MyData(field1=1, field2=None)


def test_from_path_subscripted_with_a_dataclass(tmp_path: Path) -> None:
    """Test that from_path subscripted with a dataclass writes records of that type."""
    with TsvWriter.from_path[MyData](tmp_path / "test.tsv") as tsv_writer:
        _ = assert_type(tsv_writer, TsvWriter[MyData])
        assert type(tsv_writer) is TsvWriter[MyData]
        tsv_writer.write_header()
        tsv_writer.write(RECORD)

    assert (tmp_path / "test.tsv").read_text() == "field1\tfield2\n1\t\n"

    with CsvWriter.from_path[MyData](tmp_path / "test.csv", none_field="NA") as csv_writer:
        _ = assert_type(csv_writer, CsvWriter[MyData])
        csv_writer.write(RECORD)

    assert (tmp_path / "test.csv").read_text() == "1,NA\n"


def test_from_path_on_a_generic_subclass(tmp_path: Path) -> None:
    """Test that from_path on a generic subclass builds that subclass, typed as its parent."""
    with MyCsvWriter.from_path[MyData](tmp_path / "test.csv") as writer:
        _ = assert_type(writer, CsvWriter[MyData])
        assert type(writer) is MyCsvWriter[MyData]
        writer.write(RECORD)

    assert (tmp_path / "test.csv").read_text() == "1,\n"


def test_constructor_subscripted_with_a_dataclass_writes_to_any_text_stream() -> None:
    """Test that the writer class subscripted with a dataclass writes to any text stream."""
    stream = StringIO()
    writer = TsvWriter[MyData](stream)
    _ = assert_type(writer, TsvWriter[MyData])
    writer.write(RECORD)
    assert stream.getvalue() == f"1\t{linesep}"


def test_subscripted_writer_classes_are_cached_and_keep_their_delimiter() -> None:
    """Test that a subscripted writer class is created once and keeps its delimiter."""
    assert CsvWriter[MyData] is CsvWriter[MyData]
    assert TsvWriter[MyData].delimiter == "\t"
    assert CsvWriter[MyData].__name__ == "CsvWriter[MyData]"


def test_subclass_of_a_subscripted_writer_keeps_its_record_type() -> None:
    """Test that a subclass of a subscripted writer constructs with the bound record type."""

    class MyDataWriter(CsvWriter[MyData]):
        """A writer dedicated to MyData records."""

    stream = StringIO()
    MyDataWriter(stream).write(RECORD)
    assert stream.getvalue() == f"1,{linesep}"


def test_from_path_uses_the_defaults_of_a_subclass(tmp_path: Path) -> None:
    """Test that from_path leaves options it is not given at the defaults of the writer's class."""

    class DotCsvWriter(CsvWriter[RecordType]):
        """A comma-delimited writer that writes None as a period by default."""

        @override
        def __init__(self, handle: TextIO, /, **options: Unpack[WriterOptions]) -> None:
            _ = options.setdefault("none_field", ".")
            super().__init__(handle, **options)

    with DotCsvWriter.from_path[MyData](tmp_path / "test.csv") as writer:
        writer.write(RECORD)

    assert (tmp_path / "test.csv").read_text() == "1,.\n"


def test_from_path_with_the_record_type_as_an_argument(tmp_path: Path) -> None:
    """Test that the record type can no longer be passed to from_path as an argument."""
    with pytest.raises(
        TypeError, match=r"^CsvWriter.from_path must be subscripted with a dataclass"
    ):
        CsvWriter.from_path(tmp_path / "test.csv", MyData)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]
    assert not (tmp_path / "test.csv").exists()


def test_constructor_with_the_record_type_as_an_argument() -> None:
    """Test that the record type can no longer be passed to the constructor as an argument."""
    with pytest.raises(TypeError, match=r"positional argument"):
        _ = TsvWriter[MyData](StringIO(), MyData)  # type: ignore[call-arg, arg-type]  # pyright: ignore[reportCallIssue]  # ty: ignore[too-many-positional-arguments]


def test_constructor_without_a_subscript() -> None:
    """Test that constructing a writer without a record type is refused."""
    with pytest.raises(TypeError, match=r"^TsvWriter must be subscripted with a dataclass"):
        unbound: type[TsvWriter[Any]] = TsvWriter
        _ = unbound(StringIO())


def test_from_path_on_a_subscripted_writer(tmp_path: Path) -> None:
    """Test that from_path through a writer class that already has a record type is refused."""
    message = r"^CsvWriter\[MyData\] already has a record type! Use CsvWriter.from_path\[MyData\]"
    with pytest.raises(TypeError, match=message):
        CsvWriter[MyData].from_path(tmp_path / "test.csv")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[no-matching-overload]


def test_from_path_subscripted_twice(tmp_path: Path) -> None:
    """Test that subscripting both the writer class and from_path is refused, only at runtime."""
    message = r"^CsvWriter\[MyData\] already has a record type! Use CsvWriter.from_path\[MyData\]"
    with pytest.raises(TypeError, match=message):
        _ = CsvWriter[MyData].from_path[MyData](tmp_path / "test.csv")


def test_from_path_with_an_unknown_keyword(tmp_path: Path) -> None:
    """Test that from_path keeps the signature of the classmethod it wraps."""
    with pytest.raises(TypeError, match=r"unexpected keyword argument 'nonefield'"):
        CsvWriter.from_path[MyData](tmp_path / "test.csv", nonefield=".")  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # ty: ignore[unknown-argument]


def test_from_path_subscripted_with_a_non_dataclass(tmp_path: Path) -> None:
    """Test that from_path subscripted with a non-dataclass is refused before creating the file."""
    message = r"^CsvWriter.from_path must be subscripted with a dataclass, not <class 'int'>!"
    with pytest.raises(TypeError, match=message):
        CsvWriter.from_path[int](tmp_path / "test.csv")  # type: ignore[type-var]  # pyright: ignore[reportCallIssue, reportArgumentType]  # ty: ignore[invalid-argument-type]
    assert not (tmp_path / "test.csv").exists()


def test_write_refuses_a_record_of_another_type() -> None:
    """Test that a writer refuses records that are not of its record type."""

    @dataclass
    class OtherData:
        other: int

    writer = CsvWriter[MyData](StringIO())
    with pytest.raises(ValueError, match=r"^Expected MyData but found OtherData!"):
        writer.write(OtherData(1))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]


class MyDataWriter(CsvWriter[MyData], FixedRecordType):
    """A comma-delimited writer fixed to one record type."""


def test_from_path_on_a_writer_with_a_fixed_record_type(tmp_path: Path) -> None:
    """Test that a writer fixed to one record type builds itself with an unsubscripted from_path."""
    with MyDataWriter.from_path(tmp_path / "test.csv", none_field="NA") as writer:
        _ = assert_type(writer, MyDataWriter)
        writer.write(RECORD)

    assert (tmp_path / "test.csv").read_text() == "1,NA\n"


def test_base_writer_without_a_delimiter() -> None:
    """Test that the base writer, which has no delimiter, is refused with a clear message."""
    with pytest.raises(TypeError, match=r"^DelimitedDataWriter has no delimiter! Subclass it"):
        _ = DelimitedDataWriter[MyData](StringIO())


def test_from_path_with_an_unknown_keyword_leaves_an_existing_file_alone(tmp_path: Path) -> None:
    """Test that from_path refuses unknown options before it opens, and so empties, the file."""
    _ = (tmp_path / "test.csv").write_text("keep me\n")
    with pytest.raises(TypeError, match=r"unexpected keyword argument 'nonefield'"):
        CsvWriter.from_path[MyData](tmp_path / "test.csv", nonefield=".")  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # ty: ignore[unknown-argument]
    assert (tmp_path / "test.csv").read_text() == "keep me\n"


@dataclass
class Misplaced:
    """A record whose ExtraColumns field is not its last field, which writers refuse."""

    extra: ExtraColumns
    field1: int


def open_with_a_bad_option_value(path: Path) -> object:
    """Open a writer with no comment prefixes, which writers refuse."""
    return CsvWriter.from_path[MyData](path, comment_prefixes=())


def open_with_a_bad_record_type(path: Path) -> object:
    """Open a writer for a record whose ExtraColumns field is not last, which writers refuse."""
    return CsvWriter.from_path[Misplaced](path)


def open_without_a_delimiter(path: Path) -> object:
    """Open the base writer, which has no delimiter."""
    return DelimitedDataWriter.from_path[MyData](path)


@pytest.mark.parametrize(
    "open_writer",
    [open_with_a_bad_option_value, open_with_a_bad_record_type, open_without_a_delimiter],
)
def test_from_path_leaves_an_existing_file_alone_when_the_writer_is_refused(
    tmp_path: Path, open_writer: Callable[[Path], object]
) -> None:
    """Test that from_path checks a writer fully before it opens, and so empties, a file."""
    path = tmp_path / "test.csv"
    _ = path.write_text("keep me\n")

    with pytest.raises((TypeError, ValueError)):
        _ = open_writer(path)

    assert path.read_text() == "keep me\n"


def test_from_path_with_an_unknown_keyword_through_a_subclass_leaves_a_file_alone(
    tmp_path: Path,
) -> None:
    """Test that an unknown option passed through a subclass's options is refused before opening."""

    class DotCsvWriter(CsvWriter[RecordType]):
        """A comma-delimited writer that writes None as a period by default."""

        @override
        def __init__(self, handle: TextIO, /, **options: Unpack[WriterOptions]) -> None:
            _ = options.setdefault("none_field", ".")
            super().__init__(handle, **options)

    path = tmp_path / "test.csv"
    _ = path.write_text("keep me\n")
    options: Any = {"nonefield": "."}
    with pytest.raises(TypeError, match=r"unexpected keyword argument 'nonefield'"):
        _ = DotCsvWriter.from_path[MyData](path, **options)

    assert path.read_text() == "keep me\n"
