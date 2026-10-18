from collections import Counter
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import AbstractContextManager
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from enum import Enum
from enum import Flag
from inspect import Parameter
from inspect import isabstract
from inspect import signature
from itertools import islice
from pathlib import Path
from types import NoneType
from types import TracebackType
from types import UnionType
from typing import Any
from typing import Callable
from typing import Generic
from typing import Literal
from typing import TextIO
from typing import TypeAlias
from typing import Union  # pyright: ignore[reportDeprecated]
from typing import cast
from typing import get_args
from typing import get_origin
from uuid import UUID

from msgspec.json import Decoder as JSONDecoder
from typing_extensions import Self
from typing_extensions import TypedDict
from typing_extensions import Unpack
from typing_extensions import override

from ._binding import DelimitedData
from ._binding import SubscriptableClassmethod
from ._columns import NO_RENAME
from ._columns import in_column
from ._columns import name_columns
from ._comment import Comment
from ._comment import comment_prefixes_of
from ._counter_columns import CounterFields
from ._data_types import RecordType
from ._data_types import accepts_none
from ._data_types import extra_columns_field
from ._data_types import field_types
from ._data_types import find_codec
from ._data_types import is_text
from ._data_types import strip_optional
from ._data_types import type_name
from ._files import on_line
from ._files import open_for_reading
from ._native import RecordBuilder
from ._native import RowReader
from .codecs import NO_CODECS
from .codecs import Codecs

DEFAULT_COMMENT_PREFIXES: tuple[str, ...] = ("#",)
"""The default line prefixes that will tell the reader to skip those lines."""

NATIVE_LANES: dict[type, str] = {
    bool: "bool",
    date: "date",
    datetime: "datetime",
    Decimal: "decimal",
    float: "float",
    int: "int",
    str: "text",
    time: "time",
    UUID: "uuid",
}
"""The types the native engine reads itself, and the lane it reads each in."""

PLAIN_LANES: frozenset[str] = frozenset({"bool", "float", "int", "text"})
"""The lanes of the plain types a union of them may hold."""

BY_KEYWORD: frozenset[Any] = frozenset({Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY})
"""The kinds of parameter a field may be passed to by keyword."""

FieldSpec: TypeAlias = tuple[str, int, str, str | None, Any, str, str]
"""How the native engine reads a field: its name, its column, its lane and the details of the
lane, its text for None, and how the field and its column are named in errors."""


class ReaderOptions(TypedDict, total=False, closed=True):
    """The options of a delimited data reader."""

    header: bool
    """Whether the first record is a header, whose columns are matched to fields by name."""

    rename: Mapping[str, str]
    """A new column name for each field, by field name; a field not named here keeps its own."""

    comment_prefixes: Collection[str]
    """Skip lines that start with any of these prefixes, between records."""

    none_field: str
    """The text read as None in fields that allow None; a `str` field keeps it as text."""

    codecs: Codecs
    """How to read a field from its text, by the field's type."""

    dec_hook: Callable[[type, Any], Any] | None
    """Decode custom types anywhere in a record, with the semantics of msgspec's `dec_hook`."""

    on_comment: Callable[[Comment], None] | None
    """Receive each comment line as it is skipped; comments are dropped if None."""

    quoting: bool
    """Whether `"` quotes fields, as in CSV, or is ordinary text, for formats without quoting."""


class DelimitedDataReader(
    DelimitedData,
    AbstractContextManager["DelimitedDataReader[RecordType]"],
    Iterable[RecordType],
    Generic[RecordType],
):
    """A reader for reading delimited text data into dataclasses."""

    def __init__(
        self,
        handle: TextIO,
        /,
        *,
        header: bool = True,
        rename: Mapping[str, str] = NO_RENAME,
        comment_prefixes: Collection[str] = DEFAULT_COMMENT_PREFIXES,
        none_field: str = "",
        codecs: Codecs = NO_CODECS,
        dec_hook: Callable[[type, Any], Any] | None = None,
        on_comment: Callable[[Comment], None] | None = None,
        quoting: bool = True,
    ):
        """Instantiate a new delimited data reader.

        Args:
            handle: a file-like object to read delimited data from.
            header: whether the first record is a header, matched to fields by name.
            rename: a new name for the column of each field, by field name; a header is matched
                to these names, which have no effect without one.
            comment_prefixes: skip lines that start with any of these prefixes, between records.
            none_field: the text read as None in fields that allow None; a `str` field keeps it.
            codecs: how to read a field from its text, by the field's type.
            dec_hook: decode custom types anywhere in a record, like msgspec's `dec_hook`.
            on_comment: receive each comment line as it is skipped; comments are dropped if None.
            quoting: whether `"` quotes fields, or is ordinary text.
        """
        record_type = cast(type[RecordType], self._bound_record_type())

        # Initialize and save internal attributes of this class.
        self._handle: TextIO = handle
        name = getattr(handle, "name", None)
        self._file_name: str | None = name if isinstance(name, str) else None
        prefixes = comment_prefixes_of(comment_prefixes)
        self._none_field: str = none_field
        self._dec_hook: Callable[[type, Any], Any] | None = dec_hook

        # Inspect the record type, and decide once how each field is read from its text.
        self._field_type_map: dict[str, Any] = field_types(record_type)
        self._extra_field: str | None = extra_columns_field(record_type, self._field_type_map)
        self._counters: CounterFields = CounterFields(record_type, self._field_type_map)
        self._column_of: dict[str, str] = name_columns(
            record_type, self._field_type_map, self._extra_field, self._counters, rename
        )
        expected: list[str] = [
            column
            for name in self._field_type_map
            if name != self._extra_field
            for column in self._counters.columns_of(name, self._column_of)
        ]

        # Read rows natively, skipping blank and comment lines between records.
        self._rows: RowReader = RowReader(
            handle,
            self.delimiter,
            quoting,
            list(prefixes),
            None if on_comment is None else lambda line, text: on_comment(Comment(line, text)),
        )

        # Match a header's columns to fields by name, so the columns may come in any order.
        found: list[str] | None = None
        at: int | None = None
        if header and (first := self._rows.read_row()) is not None:
            found, at = first
        columns: set[str] = set(expected)
        if found is not None:
            repeated: list[str] = sorted(
                name for name, count in Counter(found).items() if count > 1 and name in columns
            )
            if repeated:
                raise ValueError(
                    f"Columns of header repeat a name{on_line(at, self._file_name)}!"
                    + f" Header: {found}. Repeated in header: {repeated}."
                )
            present: set[str] = set(found)
            missing: list[str] = [name for name in expected if name not in present]
            unexpected: list[str] = [name for name in found if name not in columns]
            if missing or (unexpected and self._extra_field is None):
                raise ValueError(
                    f"Columns of header do not match fields of {record_type.__name__}"
                    + f"{on_line(at, self._file_name)}! Header: {found}."
                    + f" Columns of {record_type.__name__}: {expected}."
                    + (f" Missing from header: {missing}." if missing else "")
                    + (f" Unexpected in header: {unexpected}." if unexpected else "")
                )
        layout: list[str] = expected if found is None else found
        index_of: dict[str, int] = {name: index for index, name in enumerate(layout)}
        builder = RecordBuilder(
            record_type,
            [
                self._field_spec(name, index_of[column], codecs)
                for name, column in self._column_of.items()
            ],
            init_fields(record_type, list(self._field_type_map)),
            len(layout),
            None
            if self._extra_field is None
            else (
                self._extra_field,
                [index for index, name in enumerate(layout) if name not in columns],
            ),
            self._counters.locate(layout),
            self._file_name,
        )
        self._rows.bind(builder)

    @override
    def __enter__(self) -> Self:
        """Enter this context."""
        _ = super().__enter__()
        return self

    @override
    def __exit__(  # pyright: ignore[reportMissingSuperCall]
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        """Exit this context while closing all open resources."""
        self.close()
        return None

    def _field_spec(self, name: str, column: int, codecs: Codecs) -> FieldSpec:
        """Decide how the native engine reads a field from the text of its column."""
        field_type = self._field_type_map[name]
        kind = supertype(strip_optional(field_type))
        label = f"field '{name}' of type {type_name(field_type)}"
        place = in_column(name, self._column_of[name])
        none = self._none_field if accepts_none(field_type) else None
        codec = find_codec(field_type, codecs)
        if codec is not None:
            missing = none if none is None or codec.missing is None else codec.missing
            return (name, column, "codec", missing, codec.from_text, label, place)
        lane = next((lane for t, lane in NATIVE_LANES.items() if t is kind), None)
        if lane is not None:
            return (name, column, lane, none, None, label, place)
        members = plain_members(kind)
        if members is not None:
            return (name, column, "union", none, members, label, place)
        table = lookup_table(kind)
        if table is not None:
            return (name, column, "lookup", none, table, label, place)
        try:
            decode = JSONDecoder(field_type, strict=False, dec_hook=self._dec_hook).decode
        except TypeError as exception:
            raise TypeError(f"Cannot read {label}, which msgspec cannot decode!") from exception
        return (name, column, "decode", none, (decode, not is_text(field_type)), label, place)

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        return self._rows

    def decode(self, line: str) -> RecordType:
        """Decode one record from a line of text, just as this reader reads records from its file.

        The line may end with a line break, and a quoted field may hold line breaks.
        Nothing is read from the reader's handle, so decoding may be mixed with reading records.

        Args:
            line: the text of one record.

        Raises:
            ValueError: if the line is blank, is a comment, holds more than one record, or ends
                inside a quoted field, or if its fields cannot be read into a record.
        """
        return cast(RecordType, self._rows.decode(line))

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()

    @SubscriptableClassmethod
    @classmethod
    def from_path(cls, path: Path | str, /, **options: Unpack[ReaderOptions]) -> Self:
        """Construct a delimited data reader from a file path.

        The file is read as UTF-8, decompressed when it is gzip, bzip2, or xz, and closed once read
        to the end or when a record fails.

        Args:
            path: the path to the file to read delimited data from.
            options: the options of the reader, left at the reader's defaults when not given.
        """
        handle = open_for_reading(path)
        try:
            reader = cls(handle, **options)
        except BaseException:
            handle.close()
            raise
        reader._rows.close_when_read = True
        return reader


class CsvReader(DelimitedDataReader[RecordType], delimiter=","):
    r"""A reader for reading comma-delimited data into dataclasses.

    Example:
        ```pycon
        >>> from pathlib import Path
        >>> from dataclasses import dataclass
        >>> from tempfile import TemporaryDirectory
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import CsvReader
        >>>
        >>> with TemporaryDirectory() as folder:
        ...     path = Path(folder) / "data.csv"
        ...     _ = path.write_text("field1,field2\nmy-name,0.2\n")
        ...     with CsvReader.from_path[MyData](path) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """


class TsvReader(DelimitedDataReader[RecordType], delimiter="\t"):
    r"""A reader for reading tab-delimited data into dataclasses.

    Example:
        ```pycon
        >>> from pathlib import Path
        >>> from dataclasses import dataclass
        >>> from tempfile import TemporaryDirectory
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import TsvReader
        >>>
        >>> with TemporaryDirectory() as folder:
        ...     path = Path(folder) / "data.tsv"
        ...     _ = path.write_text("field1\tfield2\nmy-name\t0.2\n")
        ...     with TsvReader.from_path[MyData](path) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """


def supertype(kind: Any) -> Any:
    """Return the type a `NewType` stands for, through any `NewType`s between, else the type."""
    while hasattr(kind, "__supertype__"):
        kind = kind.__supertype__
    return kind


def plain_members(kind: Any) -> list[str] | None:
    """Return the lanes of a union of plain types, e.g. `int | str`, or None for any other type."""
    if get_origin(kind) not in (Union, UnionType):  # pyright: ignore[reportDeprecated]
        return None
    lanes = [
        next((lane for t, lane in NATIVE_LANES.items() if t is supertype(member)), None)
        for member in get_args(kind)
        if member is not NoneType
    ]
    return [lane for lane in lanes if lane] if all(lane in PLAIN_LANES for lane in lanes) else None


def written_text(value: object) -> str | None:
    """Return the text a writer writes for a plain value, or None for any other value."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | str):
        return str(value)
    return None


def lookup_table(kind: Any) -> dict[str, Any] | None:
    """Return the values of an enum or `Literal` by the text a writer writes for each of them.

    None is returned when a value is not text, an integer, or a boolean, when two values are
    written alike, and for a `Flag`, whose members combine into values no table can hold.
    """
    if isinstance(kind, type) and issubclass(kind, Enum) and not issubclass(kind, Flag):
        values: list[tuple[object, object]] = [(member.value, member) for member in kind]
    elif get_origin(kind) is Literal:
        values = [(value, value) for value in get_args(kind)]
    else:
        return None
    table: dict[str, Any] = {}
    for value, result in values:
        text = written_text(value)
        if text is None or table.setdefault(text, result) is not result:
            return None
    return table


def init_fields(record_type: type[Any], names: list[str]) -> list[str] | None:
    """Return the fields passed by keyword to a record type's `__init__`, or None to skip it.

    A dataclass's generated `__init__` only sets the fields it takes, unless it takes a parameter
    that is not a field, e.g. an `InitVar`, which it hands on to `__post_init__`. A record of a
    type that uses it, that `object.__new__` allocates, and whose fields are set as
    `object.__setattr__` sets them, is built as calling the type would build it, with its fields
    set directly. Any other type is called, so its own `__new__`, e.g. `BaseException.__new__`,
    runs first, and its own `__setattr__` sees each field. A field that `__init__` does not take,
    e.g. one with `init=False`, is set once the record is built.
    """
    init = record_type.__init__
    parameters = dict(islice(signature(init).parameters.items(), 1, None))
    own_new = any("__new__" in vars(base) for base in record_type.__mro__ if base is not object)
    setattr_: Any = record_type.__setattr__
    if (
        generated(init)
        and set(parameters) <= set(names)
        and not own_new
        and (setattr_ is object.__setattr__ or generated(setattr_))
        and not isabstract(record_type)
    ):
        return None
    return [name for name in names if name in parameters and parameters[name].kind in BY_KEYWORD]


def generated(method: Any) -> bool:
    """Return whether a dataclass generated a method, which it compiles from a string.

    A frozen dataclass's generated `__setattr__` only refuses to set a field, and its generated
    `__init__` sets each field as `object.__setattr__` does.
    """
    code = getattr(method, "__code__", None)
    return code is not None and code.co_filename == "<string>"
