import csv
from collections import Counter
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import AbstractContextManager
from enum import Enum
from functools import partial
from inspect import Parameter
from inspect import signature
from io import StringIO
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import Literal
from typing import TextIO
from typing import TypeAlias
from typing import cast
from typing import get_args
from typing import get_origin

from msgspec import DecodeError
from msgspec import convert
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

NATIVE_LANES: dict[type, str] = {str: "text", int: "int", float: "float", bool: "bool"}
"""The types the native engine reads itself, and the lane it reads each in."""

POSITIONAL: frozenset[Any] = frozenset({Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD})
"""The kinds of parameter a field may be passed to positionally."""

FieldSpec: TypeAlias = tuple[
    int,
    str,
    str | None,
    bool,
    dict[str, Any] | None,
    Callable[[Any], Any] | None,
    type | None,
    Callable[[Any], Any] | None,
    str,
    str,
]
"""How the native engine reads a field: its column, lane, none text, whether its text may be JSON,
the table it is looked up in, its codec or decoder, the class of a codec's value kept as it is, how
other values of a codec are coerced, and how the field and its column are named in errors."""


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
        self._record_line: int = 0
        self._record_type: type[RecordType] = record_type
        if isinstance(comment_prefixes, str):
            raise TypeError(
                "comment_prefixes must be a collection of strings,"
                + f" not the string {comment_prefixes!r}!"
            )
        self._comment_prefixes: tuple[str, ...] = tuple(comment_prefixes)
        self._none_field: str = none_field
        self._dec_hook: Callable[[type, Any], Any] | None = dec_hook

        # Inspect the record type, and decide once how each field is read from its text.
        self._field_type_map: dict[str, Any] = field_types(record_type)
        self._extra_field: str | None = extra_columns_field(record_type, self._field_type_map)
        self._counters: CounterFields = CounterFields(record_type, self._field_type_map)
        self._column_of: dict[str, str] = name_columns(
            record_type, self._field_type_map, self._extra_field, self._counters, rename
        )
        self._named: list[str] = [name for name in self._field_type_map if name in self._column_of]
        self._header: list[str] = [self._column_of[name] for name in self._named]
        self._columns: list[str] = [
            column
            for name in self._field_type_map
            if name != self._extra_field
            for column in self._counters.columns_of(name, self._column_of)
        ]

        # Read rows natively, skipping blank and comment lines between records.
        self._dialect: dict[str, Any] = {
            "delimiter": self.delimiter,
            "quotechar": '"' if quoting else None,
            "quoting": csv.QUOTE_MINIMAL if quoting else csv.QUOTE_NONE,
        }
        self._rows: RowReader = RowReader(
            handle,
            self.delimiter,
            quoting,
            list(self._comment_prefixes),
            None if on_comment is None else lambda line, text: on_comment(Comment(line, text)),
        )

        # Match a header's columns to fields by name, so the columns may come in any order.
        self._positions: list[int] | None = None
        self._extra_positions: list[int] = []
        found: list[str] | None = None
        if header and (first := self._rows.read_row()) is not None:
            found, self._record_line = first
        columns: set[str] = set(self._columns)
        if found is not None:
            repeated: list[str] = sorted(
                name for name, count in Counter(found).items() if count > 1 and name in columns
            )
            if repeated:
                raise ValueError(
                    f"Columns of header repeat a name{on_line(self._record_line, self._file_name)}!"
                    + f" Header: {found}. Repeated in header: {repeated}."
                )
            present: set[str] = set(found)
            missing: list[str] = [name for name in self._columns if name not in present]
            unexpected: list[str] = [name for name in found if name not in columns]
            if missing or (unexpected and self._extra_field is None):
                raise ValueError(
                    f"Columns of header do not match fields of {record_type.__name__}"
                    + f"{on_line(self._record_line, self._file_name)}! Header: {found}."
                    + f" Columns of {record_type.__name__}: {self._columns}."
                    + (f" Missing from header: {missing}." if missing else "")
                    + (f" Unexpected in header: {unexpected}." if unexpected else "")
                )
        layout: list[str] = self._columns if found is None else found
        self._width: int = len(layout)
        self._counters.locate(layout)
        if self._counters or layout[: len(self._header)] != self._header:
            index_of: dict[str, int] = {name: index for index, name in enumerate(layout)}
            self._positions = [index_of[name] for name in self._header]
            self._extra_positions = [
                index for index, name in enumerate(layout) if name not in columns
            ]
        self._builder: RecordBuilder = self._record_builder(codecs)
        self._rows.bind(self._builder)

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

    def _convert_hook(self, type_: Any, obj: Any) -> Any:
        """Pass through values a codec already built, and hand other custom types to dec_hook."""
        kind = get_origin(type_) or type_
        if isinstance(kind, type) and isinstance(obj, kind):
            return obj
        if self._dec_hook is not None:
            return self._dec_hook(type_, obj)
        raise TypeError(f"Expected {type_name(type_)}, got {type(obj).__name__}")

    def _field_spec(self, name: str, column: int, codecs: Codecs) -> FieldSpec:
        """Decide how the native engine reads a field from the text of its column."""
        field_type = self._field_type_map[name]
        kind = strip_optional(field_type)
        label = f"field '{name}' of type {type_name(field_type)}"
        place = in_column(name, self._column_of[name])
        none = self._none_field if accepts_none(field_type) else None
        codec = find_codec(field_type, codecs)
        if codec is not None:
            missing = none if none is None or codec.missing is None else codec.missing
            origin = get_origin(kind) or kind
            trusted = origin if isinstance(origin, type) else None
            coerce = partial(
                convert, type=field_type, strict=False, str_keys=True, dec_hook=self._convert_hook
            )
            return (
                column,
                "codec",
                missing,
                False,
                None,
                codec.from_text,
                trusted,
                coerce,
                label,
                place,
            )
        lane = next((lane for t, lane in NATIVE_LANES.items() if t is kind), None)
        if lane is not None:
            return (column, lane, none, False, None, None, None, None, label, place)
        table = lookup_table(kind)
        if table is not None:
            return (column, "lookup", none, False, table, None, None, None, label, place)
        try:
            decode = JSONDecoder(field_type, strict=False, dec_hook=self._convert_hook).decode
        except TypeError as exception:
            raise TypeError(f"Cannot read {label}, which msgspec cannot decode!") from exception
        json_like = not is_text(field_type)
        return (column, "decode", none, json_like, None, decode, None, None, label, place)

    def _record_builder(self, codecs: Codecs) -> RecordBuilder:
        """Build the native engine that reads a row's columns into a record."""
        positions = self._positions
        fields = [
            self._field_spec(name, index if positions is None else positions[index], codecs)
            for index, name in enumerate(self._named)
        ]
        index_of: dict[str, int] = {name: index for index, name in enumerate(self._named)}
        names = list(self._field_type_map)
        direct = builds_directly(self._record_type, names)
        plan = (
            [(name, "after") for name in names] if direct else init_plan(self._record_type, names)
        )
        slots = [
            (name, index_of.get(name, -1 if name == self._extra_field else -2), how)
            for name, how in plan
        ]
        extra = (
            None
            if self._extra_field is None
            else (self._extra_positions, len(self._header) if positions is None else self._width)
        )
        counters, file_name = self._counters, self._file_name
        return RecordBuilder(
            self._record_type,
            direct,
            fields,
            slots,
            self._width,
            extra,
            (lambda row, line: counters.read(row, line, file_name)) if counters else None,
            file_name,
            DecodeError,
        )

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
        name = self._record_type.__name__
        if self.delimiter not in line and not line.strip():
            raise ValueError(f"Could not decode {name} from blank line {line!r}!")
        if line.startswith(self._comment_prefixes):
            raise ValueError(f"Could not decode {name} from comment line {line!r}!")
        lines = StringIO(line, newline="").readlines()
        # The empty last line is read only when the text ends inside a quoted field.
        rows = csv.reader([*lines, ""], **self._dialect)
        row = next(rows)
        if rows.line_num > len(lines):
            raise ValueError(
                f"Could not decode {name} from {line!r}, which ends inside a quoted field!"
            )
        if rows.line_num < len(lines):
            raise ValueError(
                f"Could not decode {name} from {line!r}, which holds more than one record!"
            )
        return cast(RecordType, self._builder.build(row, None))

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


def lookup_table(kind: Any) -> dict[str, Any] | None:
    """Return the values of an enum or `Literal` by their text, when all their values are text."""
    if isinstance(kind, type) and issubclass(kind, Enum):
        table: dict[Any, Any] = {member.value: member for member in kind}
    elif get_origin(kind) is Literal:
        table = {value: value for value in get_args(kind)}
    else:
        return None
    return table if all(isinstance(value, str) for value in table) else None


def builds_directly(record_type: type[Any], names: list[str]) -> bool:
    """Return whether a record type's fields may be set directly, as its generated `__init__` would.

    A dataclass's generated `__init__` is compiled from a string, unlike one written in a module,
    and only sets the fields it takes, unless it takes a parameter that is not a field, e.g. an
    `InitVar`, which it hands on to `__post_init__`.
    """
    code = getattr(record_type.__init__, "__code__", None)
    return (
        code is not None
        and code.co_filename == "<string>"
        and set(signature(record_type).parameters) <= set(names)
    )


def init_plan(record_type: type[Any], names: list[str]) -> list[tuple[str, str]]:
    """Order the fields of a record by how they are handed to its type when it is built.

    Fields are passed positionally while the parameters of the type follow them, then by keyword,
    and a field the type does not take, e.g. one with `init=False`, is set once it is built.
    """
    fields = set(names)
    positional: list[str] = []
    keyword: list[str] = []
    in_order = True
    for parameter in signature(record_type).parameters.values():
        passable = parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
        if in_order and parameter.name in fields and parameter.kind in POSITIONAL:
            positional.append(parameter.name)
        elif parameter.name in fields and passable:
            keyword.append(parameter.name)
            in_order = False
        else:
            in_order = False
    passed = {*positional, *keyword}
    return [
        *((name, "positional") for name in positional),
        *((name, "keyword") for name in keyword),
        *((name, "after") for name in names if name not in passed),
    ]
