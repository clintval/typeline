import csv
from collections import Counter
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import AbstractContextManager
from io import StringIO
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TextIO
from typing import cast
from typing import get_origin

from msgspec import DecodeError
from msgspec import ValidationError
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
from ._data_types import type_name
from ._files import open_for_reading
from .codecs import NO_CODECS
from .codecs import Codecs
from .codecs import FieldCodec

DEFAULT_COMMENT_PREFIXES: tuple[str, ...] = ("#",)
"""The default line prefixes that will tell the reader to skip those lines."""

JSON_LITERAL_KEYWORDS: frozenset[str] = frozenset({"null", "true", "false"})
"""JSON literal keywords that require parsing."""


def _on_line(line_number: int | None) -> str:
    """Name the line a record starts on in an error, or nothing when it was not read from a file."""
    return "" if line_number is None else f" on line {line_number}"


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
        self._on_comment: Callable[[Comment], None] | None = on_comment
        self._close_when_read: bool = False

        # Build a JSON decoder for parsing string values into Python objects
        self._json_decoder: JSONDecoder[Any] = JSONDecoder()

        # Inspect the record type, and decide once how each field is read from its text.
        self._field_type_map: dict[str, Any] = field_types(record_type)
        self._extra_field: str | None = extra_columns_field(record_type, self._field_type_map)
        self._counters: CounterFields = CounterFields(record_type, self._field_type_map)
        self._column_of: dict[str, str] = name_columns(
            record_type, self._field_type_map, self._extra_field, self._counters, rename
        )
        self._field_readers: list[tuple[str, Callable[[str, int | None], Any] | None]] = [
            (name, self._field_reader(name, field_type, find_codec(field_type, codecs)))
            for name, field_type in self._field_type_map.items()
            if name in self._column_of
        ]
        self._header: list[str] = [self._column_of[name] for name, _ in self._field_readers]
        self._columns: list[str] = [
            column
            for name in self._field_type_map
            if name != self._extra_field
            for column in self._counters.columns_of(name, self._column_of)
        ]

        # Read rows as lists, filtering out blank and comment lines between records.
        self._dialect: dict[str, Any] = {
            "delimiter": self.delimiter,
            "quotechar": '"' if quoting else None,
            "quoting": csv.QUOTE_MINIMAL if quoting else csv.QUOTE_NONE,
        }
        self._record_end: int = 0
        self._rows: Any = csv.reader(self._filter_out_comments(handle), **self._dialect)

        # Match a header's columns to fields by name, so the columns may come in any order.
        self._positions: list[int] | None = None
        self._extra_positions: list[int] = []
        found: list[str] | None = next(self._rows, None) if header else None
        self._record_end = self._rows.line_num
        columns: set[str] = set(self._columns)
        if found is not None:
            repeated: list[str] = sorted(
                name for name, count in Counter(found).items() if count > 1 and name in columns
            )
            if repeated:
                raise ValueError(
                    f"Columns of header repeat a name on line {self._record_line}!"
                    + f" Header: {found}. Repeated in header: {repeated}."
                )
            present: set[str] = set(found)
            missing: list[str] = [name for name in self._columns if name not in present]
            unexpected: list[str] = [name for name in found if name not in columns]
            if missing or (unexpected and self._extra_field is None):
                raise ValueError(
                    f"Columns of header do not match fields of {record_type.__name__}"
                    + f" on line {self._record_line}! Header: {found}."
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
        self._read_record: Callable[[list[str], int | None], RecordType] = self._record_reader()

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

    def _filter_out_comments(self, lines: Iterator[str]) -> Iterator[str]:
        """Yield lines, skipping blank lines and comments, and sending comments to on_comment.

        A blank line holds only whitespace and no delimiter. Only a line that starts a record is
        skipped, so a quoted field keeps every line it holds.
        """
        prefixes = self._comment_prefixes
        delimiter = self.delimiter
        on_comment = self._on_comment
        line_number = 0
        yielded = 0
        for line in lines:
            line_number += 1
            if yielded == self._record_end:
                if line_number == 1:
                    line = line.removeprefix("\ufeff")
                if line.isspace() and delimiter not in line:
                    continue
                if prefixes and line.startswith(prefixes):
                    if on_comment is not None:
                        on_comment(Comment(line_number, line.rstrip("\r\n")))
                    continue
                self._record_line = line_number
            yielded += 1
            yield line

    def _field_reader(
        self, name: str, field_type: Any, codec: FieldCodec[Any] | None
    ) -> Callable[[str, int | None], Any] | None:
        """Decide how a field is read from its text, or return None to keep the text as-is."""
        none_field = self._none_field if accepts_none(field_type) else None

        if codec is not None:
            missing = none_field if codec.missing is None else codec.missing
            where = in_column(name, self._column_of[name])

            def read_with_codec(text: str, line_number: int | None) -> Any:
                if text == missing:
                    return None
                try:
                    return codec.from_text(text)
                except Exception as exception:
                    raise ValueError(
                        f"Could not read field '{name}' of type {type_name(field_type)} from text"
                        + f" '{text}'{where}{_on_line(line_number)}!"
                    ) from exception

            return read_with_codec

        if is_text(field_type):
            if none_field is None:
                return None
            return lambda text, _: None if text == none_field else text

        decode = self._json_decoder.decode

        def read_as_json(text: str, _: int | None) -> Any:
            if text == none_field:
                return None
            stripped = text.strip()
            if stripped in JSON_LITERAL_KEYWORDS or (stripped and stripped[0] in "{["):
                try:
                    return decode(text)
                except DecodeError:
                    return text
            return text

        return read_as_json

    def _convert_hook(self, type_: Any, obj: Any) -> Any:
        """Pass through values a codec already built, and hand other custom types to dec_hook."""
        kind = get_origin(type_) or type_
        if isinstance(kind, type) and isinstance(obj, kind):
            return obj
        if self._dec_hook is not None:
            return self._dec_hook(type_, obj)
        raise TypeError(f"Expected {type_name(type_)}, got {type(obj).__name__}")

    def _record_reader(self) -> Callable[[list[str], int | None], RecordType]:
        """Decide once how a row's columns are built into a record, naming its line in errors."""
        width = self._width
        named = len(self._header)
        positions = self._positions
        extra_positions = self._extra_positions
        field_readers = self._field_readers
        extra_field = self._extra_field
        counters = self._counters
        has_counters = bool(counters)
        record_type = self._record_type
        convert_hook = self._convert_hook

        def read_record(row: list[str], line_number: int | None) -> RecordType:
            if len(row) != width and (extra_field is None or len(row) < width):
                at_least = "" if extra_field is None else "at least "
                raise ValueError(
                    f"Expected {at_least}{width} columns but found {len(row)}"
                    + f"{_on_line(line_number)} for record type: {record_type.__name__}."
                )
            values = row if positions is None else [row[index] for index in positions]
            preprocessed = {
                name: text if read is None else read(text, line_number)
                for (name, read), text in zip(field_readers, values, strict=False)
            }
            if extra_field is not None and positions is None:
                preprocessed[extra_field] = tuple(row[named:])
            elif extra_field is not None:
                extra = [row[index] for index in extra_positions]
                preprocessed[extra_field] = (*extra, *row[width:])
            if has_counters:
                preprocessed.update(counters.read(row, line_number))
            try:
                return convert(
                    preprocessed, record_type, strict=False, str_keys=True, dec_hook=convert_hook
                )
            except ValidationError as exception:
                raise ValidationError(
                    f"Could not build {record_type.__name__} from {preprocessed}"
                    + f"{_on_line(line_number)}! {exception}"
                ) from exception

        return read_record

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        read_record = self._read_record
        try:
            rows = self._rows
            for row in rows:
                self._record_end = rows.line_num
                yield read_record(row, self._record_line)
        except Exception:
            if self._close_when_read:
                self.close()
            raise
        if self._close_when_read:
            self.close()

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
        return self._read_record(row, None)

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
        reader._close_when_read = True
        return reader


class CsvReader(DelimitedDataReader[RecordType], delimiter=","):
    r"""A reader for reading comma-delimited data into dataclasses.

    Example:
        ```pycon
        >>> from pathlib import Path
        >>> from dataclasses import dataclass
        >>> from tempfile import NamedTemporaryFile
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import CsvReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1,field2\nmy-name,0.2\n")
        ...     _ = tmpfile.flush()
        ...     with CsvReader.from_path[MyData](tmpfile.name) as reader:
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
        >>> from tempfile import NamedTemporaryFile
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import TsvReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1\tfield2\nmy-name\t0.2\n")
        ...     _ = tmpfile.flush()
        ...     with TsvReader.from_path[MyData](tmpfile.name) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """
