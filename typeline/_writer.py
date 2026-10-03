import csv
import re
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import AbstractContextManager
from io import StringIO
from math import isfinite
from os import linesep
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TextIO
from typing import cast

from msgspec import to_builtins
from msgspec.json import Encoder as JSONEncoder
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
from ._data_types import extra_columns_field
from ._data_types import field_types
from ._data_types import find_codec
from ._data_types import type_name
from ._files import open_for_writing
from .codecs import NO_CODECS
from .codecs import Codecs
from .codecs import FieldCodec

DEFAULT_COMMENT_PREFIXES: tuple[str, ...] = ("#",)
"""The default prefixes of comment lines a writer writes."""


class WriterOptions(TypedDict, total=False, closed=True):
    """The options of a delimited data writer."""

    rename: Mapping[str, str]
    """A new column name for each field, by field name; a field not named here keeps its own."""

    none_field: str
    """The text written for None; with the default `""`, an empty `str | None` reads as None."""

    codecs: Codecs
    """How to write a field into its text, by the field's type."""

    enc_hook: Callable[[Any], Any] | None
    """Encode custom types anywhere in a record, with the semantics of msgspec's `enc_hook`."""

    comment_prefixes: Sequence[str]
    """The prefixes a comment line may start with; the first is added to lines without one."""

    quoting: bool
    """Whether fields are quoted when needed, as in CSV, or never, for formats without quoting."""


LINE_BREAK: re.Pattern[str] = re.compile(r"\r\n|\r|\n")
"""The line breaks a reader splits lines at."""


class DelimitedDataWriter(
    DelimitedData,
    AbstractContextManager["DelimitedDataWriter[RecordType]"],
    Generic[RecordType],
):
    """A writer for writing dataclasses into delimited data."""

    def __init__(
        self,
        handle: TextIO,
        /,
        *,
        rename: Mapping[str, str] = NO_RENAME,
        none_field: str = "",
        codecs: Codecs = NO_CODECS,
        enc_hook: Callable[[Any], Any] | None = None,
        comment_prefixes: Sequence[str] = DEFAULT_COMMENT_PREFIXES,
        quoting: bool = True,
    ) -> None:
        """Instantiate a new delimited record writer.

        Args:
            handle: a text stream to write delimited data to, opened with `newline=""`.
            rename: a new name for the column of each field, by field name; the header is
                written with these names.
            none_field: the text written for None; with the default `""`, an empty `str | None`
                reads as None.
            codecs: how to write a field into its text, by the field's type.
            enc_hook: encode custom types anywhere in a record, like msgspec's `enc_hook`.
            comment_prefixes: the prefixes a comment line may start with; the first is added to
                comment lines written without one.
            quoting: whether fields are quoted when needed, or never; without quoting, text
                holding the delimiter or a line break cannot be written and is refused.
        """
        record_type = cast(type[RecordType], self._bound_record_type())

        # Initialize and save internal attributes of this class.
        self._handle: TextIO = handle
        self._record_type: type[RecordType] = record_type
        self._none_field: str = none_field
        self._enc_hook: Callable[[Any], Any] | None = enc_hook
        self._quoting: bool = quoting
        if isinstance(comment_prefixes, str):
            raise TypeError(
                "comment_prefixes must be a collection of strings,"
                + f" not the string {comment_prefixes!r}!"
            )
        if not comment_prefixes:
            raise ValueError("comment_prefixes must hold at least one prefix!")
        self._comment_prefixes: tuple[str, ...] = tuple(comment_prefixes)

        # Inspect the record type and save the fields and field names.
        self._field_type_map: dict[str, Any] = field_types(record_type)
        self._extra_field: str | None = extra_columns_field(record_type, self._field_type_map)
        self._counters: CounterFields = CounterFields(record_type, self._field_type_map)
        self._column_of: dict[str, str] = name_columns(
            record_type, self._field_type_map, self._extra_field, self._counters, rename
        )
        layout: list[tuple[str, str]] = [
            (name, column)
            for name in self._field_type_map
            if name != self._extra_field
            for column in self._counters.columns_of(name, self._column_of)
        ]
        self._header: tuple[str, ...] = tuple(column for _, column in layout)
        self._header_fields: tuple[str, ...] = tuple(name for name, _ in layout)
        self._field_codecs: list[tuple[str, FieldCodec[Any] | None, bool]] = [
            (name, find_codec(field_type, codecs), name in self._counters)
            for name, field_type in self._field_type_map.items()
            if name != self._extra_field
        ]

        # Build a JSON encoder for writing values that are not strings once converted to builtins.
        self._encoder: JSONEncoder = JSONEncoder()

        # Build the delimited writer which will use platform-dependent newlines.
        self._writer: Any = csv.writer(
            handle,
            delimiter=self.delimiter,
            lineterminator=linesep,
            quotechar='"' if quoting else None,
            quoting=csv.QUOTE_MINIMAL if quoting else csv.QUOTE_NONE,
        )
        self._quoting_writer: Any = csv.writer(
            handle, delimiter=self.delimiter, lineterminator=linesep, quoting=csv.QUOTE_ALL
        )

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

    def _format(self, field_name: str, value: object, codec: FieldCodec[Any] | None) -> str:
        """Write the value of one field into its text."""
        if value is None:
            if codec is not None and codec.missing is not None:
                return codec.missing
            return self._none_field

        if codec is not None:
            try:
                return codec.into_text(value)
            except Exception as exception:
                raise self._unwritable(field_name) from exception

        kind = type(value)
        if kind is str and isinstance(value, str):
            return value
        if kind is int:
            return str(value)
        if kind is bool:
            return "true" if value else "false"
        if kind is float and isinstance(value, float):
            return self._encoder.encode(value).decode("utf-8") if isfinite(value) else repr(value)

        try:
            builtin = to_builtins(value, str_keys=True, enc_hook=self._enc_hook)
            return builtin if isinstance(builtin, str) else self._encoder.encode(builtin).decode()
        except (TypeError, ValueError) as exception:
            raise self._unwritable(field_name) from exception

    def _unwritable(self, field_name: str) -> ValueError:
        """Explain which field could not be written into its text."""
        field_type = type_name(self._field_type_map[field_name])
        where = in_column(field_name, self._column_of[field_name])
        return ValueError(f"Could not write field '{field_name}' of type {field_type}{where}!")

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object.

        Each field is written as the `none_field` when None, or with the codec for its type. Other
        values are converted to builtin types, then written as-is when a string, or else as JSON.
        """
        if not isinstance(record, self._record_type):
            raise ValueError(
                f"Expected {self._record_type.__name__} but found {type(record).__name__}!"
            )
        if self._counters:
            row: list[str] = []
            for name, codec, is_counter in self._field_codecs:
                if is_counter:
                    row.extend(self._counters.write(name, getattr(record, name)))
                else:
                    row.append(self._format(name, getattr(record, name), codec))
        else:
            row = [
                self._format(name, getattr(record, name), codec)
                for name, codec, _ in self._field_codecs
            ]
        if self._extra_field is not None:
            extra = getattr(record, self._extra_field)
            if not all(type(text) is str for text in extra):
                raise ValueError(
                    f"The ExtraColumns field '{self._extra_field}' of"
                    + f" {self._record_type.__name__} must hold text, but holds {extra!r}!"
                )
            row.extend(extra)
        if self._quoting and not (row and row[0].startswith(self._comment_prefixes)):
            self._writer.writerow(row)
        else:
            self._write_row(row)

    def _write_row(self, row: list[str] | tuple[str, ...]) -> None:
        """Write a row, quoting it whole when its first field would read as a comment."""
        like_comment = bool(row) and row[0].startswith(self._comment_prefixes)
        if not self._quoting:
            if like_comment or list(row) == [""] or any(map(self._needs_quoting, row)):
                raise self._unquotable(row)
            self._writer.writerow(row)
        elif like_comment:
            self._quoting_writer.writerow(row)
        else:
            self._writer.writerow(row)

    def _needs_quoting(self, text: str) -> bool:
        """Return whether text holds the delimiter or a line break, and so must be quoted."""
        return self.delimiter in text or "\n" in text or "\r" in text

    def _unquotable(self, row: list[str] | tuple[str, ...]) -> ValueError:
        """Explain which field of a row cannot be written without quoting."""
        index, reason = next(
            (
                (index, f"its text holds the delimiter or a line break: {text!r}")
                for index, text in enumerate(row)
                if self._needs_quoting(text)
            ),
            (0, "a record of one empty field must be quoted")
            if list(row) == [""]
            else (0, f"its text starts with a comment prefix: {row[0]!r}"),
        )
        named = index < len(self._header)
        name = self._header_fields[index] if named else self._extra_field
        where = in_column(self._header_fields[index], self._header[index]) if named else ""
        return ValueError(
            f"Cannot write field '{name}'{where} of {self._record_type.__name__} without quoting,"
            + f" because {reason}!"
        )

    def write_header(self) -> None:
        """Write the header line to the open file-like object."""
        self._write_row(self._header)

    def write_comment(self, comment: str | Comment) -> None:
        """Write a comment, e.g. one a reader sent to `on_comment`.

        A `Comment` is written as it was read. Each line of a string is written as-is when it
        starts with one of the writer's comment prefixes, or else after the first prefix.
        """
        if isinstance(comment, Comment):
            if LINE_BREAK.search(comment.text):
                raise ValueError(
                    f"A Comment is one line, but this one holds a line break: {comment.text!r}!"
                )
            _ = self._handle.write(f"{comment.text}{linesep}")
            return
        prefix = self._comment_prefixes[0]
        for line in LINE_BREAK.split(comment.rstrip("\r\n")):
            text = line if line.startswith(self._comment_prefixes) else f"{prefix} {line}".rstrip()
            _ = self._handle.write(f"{text}{linesep}")

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()

    @SubscriptableClassmethod
    @classmethod
    def from_path(cls, path: Path | str, /, **options: Unpack[WriterOptions]) -> Self:
        """Construct a delimited data writer from a file path.

        The file is written as UTF-8, and compressed when its path ends in `.gz`, `.bz2`, or `.xz`.
        The writer is checked before the file is opened, so a refused writer leaves a file alone.

        Args:
            path: the path to the file to write delimited data to.
            options: the options of the writer, left at the writer's defaults when not given.
        """
        _ = cls(StringIO(), **options)
        handle = open_for_writing(path)
        try:
            return cls(handle, **options)
        except BaseException:
            handle.close()
            raise


class CsvWriter(DelimitedDataWriter[RecordType], delimiter=","):
    r"""A writer for writing dataclasses into comma-delimited data.

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
        >>> from typeline import CsvWriter
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     with CsvWriter.from_path[MyData](tmpfile.name) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     Path(tmpfile.name).read_text()
        'field1,field2\nmy-name,0.2\n'

        ```
    """


class TsvWriter(DelimitedDataWriter[RecordType], delimiter="\t"):
    r"""A writer for writing dataclasses into tab-delimited data.

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
        >>> from typeline import TsvWriter
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     with TsvWriter.from_path[MyData](tmpfile.name) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     Path(tmpfile.name).read_text()
        'field1\tfield2\nmy-name\t0.2\n'

        ```
    """
