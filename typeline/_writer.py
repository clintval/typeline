import csv
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import Field
from dataclasses import fields as fields_of
from inspect import Parameter
from inspect import signature
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
from ._comment import Comment
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

    none_field: str
    """The text written for None; with the default `""`, an empty `str | None` reads as None."""

    codecs: Codecs
    """How to write a field into its text, by the field's type."""

    enc_hook: Callable[[Any], Any] | None
    """Encode custom types anywhere in a record, with the semantics of msgspec's `enc_hook`."""

    comment_prefixes: Sequence[str]
    """The prefixes a comment line may start with; the first is added to lines without one."""

    quoting: bool
    """Whether fields are quoted when needed, as in CSV, or never, as in formats like BED."""


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
        none_field: str = "",
        codecs: Codecs = NO_CODECS,
        enc_hook: Callable[[Any], Any] | None = None,
        comment_prefixes: Sequence[str] = DEFAULT_COMMENT_PREFIXES,
        quoting: bool = True,
    ) -> None:
        """Instantiate a new delimited record writer.

        Args:
            handle: a file-like object to write delimited data to.
            none_field: the string that is used in place of None for a field.
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
        if not comment_prefixes:
            raise ValueError("comment_prefixes must hold at least one prefix!")
        self._comment_prefixes: tuple[str, ...] = tuple(comment_prefixes)

        # Inspect the record type and save the fields and field names.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._field_type_map: dict[str, Any] = field_types(record_type)
        self._extra_field: str | None = extra_columns_field(record_type, self._field_type_map)
        self._header: tuple[str, ...] = tuple(
            field.name for field in self._fields if field.name != self._extra_field
        )
        self._field_codecs: list[tuple[str, FieldCodec[Any] | None]] = [
            (name, find_codec(field_type, codecs))
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
                field_type = type_name(self._field_type_map[field_name])
                raise ValueError(
                    f"Could not write field '{field_name}' of type {field_type}!"
                ) from exception

        kind = type(value)
        if kind is str and isinstance(value, str):
            return value
        if kind is int:
            return str(value)
        if kind is bool:
            return "true" if value else "false"
        if kind is float:
            return self._encoder.encode(value).decode("utf-8")

        builtin = to_builtins(value, str_keys=True, enc_hook=self._enc_hook)
        if isinstance(builtin, str):
            return builtin
        return self._encoder.encode(builtin).decode("utf-8")

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object.

        Each field is written as the `none_field` when None, or with the codec for its type. Other
        values are converted to builtin types, then written as-is when a string, or else as JSON.
        """
        if not isinstance(record, self._record_type):
            raise ValueError(
                f"Expected {self._record_type.__name__} but found {type(record).__name__}!"
            )
        row = [
            self._format(name, getattr(record, name), codec) for name, codec in self._field_codecs
        ]
        if self._extra_field is not None:
            row.extend(getattr(record, self._extra_field))
        if not self._quoting and any(map(self._needs_quoting, row)):
            raise self._unquotable(row)
        try:
            self._writer.writerow(row)
        except csv.Error as exception:
            if self._quoting:
                raise
            raise self._unquotable(row) from exception

    def _needs_quoting(self, text: str) -> bool:
        """Return whether text holds the delimiter or a line break, and so must be quoted."""
        return self.delimiter in text or "\n" in text or "\r" in text

    def _unquotable(self, row: list[str]) -> ValueError:
        """Explain which field of a row cannot be written without quoting."""
        index, reason = next(
            (
                (index, f"its text holds the delimiter or a line break: {text!r}")
                for index, text in enumerate(row)
                if self._needs_quoting(text)
            ),
            (0, "a record of one empty field must be quoted"),
        )
        name = self._header[index] if index < len(self._header) else self._extra_field
        return ValueError(
            f"Cannot write field '{name}' of {self._record_type.__name__} without quoting,"
            + f" because {reason}!"
        )

    def write_header(self) -> None:
        """Write the header line to the open file-like object."""
        self._writer.writerow(self._header)

    def write_comment(self, comment: str | Comment) -> None:
        """Write a comment, e.g. one a reader sent to `on_comment`.

        A `Comment` is written as it was read. Each line of a string is written as-is when it
        starts with one of the writer's comment prefixes, or else after the first prefix.
        """
        if isinstance(comment, Comment):
            _ = self._handle.write(f"{comment.text}{linesep}")
            return
        prefix = self._comment_prefixes[0]
        for line in comment.splitlines():
            text = line if line.startswith(self._comment_prefixes) else f"{prefix} {line}".rstrip()
            _ = self._handle.write(f"{text}{linesep}")

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()

    @SubscriptableClassmethod
    @classmethod
    def from_path(cls, path: Path | str, /, **options: Unpack[WriterOptions]) -> Self:
        """Construct a delimited data writer from a file path.

        Args:
            path: the path to the file to write delimited data to.
            options: the options of the writer, left at the writer's defaults when not given.
        """
        _refuse_unknown_options(cls, options)
        handle = open_for_writing(path)
        try:
            return cls(handle, **options)
        except BaseException:
            handle.close()
            raise


def _refuse_unknown_options(cls: type[Any], options: Mapping[str, Any]) -> None:
    """Refuse options the writer does not take, before its file is opened and so emptied."""
    parameters = signature(cls).parameters
    arguments = signature(cls).bind(None, **options).arguments
    for name, parameter in parameters.items():
        if parameter.kind is Parameter.VAR_KEYWORD:
            unknown = sorted(set(arguments.get(name, {})) - set(WriterOptions.__annotations__))
            if unknown:
                raise TypeError(
                    f"{cls.__name__}() got an unexpected keyword argument '{unknown[0]}'"
                )


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
