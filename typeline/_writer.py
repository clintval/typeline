import re
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import AbstractContextManager
from io import StringIO
from os import linesep
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TextIO
from typing import cast

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
from ._comment import comment_prefixes_of
from ._counter_columns import CounterFields
from ._data_types import RecordType
from ._data_types import extra_columns_field
from ._data_types import field_types
from ._data_types import find_codec
from ._data_types import type_name
from ._files import open_for_writing
from ._native import RecordFormatter
from .codecs import NO_CODECS
from .codecs import Codecs

DEFAULT_COMMENT_PREFIXES: tuple[str, ...] = ("#",)
"""The default prefixes of comment lines a writer writes."""


class WriterOptions(TypedDict, total=False, closed=True):
    """The options of a delimited data writer."""

    header: bool
    """Whether to write the header once, before the first record, or alone if none is written."""

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
        header: bool = False,
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
            header: whether to write the header once, before the first record, or alone when the
                writer is closed without writing a record. Comments written before then come first.
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
        self._comment_prefixes: tuple[str, ...] = comment_prefixes_of(comment_prefixes)
        if not self._comment_prefixes:
            raise ValueError("comment_prefixes must hold at least one prefix!")
        self._writes_header: bool = header
        self._header_pending: bool = header

        # Inspect the record type, and decide once how each field is written into its text.
        field_type_map = field_types(record_type)
        extra_field = extra_columns_field(record_type, field_type_map)
        counters = CounterFields(record_type, field_type_map)
        column_of = name_columns(record_type, field_type_map, extra_field, counters, rename)
        layout: list[tuple[str, str]] = [
            (name, column)
            for name in field_type_map
            if name != extra_field
            for column in counters.columns_of(name, column_of)
        ]
        self._header: tuple[str, ...] = tuple(column for _, column in layout)
        fields: list[tuple[str, str, str, Any, str]] = []
        for name, field_type in field_type_map.items():
            if name == extra_field:
                continue
            if name in counters:
                fields.append((name, "counts", "", counters.written(name), ""))
                continue
            codec = find_codec(field_type, codecs)
            none = codec.missing if codec is not None and codec.missing is not None else none_field
            where = in_column(name, column_of[name])
            unwritable = f"Could not write field '{name}' of type {type_name(field_type)}{where}!"
            if codec is None:
                fields.append((name, "value", none, None, unwritable))
            else:
                fields.append((name, "codec", none, codec.into_text, unwritable))
        self._formatter: RecordFormatter = RecordFormatter(
            record_type,
            fields,
            extra_field,
            JSONEncoder(enc_hook=enc_hook).encode,
            self.delimiter,
            linesep,
            list(self._comment_prefixes),
            quoting,
            [f"field '{name}'{in_column(name, column)}" for name, column in layout],
        )
        if header and not quoting:
            _ = self._formatter.line(self._header)

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

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object.

        Each field is written as the `none_field` when None, or with the codec for its type. Other
        values are written as-is when a string, or else as JSON, or the string their JSON holds.
        With `header=True`, the header is written first if it has not been written yet.
        """
        line = self._formatter.format(record)
        if self._header_pending:
            self.write_header()
        _ = self._handle.write(line)

    def write_all(self, records: Iterable[RecordType]) -> None:
        """Write every record in order, after the header when `header=True`.

        The header is written as this starts, so it is written even when there are no records.
        """
        if self._header_pending:
            self.write_header()
        if type(self).write is not DelimitedDataWriter[Any].write:
            for record in records:
                self.write(record)
            return
        self._formatter.write_all(records, self._handle.write)

    def write_header(self) -> None:
        """Write the header line to the open file-like object.

        With `header=True`, the header is written only once, so this writes it now if it has not
        been written yet, and otherwise does nothing.
        """
        if self._writes_header and not self._header_pending:
            return
        self._header_pending = False
        _ = self._handle.write(self._formatter.line(self._header))

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
        """Close all opened resources, first writing the header if it is still to be written."""
        try:
            if self._header_pending:
                self.write_header()
        finally:
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
        >>> from tempfile import TemporaryDirectory
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import CsvWriter
        >>>
        >>> with TemporaryDirectory() as folder:
        ...     path = Path(folder) / "data.csv"
        ...     with CsvWriter.from_path[MyData](path) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     path.read_text()
        'field1,field2\nmy-name,0.2\n'

        ```
    """


class TsvWriter(DelimitedDataWriter[RecordType], delimiter="\t"):
    r"""A writer for writing dataclasses into tab-delimited data.

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
        >>> from typeline import TsvWriter
        >>>
        >>> with TemporaryDirectory() as folder:
        ...     path = Path(folder) / "data.tsv"
        ...     with TsvWriter.from_path[MyData](path) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     path.read_text()
        'field1\tfield2\nmy-name\t0.2\n'

        ```
    """
