import csv
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import Field
from dataclasses import fields as fields_of
from os import linesep
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic
from typing import TextIO
from typing import cast

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
from ._comment import Comment
from ._data_types import RecordType
from ._data_types import accepts_none
from ._data_types import field_types
from ._data_types import find_codec
from ._data_types import is_text
from ._data_types import type_name
from .codecs import NO_CODECS
from .codecs import Codecs
from .codecs import FieldCodec

DEFAULT_COMMENT_PREFIXES: set[str] = set()
"""The default line prefixes that will tell the reader to skip those lines."""

JSON_LITERAL_KEYWORDS: frozenset[str] = frozenset({"null", "true", "false"})
"""JSON literal keywords that require parsing."""


class ReaderOptions(TypedDict, total=False, closed=True):
    """The options of a delimited data reader."""

    header: bool
    """Whether we expect the first line to be a header or not."""

    comment_prefixes: Collection[str]
    """Skip lines that have any of these string prefixes."""

    none_field: str
    """The text read as None in fields that allow None; a `str` field keeps it as text."""

    codecs: Codecs
    """How to read a field from its text, by the field's type."""

    dec_hook: Callable[[type, Any], Any] | None
    """Decode custom types anywhere in a record, with the semantics of msgspec's `dec_hook`."""

    on_comment: Callable[[Comment], None] | None
    """Receive each comment line as it is skipped; comments are dropped if None."""


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
        comment_prefixes: Collection[str] = DEFAULT_COMMENT_PREFIXES,
        none_field: str = "",
        codecs: Codecs = NO_CODECS,
        dec_hook: Callable[[type, Any], Any] | None = None,
        on_comment: Callable[[Comment], None] | None = None,
    ):
        """Instantiate a new delimited data reader.

        Args:
            handle: a file-like object to read delimited data from.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
            none_field: the string that is used in place of None for a field.
            codecs: how to read a field from its text, by the field's type.
            dec_hook: decode custom types anywhere in a record, like msgspec's `dec_hook`.
            on_comment: receive each comment line as it is skipped; comments are dropped if None.
        """
        record_type = cast(type[RecordType], self._bound_record_type())

        # Initialize and save internal attributes of this class.
        self._handle: TextIO = handle
        self._line_count: int = 0
        self._record_type: type[RecordType] = record_type
        self._comment_prefixes: tuple[str, ...] = tuple(comment_prefixes)
        self._none_field: str = none_field
        self._dec_hook: Callable[[type, Any], Any] | None = dec_hook
        self._on_comment: Callable[[Comment], None] | None = on_comment

        # Build a JSON decoder for parsing string values into Python objects
        self._json_decoder: JSONDecoder[Any] = JSONDecoder()

        # Inspect the record type, and decide once how each field is read from its text.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in self._fields]
        self._field_type_map: dict[str, Any] = field_types(record_type)
        self._field_readers: list[tuple[str, Callable[[str], Any] | None]] = [
            (name, self._field_reader(name, field_type, find_codec(field_type, codecs)))
            for name, field_type in self._field_type_map.items()
        ]

        # Read rows as lists, filtering out any comment lines along the way.
        self._rows: Iterator[list[str]] = csv.reader(
            self._filter_out_comments(handle),
            delimiter=self.delimiter,
            lineterminator=linesep,
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )

        # Protect the user from the case where a header was specified, but a data line was found!
        found: list[str] | None = next(self._rows, None) if header else None
        if found is not None and found != self._header:
            missing: list[str] = [name for name in self._header if name not in found]
            unexpected: list[str] = [name for name in found if name not in self._header]
            raise ValueError(
                "Fields of header do not match fields of dataclass!"
                + f" Header: {found}."
                + f" Fields of {record_type.__name__}: {self._header}."
                + (f" Missing from header: {missing}." if missing else "")
                + (f" Unexpected in header: {unexpected}." if unexpected else "")
                + ("" if missing or unexpected else " The fields are out of order.")
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

    def _filter_out_comments(self, lines: Iterator[str]) -> Iterator[str]:
        """Yield only lines in an iterator that do not start with a comment prefix."""
        prefixes = self._comment_prefixes
        for line in lines:
            self._line_count += 1
            stripped = line.strip()
            if not stripped:
                continue
            if prefixes and stripped.startswith(prefixes):
                if self._on_comment is not None:
                    self._on_comment(Comment(self._line_count, line.rstrip("\r\n")))
                continue
            yield line

    def _field_reader(
        self, name: str, field_type: Any, codec: FieldCodec[Any] | None
    ) -> Callable[[str], Any] | None:
        """Decide how a field is read from its text, or return None to keep the text as-is."""
        none_field = self._none_field if accepts_none(field_type) else None

        if codec is not None:
            missing = codec.missing

            def read_with_codec(text: str) -> Any:
                if text == none_field or text == missing:
                    return None
                try:
                    return codec.from_text(text)
                except Exception as exception:
                    raise ValueError(
                        f"Could not read field '{name}' of type {type_name(field_type)} from text"
                        + f" '{text}' on line {self._line_count}!"
                    ) from exception

            return read_with_codec

        if is_text(field_type):
            if none_field is None:
                return None
            return lambda text: None if text == none_field else text

        decode = self._json_decoder.decode

        def read_as_json(text: str) -> Any:
            if text == none_field:
                return None
            stripped = text.strip()
            if stripped in JSON_LITERAL_KEYWORDS or (stripped and stripped[0] in "{["):
                try:
                    return decode(text.encode("utf-8"))
                except DecodeError:
                    return text
            return text

        return read_as_json

    def _convert_hook(self, type_: Any, obj: Any) -> Any:
        """Pass through values a codec already built, and hand other custom types to dec_hook."""
        if isinstance(type_, type) and isinstance(obj, type_):
            return obj
        if self._dec_hook is not None:
            return self._dec_hook(type_, obj)
        raise NotImplementedError(f"No dec_hook to convert into {type_name(type_)}.")

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        width = len(self._header)
        field_readers = self._field_readers
        for row in self._rows:
            if len(row) != width:
                raise ValueError(
                    f"Expected {width} fields but found {len(row)} on line"
                    + f" {self._line_count} for record type: {self._record_type.__name__}."
                )
            preprocessed = {
                name: text if read is None else read(text)
                for (name, read), text in zip(field_readers, row, strict=True)
            }
            try:
                yield convert(
                    preprocessed,
                    self._record_type,
                    strict=False,
                    str_keys=True,
                    dec_hook=self._convert_hook,
                )
            except ValidationError as exception:
                raise ValidationError(
                    "Could not parse JSON-like object into requested structure:"
                    + f" {preprocessed}."
                    + f" Requested structure: {self._record_type.__name__}."
                    + f" Original exception: {exception}"
                ) from exception

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()
        return None

    @SubscriptableClassmethod
    @classmethod
    def from_path(cls, path: Path | str, /, **options: Unpack[ReaderOptions]) -> Self:
        """Construct a delimited data reader from a file path.

        Args:
            path: the path to the file to read delimited data from.
            options: the options of the reader, left at the reader's defaults when not given.
        """
        handle = Path(path).expanduser().open("r")
        try:
            return cls(handle, **options)
        except BaseException:
            handle.close()
            raise


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
