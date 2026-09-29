import csv
from abc import ABC
from collections.abc import Mapping
from contextlib import AbstractContextManager
from csv import DictWriter
from dataclasses import Field
from dataclasses import fields as fields_of
from dataclasses import is_dataclass
from io import TextIOWrapper
from os import linesep
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Generic

from msgspec import to_builtins
from msgspec.json import Encoder as JSONEncoder
from typing_extensions import Self
from typing_extensions import TypedDict
from typing_extensions import Unpack
from typing_extensions import override

from ._data_types import RecordType
from ._data_types import strip_optional
from ._data_types import type_name
from .codecs import NO_CODECS
from .codecs import FieldCodec


class WriterOptions(TypedDict, total=False, closed=True):
    """The options of a delimited data writer."""

    none_field: str
    """The string that is used in place of None for a field."""

    codecs: Mapping[Any, FieldCodec[Any]]
    """How to write a field into its text, by the field's type."""

    enc_hook: Callable[[Any], Any] | None
    """Encode custom types anywhere in a record, with the semantics of msgspec's `enc_hook`."""


class DelimitedDataWriter(
    AbstractContextManager["DelimitedDataWriter[RecordType]"],
    ABC,
    Generic[RecordType],
):
    """A writer for writing dataclasses into delimited data."""

    delimiter: str
    """The delimiter used to separate fields in the delimited data."""

    def __init__(
        self,
        handle: TextIOWrapper,
        record_type: type[RecordType],
        /,
        *,
        none_field: str = "null",
        codecs: Mapping[Any, FieldCodec[Any]] = NO_CODECS,
        enc_hook: Callable[[Any], Any] | None = None,
    ) -> None:
        """Instantiate a new delimited record writer.

        Args:
            handle: a file-like object to write delimited data to.
            record_type: the type of the object we will be writing.
            none_field: the string that is used in place of None for a field.
            codecs: how to write a field into its text, by the field's type.
            enc_hook: encode custom types anywhere in a record, like msgspec's `enc_hook`.
        """
        if not is_dataclass(record_type):
            raise ValueError("record_type is not a dataclass but must be!")

        # Initialize and save internal attributes of this class.
        self._handle: TextIOWrapper = handle
        self._record_type: type[RecordType] = record_type
        self._none_field: str = none_field
        self._enc_hook: Callable[[Any], Any] | None = enc_hook

        # Inspect the record type and save the fields and field names.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: tuple[str, ...] = tuple(field.name for field in self._fields)
        self._header_list: list[str] = list(self._header)  # DictWriter needs a list
        self._field_type_map: dict[str, type | Any | str] = {f.name: f.type for f in self._fields}
        self._field_codecs: dict[str, FieldCodec[Any]] = {
            field.name: codecs[strip_optional(field.type)]
            for field in self._fields
            if strip_optional(field.type) in codecs
        }

        # Build a JSON encoder for writing values that are not strings once converted to builtins.
        self._encoder: JSONEncoder = JSONEncoder()

        # Build the delimited dictionary writer which will use platform-dependent newlines.
        self._writer: DictWriter[str] = DictWriter(
            handle,
            fieldnames=self._header_list,
            delimiter=self.delimiter,
            lineterminator=linesep,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

    @override
    def __enter__(self) -> Self:
        """Enter this context."""
        _ = super().__enter__()
        return self

    @override
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        """Exit this context while closing all open resources."""
        self.close()
        return None

    def _format(self, field_name: str, value: Any) -> str:
        """Write the value of one field into its text."""
        if value is None:
            return self._none_field

        codec = self._field_codecs.get(field_name)
        if codec is not None:
            try:
                return codec.into_text(value)
            except Exception as exception:
                field_type = type_name(strip_optional(self._field_type_map[field_name]))
                raise ValueError(
                    f"Could not write field '{field_name}' of type {field_type}!"
                ) from exception

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
                f"Expected {self._record_type.__name__} but found {record.__class__.__qualname__}!"
            )
        self._writer.writerow({
            name: self._format(name, getattr(record, name)) for name in self._header
        })

    def write_header(self) -> None:
        """Write the header line to the open file-like object."""
        self._writer.writeheader()

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()

    @classmethod
    def from_path(
        cls, path: Path | str, record_type: type[RecordType], /, **options: Unpack[WriterOptions]
    ) -> Self:
        """Construct a delimited data writer from a file path.

        Args:
            path: the path to the file to write delimited data to.
            record_type: the type of the object we will be writing.
            options: the options of the writer, left at the writer's defaults when not given.
        """
        handle = Path(path).expanduser().open("w")
        try:
            return cls(handle, record_type, **options)
        except BaseException:
            handle.close()
            raise

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "delimiter") or not isinstance(cls.delimiter, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(
                f"Subclass {cls.__name__} must define a string 'delimiter' class attribute"
            )


class CsvWriter(DelimitedDataWriter[RecordType]):
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
        ...     with CsvWriter.from_path(tmpfile.name, MyData) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     Path(tmpfile.name).read_text()
        'field1,field2\nmy-name,0.2\n'

        ```
    """

    delimiter: str = ","


class TsvWriter(DelimitedDataWriter[RecordType]):
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
        ...     with TsvWriter.from_path(tmpfile.name, MyData) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     Path(tmpfile.name).read_text()
        'field1\tfield2\nmy-name\t0.2\n'

        ```
    """

    delimiter: str = "\t"
