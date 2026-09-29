import csv
from abc import ABC
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
from typing_extensions import override

from ._data_types import RecordType


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
        none_field: str = "null",
        enc_hook: Callable[[Any], Any] | None = None,
    ) -> None:
        """Instantiate a new delimited record writer.

        Args:
            handle: a file-like object to write delimited data to.
            record_type: the type of the object we will be writing.
            none_field: the string that is used in place of None for a field.
            enc_hook: a custom encoder hook for converting values to builtin types.
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

        # Build a JSON encoder for intermediate data conversion (after dataclass; before delimited).
        self._encoder: JSONEncoder = JSONEncoder(enc_hook=enc_hook)

        # Build the delimited dictionary writer which will use platform-dependent newlines.
        self._writer: DictWriter[str] = DictWriter(
            handle,
            fieldnames=self._header_list,
            delimiter=self.delimiter,
            lineterminator=linesep,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

    def with_encoder(self, enc_hook: Callable[[Any], Any]) -> Self:
        """Chain an additional encoder hook.

        This allows building up multiple transformations::

            writer = (CsvWriter.from_path("out.csv", MyData)
                .with_encoder(custom_encoder_1)
                .with_encoder(custom_encoder_2))

        Args:
            enc_hook: A function that takes a value and returns the encoded value.

        Returns:
            Self for method chaining.
        """
        old_hook = self._enc_hook
        if old_hook is None:
            self._enc_hook = enc_hook
        else:
            self._enc_hook = lambda x: old_hook(enc_hook(x))
        self._encoder = JSONEncoder(enc_hook=self._enc_hook)
        return self

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

    def _encode(self, item: Any) -> Any:
        """Encode a value before writing to the delimited file.

        This method can be overridden in subclasses to provide custom encoding logic
        for specific types. The method receives a field value and should return the
        encoded representation (often a string or value that can be serialized).

        Args:
            item: The value to encode.

        Returns:
            The encoded value ready to be written.

        Example:
            >>> def _encode(self, item):
            ...     # Convert None to a period for BED format
            ...     if item is None:
            ...         return "."
            ...     # Convert lists to comma-separated strings
            ...     if isinstance(item, list):
            ...         return ",".join(map(str, item))
            ...     return item
        """
        # If an enc_hook function was provided, use it
        if self._enc_hook is not None:
            return self._enc_hook(item)
        return item

    def _format(self, value: Any) -> str:
        """Format a field value as the text of one delimited field."""
        builtin = to_builtins(self._encode(value), str_keys=True, enc_hook=self._enc_hook)
        if builtin is None:
            return self._none_field
        if isinstance(builtin, str):
            return builtin
        return self._encoder.encode(builtin).decode("utf-8")

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object.

        Each field value is passed through `_encode()`, converted to builtin types, and then written
        as-is when a string, as the `none_field` when None, or as JSON otherwise.
        """
        if not isinstance(record, self._record_type):
            raise ValueError(
                f"Expected {self._record_type.__name__} but found {record.__class__.__qualname__}!"
            )
        self._writer.writerow({name: self._format(getattr(record, name)) for name in self._header})

    def write_header(self) -> None:
        """Write the header line to the open file-like object."""
        self._writer.writeheader()

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()

    @classmethod
    def from_path(
        cls: type["DelimitedDataWriter[RecordType]"],
        path: Path | str,
        record_type: type[RecordType],
        none_field: str = "null",
        enc_hook: Callable[[Any], Any] | None = None,
    ) -> "DelimitedDataWriter[RecordType]":
        """Construct a delimited data writer from a file path.

        Args:
            path: the path to the file to write delimited data to.
            record_type: the type of the object we will be writing.
            none_field: the string that is used in place of None for a field.
            enc_hook: a custom encoder hook for the underlying JSON encoder.
        """
        return cls(
            Path(path).expanduser().open("w"), record_type, none_field=none_field, enc_hook=enc_hook
        )

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
