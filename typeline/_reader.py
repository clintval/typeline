import csv
from abc import ABC
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import AbstractContextManager
from csv import DictReader
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

from msgspec import ValidationError
from msgspec import convert
from typing_extensions import Self
from typing_extensions import override

from ._data_types import RecordType

DEFAULT_COMMENT_PREFIXES: set[str] = set([])
"""The default line prefixes that will tell the reader to skip those lines."""


class DelimitedDataReader(
    AbstractContextManager["DelimitedDataReader[RecordType]"],
    Iterable[RecordType],
    Generic[RecordType],
    ABC,
):
    """A reader for reading delimited text data into dataclasses."""

    delimiter: str
    """The delimiter used to separate fields in the delimited data."""

    def __init__(
        self,
        handle: TextIOWrapper,
        record_type: type[RecordType],
        /,
        header: bool = True,
        comment_prefixes: Collection[str] = DEFAULT_COMMENT_PREFIXES,
        none_field: str = "",
        dec_hook: Callable[[type, Any], Any] | None = None,
    ):
        """Instantiate a new delimited data reader.

        Args:
            handle: a file-like object to read delimited data from.
            record_type: the type of the object we will be writing.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
            none_field: the string that is used in place of None for a field.
            dec_hook: a custom decoder hook for the JSON decoder.
        """
        if not is_dataclass(record_type):
            raise ValueError("record_type is not a dataclass but must be!")

        # Initialize and save internal attributes of this class.
        self._handle: TextIOWrapper = handle
        self._line_count: int = 0
        self._record_type: type[RecordType] = record_type
        self._comment_prefixes: Collection[str] = set(comment_prefixes)
        self._none_field: str = none_field
        self._dec_hook: Callable[[type, Any], Any] = (
            lambda x, y: (x, y) if dec_hook is None else dec_hook
        )

        # Inspect the record type and save the fields, field names, and field types.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in self._fields]
        self._field_types: list[type | Any | str] = [field.type for field in self._fields]

        # Build the delimited dictionary reader, filtering out any comment lines along the way.
        self._reader: DictReader[Any] = DictReader(
            self._filter_out_comments(handle),
            delimiter=self.delimiter,
            fieldnames=self._header if not header else None,
            lineterminator=linesep,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

        # Protect the user from the case where a header was specified, but a data line was found!
        if self._reader.fieldnames is not None and self._reader.fieldnames != self._header:
            raise ValueError("Fields of header do not match fields of dataclass!")

    @override
    def __init_subclass__(cls, delimiter: str, **kwargs: object) -> None:
        """Define a delimiter upon the subclass using metaclass programming."""
        cls.delimiter = delimiter
        super().__init_subclass__(**kwargs)

    def with_decoder(self, dec_hook: Any) -> Self:
        """Set a custom decoder hook for the JSON decoder."""
        self._dec_hook = lambda typ, val: self._dec_hook(typ, dec_hook(typ, val))
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

    def _filter_out_comments(self, lines: Iterator[str]) -> Iterator[str]:
        """Yield only lines in an iterator that do not start with a comment prefix."""
        for line in lines:
            self._line_count += 1
            if not line or not (stripped := line.strip()):
                continue
            elif any(stripped.startswith(prefix) for prefix in self._comment_prefixes):
                continue
            yield line

    def _preprocess(self, value: Any) -> Any:
        """A custom preprocessing step that will pre-process a value prior to serialization."""
        if isinstance(value, str):
            return value
        elif value is None:
            return self._none_field
        else:
            return value

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        for record in self._reader:
            preprocessed = {key: self._preprocess(value) for key, value in record.items()}
            try:
                yield convert(
                    preprocessed,
                    self._record_type,
                    strict=False,
                    str_keys=True,
                    dec_hook=self._dec_hook,
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

    @classmethod
    def from_path(
        cls,
        path: Path | str,
        record_type: type[RecordType],
        /,
        header: bool = True,
        comment_prefixes: Collection[str] = DEFAULT_COMMENT_PREFIXES,
        none_field: str = "",
        dec_hook: Callable[[type, Any], Any] | None = None,
    ) -> Self:
        """Construct a delimited data reader from a file path.

        Args:
            path: the path to the file to read delimited data from.
            record_type: the type of the object we will be reading.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
            none_field: the string that is used in place of None for a field.
            dec_hook: a custom decoder hook for the underlying JSON decoder.
        """
        handle = Path(path).expanduser().open("r")
        reader = cls(
            handle,
            record_type,
            header=header,
            comment_prefixes=comment_prefixes,
            none_field=none_field,
            dec_hook=dec_hook,
        )
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
        ...     with CsvReader.from_path(tmpfile.name, MyData) as reader:
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
        ...     with TsvReader.from_path(tmpfile.name, MyData) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """
