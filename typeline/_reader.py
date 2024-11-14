import csv
from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import AbstractContextManager
from csv import DictReader
from dataclasses import Field
from dataclasses import fields as fields_of
from dataclasses import is_dataclass
from io import TextIOWrapper
from pathlib import Path
from types import NoneType
from types import TracebackType
from typing import Any
from typing import Generic
from typing import final
from typing import get_args

from msgspec import DecodeError
from msgspec import ValidationError
from msgspec import convert
from msgspec.json import Decoder as JSONDecoder
from typing_extensions import Self
from typing_extensions import override

from ._data_types import JsonType
from ._data_types import RecordType
from ._data_types import build_union
from ._data_types import is_union

DEFAULT_COMMENT_PREFIXES: set[str] = set([])
"""The default line prefixes that will tell the reader to skip those lines."""

# TODO: line number support for when errors are raised.


class DelimitedRecordReader(
    AbstractContextManager["DelimitedRecordReader[RecordType]"],
    Iterable[RecordType],
    Generic[RecordType],
    ABC,
):
    """A reader for reading delimited text data into dataclasses."""

    def __init__(
        self,
        handle: TextIOWrapper,
        record_type: type[RecordType],
        /,
        header: bool = True,
        comment_prefixes: set[str] = DEFAULT_COMMENT_PREFIXES,
    ):
        """Instantiate a new delimited data reader.

        Args:
            handle: a file-like object to read delimited data from.
            record_type: the type of the object we will be writing.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
        """
        if not is_dataclass(record_type):
            raise ValueError("record_type is not a dataclass but must be!")

        # Initialize and save internal attributes of this class.
        self._handle: TextIOWrapper = handle
        self._record_type: type[RecordType] = record_type
        self._comment_prefixes: set[str] = comment_prefixes

        # Inspect the record type and save the fields, field names, and field types.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in self._fields]
        self._field_types: list[type | str | Any] = [field.type for field in self._fields]

        # Build a JSON decoder for intermediate data conversion (after delimited, before dataclass).
        self._decoder: JSONDecoder[Any] = JSONDecoder(strict=False)

        # Build the delimited dictionary reader, filtering out any comment lines along the way.
        self._reader: DictReader[str] = DictReader(
            self._filter_out_comments(handle),
            fieldnames=self._header if not header else None,
            delimiter=self.delimiter,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

        # Protect the user from the case where a header was specified, but a data line was found!
        if self._reader.fieldnames is not None and (
            set(self._reader.fieldnames) != set(self._header)
        ):
            raise ValueError("Fields of header do not match fields of dataclass!")

    @property
    @abstractmethod
    def delimiter(self) -> str:
        """The single-character string that is expected to separate the delimited data."""

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
            stripped: str = line.strip()
            if not stripped:
                continue
            elif any(stripped.startswith(prefix) for prefix in self._comment_prefixes):
                continue
            yield line

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        for record in self._reader:
            as_builtins = self._csv_dict_to_json(record)
            try:
                yield convert(as_builtins, self._record_type, strict=False)
            except ValidationError as exception:
                raise ValidationError(
                    "Could not parse JSON-like object into requested structure:"
                    + f" {sorted(as_builtins.items())}."
                    + f" Requested structure: {self._record_type.__name__}."
                    + f" Original exception: {exception}"
                ) from exception

    def _csv_dict_to_json(self, record: dict[str, str]) -> dict[str, JsonType]:
        """Build a list of builtin-like objects from a string-only dictionary."""
        items: list[str] = []

        for (name, item), field_type in zip(record.items(), self._field_types, strict=True):
            decoded: str = self._decode(field_type, item)
            decoded = decoded.replace("\t", "\\t")
            decoded = decoded.replace("\r", "\\r")
            decoded = decoded.replace("\n", "\\n")
            items.append(f'"{name}":{decoded}')

        json_string: str = f"{{{','.join(items)}}}"

        try:
            as_builtins: dict[str, JsonType] = self._decoder.decode(json_string)
        except DecodeError as exception:
            raise DecodeError(
                "Could not load delimited data line into JSON-like format."
                + f" Built improperly formatted JSON: {json_string}."
                + f" Original exception: {exception}."
            ) from exception

        return as_builtins

    def _decode(self, field_type: type[Any] | str | Any, item: str) -> str:
        """A callback for overriding the string formatting of builtin and custom types."""
        if field_type is str:
            return f'"{item}"'
        elif field_type in (float, int):
            return f"{item}"
        elif field_type is bool:
            return f"{item}".lower()

        if not is_union(field_type):
            return f"{item}"
        else:
            type_args: tuple[type, ...] = get_args(field_type)

            if NoneType in type_args:
                other_types: set[type]
                if item == "" or item == "null":
                    return "null"
                elif len(type_args) == 2:
                    other_types = set(type_args) - {NoneType}
                    return self._decode(next(iter(other_types)), item)
                else:
                    other_types = set(type_args) - {NoneType}
                    return self._decode(build_union(*other_types), item)
            elif str in type_args:
                return f'"{item}"'
            elif any(_type in type_args for _type in (float, int)):
                return f"{item}"
            elif bool in type_args:
                return f"{item}".lower()

        return f"{item}"

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
        comment_prefixes: set[str] = DEFAULT_COMMENT_PREFIXES,
    ) -> Self:
        """Construct a delimited data reader from a file path.

        Args:
            path: the path to the file to read delimited data from.
            record_type: the type of the object we will be writing.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
        """
        handle = Path(path).expanduser().open("r")
        reader = cls(handle, record_type, header=header, comment_prefixes=comment_prefixes)
        return reader


class CsvRecordReader(DelimitedRecordReader[RecordType]):
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
        >>> from typeline import CsvRecordReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1,field2\nmy-name,0.2\n")
        ...     _ = tmpfile.flush()
        ...     with CsvRecordReader.from_path(tmpfile.name, MyData) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """

    @property
    @override
    @final
    def delimiter(self) -> str:
        return ","


class TsvRecordReader(DelimitedRecordReader[RecordType]):
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
        >>> from typeline import TsvRecordReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1\tfield2\nmy-name\t0.2\n")
        ...     _ = tmpfile.flush()
        ...     with TsvRecordReader.from_path(tmpfile.name, MyData) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """

    @property
    @override
    @final
    def delimiter(self) -> str:
        return "\t"
