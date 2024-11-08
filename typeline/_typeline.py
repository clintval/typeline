import csv
import json
from abc import ABC
from csv import DictReader
from csv import DictWriter
from dataclasses import Field
from dataclasses import fields as fields_of
from dataclasses import is_dataclass
from io import TextIOWrapper
from pathlib import Path
from types import NoneType
from types import TracebackType
from types import UnionType
from typing import Any
from typing import ClassVar
from typing import ContextManager
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import Protocol
from typing import Self
from typing import TypeAlias
from typing import TypeVar
from typing import Union
from typing import get_args
from typing import get_origin
from typing import runtime_checkable

from msgspec import convert
from msgspec import to_builtins
from typing_extensions import override


@runtime_checkable
class DataclassInstance(Protocol):
    """A protocol for objects that are dataclass instances."""

    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


JsonType: TypeAlias = dict[str, "JsonType"] | list["JsonType"] | str | int | float | bool | None
"""A JSON-like data type."""

RecordType = TypeVar("RecordType", bound=DataclassInstance)
"""A type variable for the type of record (of dataclass type) for reading and writing."""


class DelimitedStructWriter(ContextManager, Generic[RecordType], ABC):
    """
    A writer for writing dataclasses into delimited data.

    Attributes:
        delimiter: the field delimiter in the output delimited data.
    """

    delimiter: str

    def __init__(self, handle: TextIOWrapper, record_type: type[RecordType]) -> None:
        """
        Instantiate a new delimited struct writer.

        Args:
            handle: a file-like object to write records to.
            record_type: the type of the object we will be writing.
        """
        assert is_dataclass(record_type), "record_type is not a dataclass but must be!"
        self._record_type = record_type
        self._handle = handle
        self._fields = fields_of(record_type)
        self._header = [field.name for field in fields_of(record_type)]
        self._writer = DictWriter(
            handle,
            fieldnames=self._header,
            delimiter=self.delimiter,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

    def __init_subclass__(cls, delimiter: str, **kwargs: Any) -> None:
        """
        Initialize all subclasses by setting the delimiter.

        Args:
            delimiter: the field delimiter in the output delimited data.
            kwargs: any other key-word arguments.
        """
        super().__init_subclass__(**kwargs)
        cls.delimiter = delimiter

    @override
    def __enter__(self) -> Self:
        """Enter this context."""
        return self

    @override
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        """Close and exit this context."""
        self.close()
        return None

    @staticmethod
    def _encode(item: Any) -> Any:
        """A callback for overriding the encoding of builtin types and custom types."""
        if isinstance(item, tuple):
            return list(item)
        return item

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object."""
        assert is_dataclass(record), "record is not a dataclass but must be!"
        assert isinstance(record, self._record_type), f"Expected {self._record_type.__name__}!"
        encoded = {name: self._encode(getattr(record, name)) for name in self._header}
        builtin = {
            name: (json.dumps(value) if not isinstance(value, str) else value)
            for name, value in to_builtins(encoded, str_keys=True).items()
        }
        self._writer.writerow(builtin)
        return None

    def writeheader(self) -> None:
        """Write the header line to the open file-like object."""
        self._writer.writeheader()
        return None

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()
        return None

    @classmethod
    def from_path(cls, path: Path | str, record_type: type[RecordType]) -> Self:
        """Construct a delimited struct writer from a file path."""
        writer = cls(Path(path).open("w"), record_type)
        return writer


class CsvStructWriter(DelimitedStructWriter, delimiter=","):
    """
    A writer for writing dataclasses into comma-delimited data.

    Attributes:
        delimiter: the field delimiter in the output delimited data.
    """


class TsvStructWriter(DelimitedStructWriter, delimiter="\t"):
    """
    A writer for writing dataclasses into tab-delimited data.

    Attributes:
        delimiter: the field delimiter in the output delimited data.
    """


class DelimitedStructReader(Iterable[RecordType], ContextManager, Generic[RecordType], ABC):
    """
    A reader for reading delimited data into dataclasses.

    Attributes:
        delimiter: the field delimiter in the input delimited data.
        comment_chars: any characters that when one prefixes a line marks it as a comment.
    """

    delimiter: str
    comment_chars: set[str]

    def __init__(
        self,
        handle: TextIOWrapper,
        record_type: type[RecordType],
        /,
        has_header: bool = True,
    ):
        """
        Instantiate a new delimited struct reader.

        Args:
            handle: a file-like object to read records from.
            record_type: the type of the object we will be writing.
            has_header: whether we expect the first line to be a header or not.
        """
        assert is_dataclass(record_type), "record_type is not a dataclass but must be!"
        self.comment_chars = {"#"}
        self._record_type = record_type
        self._handle = handle
        self._fields = fields_of(record_type)
        self._header = [field.name for field in self._fields]
        self._types = [field.type for field in self._fields]
        self._reader = DictReader(
            self._filter_out_comments(handle),
            fieldnames=self._header if not has_header else None,
            delimiter=self.delimiter,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )
        if set(self._reader.fieldnames) != set(self._header):
            raise ValueError("Fields of header do not match fields of dataclass!")

    def __init_subclass__(cls, delimiter: str, **kwargs: Any) -> None:
        """
        Initialize all subclasses by setting the delimiter.

        Args:
            delimiter: the field delimiter in the output delimited data.
            kwargs: any other key-word arguments.
        """
        super().__init_subclass__(**kwargs)
        cls.delimiter = delimiter

    @override
    def __enter__(self) -> Self:
        """Enter this context."""
        return self

    @override
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        """Close and exit this context."""
        self.close()
        return None

    def _filter_out_comments(self, lines: Iterator[str]) -> Iterator[str]:
        """Yield only lines in an iterator that do not start with a comment character."""
        for line in lines:
            stripped: str = line.strip()
            if stripped and not any(stripped.startswith(char) for char in self.comment_chars):
                yield line

    def _value_to_builtin(self, name: str, value: str, field_type: type | str | Any) -> Any:
        type_args: tuple[type, ...] = get_args(field_type)
        type_origin: type | None = get_origin(field_type)
        is_union: bool = isinstance(field_type, UnionType)

        if value is None:
            return f'"{name}":null'
        elif value == "" and is_union and NoneType in type_args:
            return f'"{name}":null'
        elif field_type is bool or (is_union and bool in type_args):
            return f'"{name}":{value.lower()}'
        elif field_type is int or (is_union and int in type_args):
            return f'"{name}":{value}'
        elif field_type is float or (is_union and float in type_args):
            return f'"{name}":{value}'
        elif field_type is str or (is_union and str in type_args):
            return f'"{name}":"{value}"'
        elif type_origin in (dict, frozenset, list, set, tuple):
            return f'"{name}":{value}'
        elif is_union and len(type_args) >= 2 and NoneType in type_args:
            other_types: set[type] = set(type_args) - {NoneType}
            return self._value_to_builtin(name, value, Union[other_types])
        else:
            return f'"{name}":{value}'

    def _csv_dict_to_json(self, record: dict[str, str]) -> JsonType:
        """Build a list of builtin-like objects from a string-only dictionary."""
        key_values: list[str] = []

        for (name, value), field_type in zip(record.items(), self._types, strict=True):
            decoded: Any = self._decode(field_type, value)

            key_value = self._value_to_builtin(name, decoded, field_type)

            key_value = key_value.replace("\t", "\\t")
            key_value = key_value.replace("\r", "\\r")
            key_value = key_value.replace("\n", "\\n")

            key_values.append(key_value)

        as_builtins: JsonType = json.loads(f"{{{','.join(key_values)}}}")

        return as_builtins

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        for record in self._reader:
            yield convert(
                self._csv_dict_to_json(record),
                self._record_type,
                strict=False,
            )

    @staticmethod
    def _decode(_: type[Any] | str | Any, item: Any) -> Any:
        """A callback for overriding the decoding of builtin types and custom types."""
        return item

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
        has_header: bool = True,
    ) -> Self:
        """Construct a delimited struct reader from a file path."""
        reader = cls(Path(path).open("r"), record_type, has_header=has_header)
        return reader


class CsvStructReader(DelimitedStructReader, delimiter=","):
    """
    A reader for reading comma-delimited data into dataclasses.

    Attributes:
        delimiter: the field delimiter in the input delimited data.
        comment_chars: any characters that when one prefixes a line marks it as a comment.
    """


class TsvStructReader(DelimitedStructReader, delimiter="\t"):
    """
    A reader for reading tab-delimited data into dataclasses.

    Attributes:
        delimiter: the field delimiter in the input delimited data.
        comment_chars: any characters that when one prefixes a line marks it as a comment.
    """
