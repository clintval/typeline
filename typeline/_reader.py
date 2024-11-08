import csv
import json
from abc import ABC
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
from types import UnionType
from typing import Any
from typing import Generic
from typing import get_args
from typing import get_origin

from msgspec import convert
from typing_extensions import Self
from typing_extensions import override

from ._typeline import JsonType
from ._typeline import RecordType


class DelimitedStructReader(
    AbstractContextManager["DelimitedStructReader[RecordType]"],
    Iterable[RecordType],
    Generic[RecordType],
    ABC,
):
    """
    A reader for reading delimited data into dataclasses.

    Attributes:
        delimiter: the field delimiter in the input delimited data.
        comment_chars: any characters that when one prefixes a line marks it as a comment.
    """

    delimiter: str

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
        self.comment_chars: set[str] = {"#"}
        self._record_type: type[RecordType] = record_type
        self._handle: TextIOWrapper = handle
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in self._fields]
        self._types: list[type | str | Any] = [field.type for field in self._fields]
        self._reader: DictReader[str] = DictReader(
            self._filter_out_comments(handle),
            fieldnames=self._header if not has_header else None,
            delimiter=self.delimiter,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )
        if self._reader.fieldnames is not None and set(self._reader.fieldnames) != set(
            self._header
        ):
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
        _ = super().__enter__()
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

    def _value_to_builtin(self, name: str, value: Any, field_type: type | str | Any) -> Any:
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
            return self._value_to_builtin(name, value, other_types)
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
            as_builtins = self._csv_dict_to_json(record)
            try:
                yield convert(as_builtins, self._record_type, strict=False)
            except ValueError as exception:
                raise ValueError(
                    f"Could not parse {record} as {self._record_type.__name__}!"
                    + f" Intermediate structure formed is: {as_builtins}"
                ) from exception

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


class CsvStructReader(DelimitedStructReader[RecordType], delimiter=","):
    """
    A reader for reading comma-delimited data into dataclasses.

    Attributes:
        delimiter: the field delimiter in the input delimited data.
        comment_chars: any characters that when one prefixes a line marks it as a comment.
    """


class TsvStructReader(DelimitedStructReader[RecordType], delimiter="\t"):
    """
    A reader for reading tab-delimited data into dataclasses.

    Attributes:
        delimiter: the field delimiter in the input delimited data.
        comment_chars: any characters that when one prefixes a line marks it as a comment.
    """
