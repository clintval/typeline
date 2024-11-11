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
from types import UnionType
from typing import Any
from typing import Generic
from typing import final
from typing import get_args
from typing import get_origin

from msgspec import DecodeError
from msgspec import ValidationError
from msgspec import convert
from msgspec.json import Decoder as JSONDecoder
from typing_extensions import Self
from typing_extensions import override

from ._data_types import JsonType
from ._data_types import RecordType


class DelimitedStructReader(
    AbstractContextManager["DelimitedStructReader[RecordType]"],
    Iterable[RecordType],
    Generic[RecordType],
    ABC,
):
    """A reader for reading delimited data into dataclasses."""

    def __init__(
        self,
        handle: TextIOWrapper,
        record_type: type[RecordType],
        /,
        has_header: bool = True,
    ):
        """Instantiate a new delimited struct reader.

        Args:
            handle: a file-like object to read records from.
            record_type: the type of the object we will be writing.
            has_header: whether we expect the first line to be a header or not.
        """
        if not is_dataclass(record_type):
            raise ValueError("record_type is not a dataclass but must be!")

        self._decoder: JSONDecoder[Any] = JSONDecoder(strict=False)
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

    @property
    @abstractmethod
    def delimiter(self) -> str:
        """Delimiter character to use in the output."""

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
            if not stripped:
                continue
            elif any(stripped.startswith(prefix) for prefix in self.comment_prefixes):
                continue
            yield line

    def _csv_dict_to_json(self, record: dict[str, str]) -> dict[str, JsonType]:
        """Build a list of builtin-like objects from a string-only dictionary."""
        items: list[str] = []

        for (name, item), field_type in zip(record.items(), self._types, strict=True):
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
                + f" Originally formatted message: {exception}."
            ) from exception

        return as_builtins

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

    def _decode(self, field_type: type[Any] | str | Any, item: Any) -> str:
        """A callback for overriding the decoding of builtin types and custom types."""
        if field_type is str:
            return f'"{item}"'
        elif field_type in (float, int):
            return f"{item}"
        elif field_type is bool:
            return f"{item}".lower()

        is_union: bool = isinstance(field_type, UnionType)
        type_args: tuple[type, ...] = get_args(field_type)

        if item == "" and is_union and NoneType in type_args:
            return "null"
        elif is_union and str in type_args:
            return f'"{item}"'
        elif is_union and int in type_args:
            return f"{item}"
        elif is_union and float in type_args:
            return f"{item}"
        elif is_union and bool in type_args:
            return f"{item}".lower()

        type_origin: type | None = get_origin(field_type)

        if type_origin in (dict, frozenset, list, set, tuple):
            return f"{item}"
        elif is_union and len(type_args) >= 2 and NoneType in type_args:
            other_types: set[type] = set(type_args) - {NoneType}
            return self._decode(other_types, item)
        else:
            return f"{item}"

    @property
    def comment_prefixes(self) -> set[str]:
        """Any string that when one prefixes a line, marks it as a comment."""
        return {"#"}

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


class CsvStructReader(DelimitedStructReader[RecordType]):
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
        >>> from typeline import CsvStructReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1,field2\nmy-name,0.2\n")
        ...     _ = tmpfile.flush()
        ...     with CsvStructReader.from_path(tmpfile.name, MyData) as reader:
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


class TsvStructReader(DelimitedStructReader[RecordType]):
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
        >>> from typeline import TsvStructReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1\tfield2\nmy-name\t0.2\n")
        ...     _ = tmpfile.flush()
        ...     with TsvStructReader.from_path(tmpfile.name, MyData) as reader:
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
