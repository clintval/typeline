import csv
import json
from abc import ABC
from contextlib import AbstractContextManager
from csv import DictWriter
from dataclasses import Field
from dataclasses import fields as fields_of
from dataclasses import is_dataclass
from io import TextIOWrapper
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Generic
from typing import cast

from msgspec import to_builtins
from typing_extensions import Self
from typing_extensions import override

from ._typeline import RecordType


class DelimitedStructWriter(
    AbstractContextManager["DelimitedStructWriter[RecordType]"],
    Generic[RecordType],
    ABC,
):
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
        self._record_type: type[RecordType] = record_type
        self._handle: TextIOWrapper = handle
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in fields_of(record_type)]
        self._writer: DictWriter[str] = DictWriter(
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

    @staticmethod
    def _encode(item: Any) -> Any:
        """A callback for overriding the encoding of builtin types and custom types."""
        if isinstance(item, tuple):
            return list(item)  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
        return item

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object."""
        assert is_dataclass(record), "record is not a dataclass but must be!"
        assert isinstance(record, self._record_type), f"Expected {self._record_type.__name__}!"
        encoded = {name: self._encode(getattr(record, name)) for name in self._header}
        builtin = {
            name: (json.dumps(value) if not isinstance(value, str) else value)
            for name, value in cast(dict[str, Any], to_builtins(encoded, str_keys=True)).items()
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
    def from_path(
        cls, path: Path | str, record_type: type[RecordType]
    ) -> "DelimitedStructWriter[RecordType]":
        """Construct a delimited struct writer from a file path."""
        writer = cls(Path(path).open("w"), record_type)
        return writer


class CsvStructWriter(DelimitedStructWriter[RecordType], delimiter=","):
    """
    A writer for writing dataclasses into comma-delimited data.

    Attributes:
        delimiter: the field delimiter in the output delimited data.
    """


class TsvStructWriter(DelimitedStructWriter[RecordType], delimiter="\t"):
    """
    A writer for writing dataclasses into tab-delimited data.

    Attributes:
        delimiter: the field delimiter in the output delimited data.
    """
