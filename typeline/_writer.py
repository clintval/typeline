import csv
from abc import ABC
from abc import abstractmethod
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
from typing import final

from msgspec import to_builtins
from msgspec.json import Encoder as JSONEncoder
from typing_extensions import Self
from typing_extensions import override

from ._data_types import RecordType


class DelimitedRecordWriter(
    AbstractContextManager["DelimitedRecordWriter[RecordType]"],
    Generic[RecordType],
    ABC,
):
    """A writer for writing dataclasses into delimited data."""

    def __init__(self, handle: TextIOWrapper, record_type: type[RecordType]) -> None:
        """Instantiate a new delimited struct writer.

        Args:
            handle: a file-like object to write records to.
            record_type: the type of the object we will be writing.
        """
        if not is_dataclass(record_type):
            raise ValueError("record_type is not a dataclass but must be!")

        # Initialize and save internal attributes of this class.
        self._handle: TextIOWrapper = handle
        self._record_type: type[RecordType] = record_type

        # Inspect the record type and save the fields, field names, and field types.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in fields_of(record_type)]

        # Build a JSON encoder for intermediate data conversion (after dataclass, before delimited).
        self._encoder: JSONEncoder = JSONEncoder()

        # Build the delimited dictionary reader, filtering out any comment lines along the way.
        self._writer: DictWriter[str] = DictWriter(
            handle,
            fieldnames=self._header,
            delimiter=self.delimiter,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

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

    def _encode(self, item: Any) -> Any:
        """A callback for overriding the encoding of builtin types and custom types."""
        if isinstance(item, tuple):
            return list(item)  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
        return item

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object."""
        if not isinstance(record, self._record_type):
            raise ValueError(
                f"Expected {self._record_type.__name__} but found"
                + f" {record.__class__.__qualname__}!"
            )

        encoded = {name: self._encode(getattr(record, name)) for name in self._header}
        builtin = cast(dict[str, Any], to_builtins(encoded, str_keys=True))
        as_dict = {
            name: value if isinstance(value, str) else self._encoder.encode(value).decode("utf-8")
            for name, value in builtin.items()
        }
        self._writer.writerow(as_dict)

        return None

    def write_header(self) -> None:
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
    ) -> "DelimitedRecordWriter[RecordType]":
        """Construct a delimited data writer from a file path.

        Args:
            path: the path to the file to write delimited data to.
            record_type: the type of the object we will be writing.
        """
        writer = cls(Path(path).expanduser().open("w"), record_type)
        return writer


class CsvRecordWriter(DelimitedRecordWriter[RecordType]):
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
        >>> from typeline import CsvRecordWriter
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     with CsvRecordWriter.from_path(tmpfile.name, MyData) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     Path(tmpfile.name).read_text()
        'field1,field2\nmy-name,0.2\n'

        ```
    """

    @property
    @override
    @final
    def delimiter(self) -> str:
        return ","


class TsvRecordWriter(DelimitedRecordWriter[RecordType]):
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
        >>> from typeline import TsvRecordWriter
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     with TsvRecordWriter.from_path(tmpfile.name, MyData) as writer:
        ...         writer.write_header()
        ...         writer.write(MyData(field1="my-name", field2=0.2))
        ...     Path(tmpfile.name).read_text()
        'field1\tfield2\nmy-name\t0.2\n'

        ```
    """

    @property
    @override
    @final
    def delimiter(self) -> str:
        return "\t"
