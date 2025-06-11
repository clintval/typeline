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
from typing import cast

from msgspec import to_builtins
from msgspec.json import Encoder as JSONEncoder
from typing_extensions import Self
from typing_extensions import override

from ._data_types import RecordType


class DelimitedDataWriter(
    AbstractContextManager["DelimitedDataWriter[RecordType]"],
    Generic[RecordType],
    ABC,
):
    """A writer for writing dataclasses into delimited data."""

    delimiter: str
    """The delimiter used to separate fields in the delimited data."""

    def __init__(
        self,
        handle: TextIOWrapper,
        record_type: type[RecordType],
        /,
        none_field: str = "",
        enc_hook: Callable[[Any], Any] | None = None,
    ) -> None:
        """Instantiate a new delimited struct writer.

        Args:
            handle: a file-like object to read delimited data from.
            record_type: the type of the object we will be writing.
            none_field: the string that is used in place of None for a field.
            enc_hook: a custom encoder hook for the JSON decoder.
        """
        if not is_dataclass(record_type):
            raise ValueError("record_type is not a dataclass but must be!")

        # Initialize and save internal attributes of this class.
        self._handle: TextIOWrapper = handle
        self._record_type: type[RecordType] = record_type
        self._none_field: str = none_field
        self._enc_hook: Callable[[Any], Any] = lambda x: x if enc_hook is None else enc_hook

        # Inspect the record type and save the fields and field names.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in self._fields]

        # Build a JSON encoder for intermediate data conversion (after dataclass; before delimited).
        self._encoder: JSONEncoder = JSONEncoder(enc_hook=enc_hook)

        # Build the delimited dictionary writer which will use platform-dependent newlines.
        self._writer: DictWriter[str] = DictWriter(
            handle,
            fieldnames=self._header,
            delimiter=self.delimiter,
            lineterminator=linesep,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

    def with_encoder(self, enc_hook: Callable[[Any], Any]) -> Self:
        """Set a custom decoder hook for the JSON encoder."""
        self._enc_hook = lambda x: self._enc_hook(enc_hook(x))
        self._encoder = JSONEncoder(enc_hook=enc_hook)
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

    def _preprocess(self, value: Any) -> Any:
        """A custom preprocessing step that will pre-process a value prior to serialization."""
        if isinstance(value, str):
            return value
        else:
            return self._encoder.encode(value).decode("utf-8")

    def _postprocess(self, value: Any) -> Any:
        """A custom postprocessing step that will post-process a value after serialization."""
        if isinstance(value, str):
            return value.lstrip('"').rstrip('"')
        elif value is None:
            return self._none_field
        else:
            return value

    def write(self, record: RecordType) -> None:
        """Write the record to the open file-like object."""
        if not isinstance(record, self._record_type):
            raise ValueError(
                f"Expected {self._record_type.__name__} but found {record.__class__.__qualname__}!"
            )

        encoded = {name: self._preprocess(getattr(record, name)) for name in self._header}
        builtin = cast(dict[str, Any], to_builtins(encoded, str_keys=True, enc_hook=self._enc_hook))
        as_dict = {name: self._postprocess(value) for name, value in builtin.items()}
        self._writer.writerow(as_dict)

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
        none_field: str = "",
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
    pass
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
