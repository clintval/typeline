from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any

from typing_extensions import Self

class RecordBuilder:
    """Builds records of one type from rows of text, calling the record type with their fields."""

    def __init__(
        self,
        record_type: type[Any],
        direct: bool,
        fields: list[tuple[int, str, str | None, Any, str, str]],
        slots: list[tuple[str, int, str]],
        width: int,
        extra: tuple[list[int], int] | None,
        row_fields: Callable[[list[str], int | None], dict[str, Any]] | None,
        file_name: str | None,
        decode_error: type[Exception],
    ) -> None: ...
    def build(self, row: list[str], line_number: int | None) -> Any:
        """Build a record from a row, naming the line it starts on in errors."""

class RecordFormatter:
    """Writes records of one type as lines of text, one field per column."""

    def __init__(
        self,
        record_type: type[Any],
        fields: list[tuple[str, str, str, Callable[[Any], Any] | None, str]],
        extra: str | None,
        encode: Callable[[Any], bytes],
        delimiter: str,
        line_end: str,
        comment_prefixes: list[str],
        quoting: bool,
        labels: list[str],
    ) -> None: ...
    def format(self, record: object) -> str:
        """Format a record as its line of text, refusing a record of another type."""
    def line(self, texts: Sequence[str]) -> str:
        """Format texts as one line, e.g. a header."""
    def write_all(self, records: Iterable[object], write: Callable[[str], object]) -> None:
        """Write records to a stream, stopping at the first that cannot be written."""

class RowReader:
    """Reads records natively from a Python text stream, one row at a time."""

    close_when_read: bool

    def __init__(
        self,
        handle: Any,
        delimiter: str,
        quoting: bool,
        comment_prefixes: list[str],
        on_comment: Callable[[int, str], object] | None,
    ) -> None: ...
    def read_row(self) -> tuple[list[str], int] | None:
        """Read the next row, e.g. a header, as its fields and the line it starts on."""
    def bind(self, builder: RecordBuilder) -> None:
        """Build records with a builder."""
    def __iter__(self) -> Self: ...
    def __next__(self) -> Any: ...
