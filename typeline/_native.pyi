from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any

from typing_extensions import Self

class RecordBuilder:
    """Builds records of one type from rows of text."""

    def __init__(
        self,
        record_type: type[Any],
        fields: list[tuple[str, int, str, str | None, Any, str, str]],
        init: list[str] | None,
        width: int,
        extra: tuple[str, list[int]] | None,
        counters: list[tuple[str, list[tuple[Any, str, int]]]],
        file_name: str | None,
    ) -> None: ...

class RecordFormatter:
    """Writes records of one type as lines of text, one field per column."""

    def __init__(
        self,
        record_type: type[Any],
        fields: list[tuple[str, str, str, Any, str]],
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

def read_float(text: str) -> float | None:
    """Read a float as a float field reads one, or None when the text is not a float."""

def write_float(value: float) -> str:
    """Write a float as a float field writes one, e.g. `1.0`, `1e-6`, or `nan`."""

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
    def decode(self, text: str) -> Any:
        """Decode one record from a line of text, reading nothing from the stream."""
    def __iter__(self) -> Self: ...
    def __next__(self) -> Any: ...
