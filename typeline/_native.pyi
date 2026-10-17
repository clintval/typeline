from collections.abc import Callable
from typing import Any

class RecordBuilder:
    """Builds records of one type from rows of text, one field per column."""

    def __init__(
        self,
        record_type: type[Any],
        fields: list[
            tuple[
                str,
                int,
                str,
                str | None,
                bool,
                dict[str, Any] | None,
                Callable[[str | bytes], Any] | None,
                Callable[[str, int | None], Any],
            ]
        ],
        width: int,
        extra: tuple[str, list[int], int] | None,
        row_fields: Callable[[list[str], int | None], dict[str, Any]] | None,
        file_name: str | None,
        decode_error: type[Exception],
        validation_error: type[Exception],
    ) -> None: ...
    def build(self, row: list[str], line_number: int | None) -> Any:
        """Build a record from a row, naming the line it starts on in errors."""

class RecordFormatter:
    """Writes records of one type as lines of text, one field per column."""

    def __init__(
        self,
        fields: list[tuple[str, str, str, Callable[[Any], Any]]],
        extra: tuple[str, Callable[[Any], Any]] | None,
        delimiter: str,
        line_end: str,
        comment_prefixes: list[str],
    ) -> None: ...
    def format(self, record: object) -> str | list[str]:
        """Format a record as its line of text, or as its texts when the line needs quoting."""
