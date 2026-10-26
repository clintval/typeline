import bz2
import gzip
import io
import lzma
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import TextIO

from typing_extensions import override

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

SIGNATURES: dict[str, re.Pattern[bytes]] = {
    "gzip": re.compile(rb"\x1f\x8b"),
    "bzip2": re.compile(rb"BZh[1-9](1AY&SY|\x17rE8P\x90)"),
    "xz": re.compile(rb"\xfd7zXZ\x00"),
}
"""The first bytes of each compression format, which reading detects."""

SIGNATURE_LENGTH: int = 10
"""The number of bytes that holds the longest signature."""

SUFFIXES: dict[str, str] = {".gz": "gzip", ".bz2": "bzip2", ".xz": "xz"}
"""The file extensions of each compression format, which writing uses."""

OPENERS: dict[str, Callable[..., Any]] = {"gzip": gzip.open, "bzip2": bz2.open, "xz": lzma.open}
"""How to open a file to write in each compression format."""

DECOMPRESSORS: dict[str, Callable[[io.BufferedReader], Any]] = {
    "gzip": lambda source: gzip.GzipFile(fileobj=source),
    "bzip2": bz2.BZ2File,
    "xz": lzma.LZMAFile,
}
"""How to decompress a stream of each compression format, which leaves the stream open."""


class _Prefixed(io.RawIOBase):
    """A stream of bytes already read from a stream, followed by the rest of that stream."""

    def __init__(self, prefix: bytes, rest: io.BufferedReader) -> None:
        self._prefix: bytes = prefix
        self._rest: io.BufferedReader = rest

    @override
    def readable(self) -> bool:
        return True

    @override
    def readinto(self, buffer: "WriteableBuffer", /) -> int:
        if not self._prefix:
            return self._rest.readinto(buffer)
        with memoryview(buffer) as view:
            count = min(len(view), len(self._prefix))
            view[:count] = self._prefix[:count]
        self._prefix = self._prefix[count:]
        return count


class _TextFile(io.TextIOWrapper):
    """UTF-8 text read from an open file, named after the file and closing it when closed."""

    def __init__(self, stream: Any, source: io.BufferedReader) -> None:
        super().__init__(stream, encoding="utf-8-sig", newline="")
        self._source: io.BufferedReader = source

    @property
    @override
    def name(self) -> Any:
        return self._source.name

    @override
    def close(self) -> None:
        try:
            super().close()
        finally:
            self._source.close()


def _start(source: io.BufferedReader) -> tuple[bytes, io.BufferedReader]:
    """Look at the first bytes of a stream, without consuming them, even when it cannot seek."""
    start = source.peek(SIGNATURE_LENGTH)[:SIGNATURE_LENGTH]
    if not start or len(start) == SIGNATURE_LENGTH:
        return start, source
    start = source.read(SIGNATURE_LENGTH)
    return start, io.BufferedReader(_Prefixed(start, source))


def on_line(line_number: int | None, file_name: str | None) -> str:
    """Name the line a record starts on in an error, and its file when it has a name."""
    if line_number is None:
        return ""
    if file_name is None:
        return f" on line {line_number}"
    return f" on line {line_number} of '{file_name}'"


def open_for_reading(path: Path | str) -> TextIO:
    """Open a file or pipe once to read as UTF-8 text, decompressing it when it is compressed.

    Gzip, bzip2, and xz are recognized by the first bytes of the file, not by its suffix.
    The file is opened and read only once, so a pipe such as a FIFO or `/dev/stdin` works.
    A byte order mark is skipped, and line breaks are left as they are.
    Closing the returned handle closes the file.

    Args:
        path: the path of the file or pipe to read; a leading `~` is the home directory.

    Returns:
        A text handle whose `name` is the path it was opened from.

    Raises:
        OSError: if the file cannot be opened, e.g. `FileNotFoundError` for a missing file.
        UnicodeDecodeError: when reading text that is not UTF-8.
    """
    source = Path(path).expanduser().open("rb")
    try:
        start, stream = _start(source)
        compression = next(
            (name for name, signature in SIGNATURES.items() if signature.match(start)), None
        )
        buffer: Any = stream if compression is None else DECOMPRESSORS[compression](stream)
        return _TextFile(buffer, source)
    except BaseException:
        source.close()
        raise


def open_for_writing(path: Path | str) -> TextIO:
    """Open a file to write as UTF-8 text, compressing it when its extension asks for it."""
    path = Path(path).expanduser()
    compression = SUFFIXES.get(path.suffix)
    opener = Path.open if compression is None else OPENERS[compression]
    handle: TextIO = opener(path, "wt", encoding="utf-8", newline="")
    return handle
