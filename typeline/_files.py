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

    @override
    def close(self) -> None:
        try:
            self._rest.close()
        finally:
            super().close()


class _ClosesSource(io.BufferedIOBase):
    """A decompressed stream that closes the stream it decompresses when it is closed."""

    source: io.BufferedReader

    @override
    def close(self) -> None:
        try:
            super().close()
        finally:
            self.source.close()


class _GzipFile(_ClosesSource, gzip.GzipFile): ...


class _BZ2File(_ClosesSource, bz2.BZ2File): ...


class _LZMAFile(_ClosesSource, lzma.LZMAFile): ...


DECOMPRESSORS: dict[str, Callable[[io.BufferedReader], _GzipFile | _BZ2File | _LZMAFile]] = {
    "gzip": lambda source: _GzipFile(fileobj=source),
    "bzip2": _BZ2File,
    "xz": _LZMAFile,
}
"""How to decompress a stream of each compression format."""


def _start(source: io.BufferedReader) -> tuple[bytes, io.BufferedReader]:
    """Look at the first bytes of a stream, without consuming them, even when it cannot seek."""
    start = source.peek(SIGNATURE_LENGTH)[:SIGNATURE_LENGTH]
    if not start or len(start) == SIGNATURE_LENGTH:
        return start, source
    start = source.read(SIGNATURE_LENGTH)
    return start, io.BufferedReader(_Prefixed(start, source))


def open_for_reading(path: Path | str) -> TextIO:
    """Open a file or pipe once to read as UTF-8 text, decompressing it when it is compressed."""
    source = Path(path).expanduser().open("rb")
    try:
        start, stream = _start(source)
        compression = next(
            (name for name, signature in SIGNATURES.items() if signature.match(start)), None
        )
        if compression is None:
            return io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")
        decompressor = DECOMPRESSORS[compression](stream)
        decompressor.source = stream
        return io.TextIOWrapper(decompressor, encoding="utf-8-sig", newline="")
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
