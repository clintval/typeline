import bz2
import gzip
import lzma
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import TextIO

SIGNATURES: dict[str, re.Pattern[bytes]] = {
    "gzip": re.compile(rb"\x1f\x8b"),
    "bzip2": re.compile(rb"BZh[1-9](1AY&SY|\x17rE8P\x90)"),
    "xz": re.compile(rb"\xfd7zXZ\x00"),
}
"""The first bytes of each compression format, which reading detects."""

SUFFIXES: dict[str, str] = {".gz": "gzip", ".bz2": "bzip2", ".xz": "xz"}
"""The file extensions of each compression format, which writing uses."""

OPENERS: dict[str, Callable[..., Any]] = {"gzip": gzip.open, "bzip2": bz2.open, "xz": lzma.open}
"""How to open a file of each compression format."""


def open_for_reading(path: Path | str) -> TextIO:
    """Open a file to read as UTF-8 text, decompressing it when its contents are compressed."""
    path = Path(path).expanduser()
    with path.open("rb") as binary:
        start = binary.read(10)
    compression = next(
        (name for name, signature in SIGNATURES.items() if signature.match(start)), None
    )
    opener = Path.open if compression is None else OPENERS[compression]
    handle: TextIO = opener(path, "rt", encoding="utf-8-sig", newline="")
    return handle


def open_for_writing(path: Path | str) -> TextIO:
    """Open a file to write as UTF-8 text, compressing it when its extension asks for it."""
    path = Path(path).expanduser()
    compression = SUFFIXES.get(path.suffix)
    opener = Path.open if compression is None else OPENERS[compression]
    handle: TextIO = opener(path, "wt", encoding="utf-8", newline="")
    return handle
