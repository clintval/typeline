import bz2
import gzip
import lzma
from pathlib import Path
from typing import TextIO

MAGIC_BYTES: dict[bytes, str] = {
    b"\x1f\x8b": "gzip",
    b"BZh": "bzip2",
    b"\xfd7zXZ\x00": "xz",
}
"""The first bytes of each compression format, which reading detects."""

SUFFIXES: dict[str, str] = {".gz": "gzip", ".bz2": "bzip2", ".xz": "xz"}
"""The file extensions of each compression format, which writing uses."""


def open_for_reading(path: Path | str) -> TextIO:
    """Open a file to read as UTF-8 text, decompressing it when its contents are compressed."""
    path = Path(path).expanduser()
    with path.open("rb") as handle:
        start = handle.read(max(len(magic) for magic in MAGIC_BYTES))
    compression = next(
        (name for magic, name in MAGIC_BYTES.items() if start.startswith(magic)), None
    )
    if compression == "gzip":
        return gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    if compression == "bzip2":
        return bz2.open(path, "rt", encoding="utf-8-sig", newline="")
    if compression == "xz":
        return lzma.open(path, "rt", encoding="utf-8-sig", newline="")
    return path.open("r", encoding="utf-8-sig", newline="")


def open_for_writing(path: Path | str) -> TextIO:
    """Open a file to write as UTF-8 text, compressing it when its extension asks for it."""
    path = Path(path).expanduser()
    compression = SUFFIXES.get(path.suffix)
    if compression == "gzip":
        return gzip.open(path, "wt", encoding="utf-8", newline="")
    if compression == "bzip2":
        return bz2.open(path, "wt", encoding="utf-8", newline="")
    if compression == "xz":
        return lzma.open(path, "wt", encoding="utf-8", newline="")
    return path.open("w", encoding="utf-8", newline="")
