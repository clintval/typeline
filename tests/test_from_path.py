import bz2
import gzip
import lzma
from collections.abc import Callable
from dataclasses import dataclass
from io import StringIO
from os import linesep
from pathlib import Path
from typing import Any

import pytest

from typeline import CsvReader
from typeline import CsvWriter
from typeline import TsvReader
from typeline import TsvWriter


@dataclass(frozen=True)
class Sample:
    """A small record for testing files read and written by from_path."""

    name: str
    reads: int


@dataclass
class Note:
    """A record with free text that can hold line breaks."""

    name: str
    text: str


TEXT = "name\treads\ntumor\t1200\nnormal\t980\n"
RECORDS = [Sample("tumor", 1200), Sample("normal", 980)]
COMPRESSORS: list[tuple[str, Callable[[bytes], bytes], Callable[[bytes], bytes]]] = [
    (".gz", gzip.compress, gzip.decompress),
    (".bz2", bz2.compress, bz2.decompress),
    (".xz", lzma.compress, lzma.decompress),
]


@pytest.mark.parametrize("suffix,compress,_decompress", COMPRESSORS)
def test_from_path_reads_compressed_files(
    tmp_path: Path, suffix: str, compress: Callable[[bytes], bytes], _decompress: Any
) -> None:
    """Test that from_path reads gzip, bzip2 and xz files, by their contents not their name."""
    for name in (f"samples.tsv{suffix}", "samples.tsv"):
        path = tmp_path / name
        _ = path.write_bytes(compress(TEXT.encode()))
        with TsvReader.from_path[Sample](path) as reader:
            assert list(reader) == RECORDS


def test_from_path_reads_a_compressed_file_that_starts_with_a_byte_order_mark(
    tmp_path: Path,
) -> None:
    """Test that a byte order mark inside a compressed file is not read as part of the header."""
    path = tmp_path / "samples.tsv.gz"
    _ = path.write_bytes(gzip.compress(b"\xef\xbb\xbf" + TEXT.encode()))

    with TsvReader.from_path[Sample](path) as reader:
        assert list(reader) == RECORDS


@pytest.mark.parametrize("suffix,_compress,decompress", COMPRESSORS)
def test_from_path_writes_compressed_files_by_their_extension(
    tmp_path: Path, suffix: str, _compress: Any, decompress: Callable[[bytes], bytes]
) -> None:
    """Test that from_path compresses a file with a .gz, .bz2 or .xz extension."""
    path = tmp_path / f"samples.tsv{suffix}"

    with TsvWriter.from_path[Sample](path) as writer:
        writer.write_header()
        for record in RECORDS:
            writer.write(record)

    assert decompress(path.read_bytes()).decode() == TEXT

    with TsvReader.from_path[Sample](path) as reader:
        assert list(reader) == RECORDS


def test_from_path_writes_other_files_uncompressed(tmp_path: Path) -> None:
    """Test that a file without a compression extension is written as plain text."""
    path = tmp_path / "samples.tsv"

    with TsvWriter.from_path[Sample](path) as writer:
        writer.write_header()
        for record in RECORDS:
            writer.write(record)

    assert path.read_text() == TEXT


def test_a_plain_file_that_starts_like_bzip2_is_read_as_text(tmp_path: Path) -> None:
    """Test that only the full bzip2 signature, not its first letters, marks a file as bzip2."""

    @dataclass(frozen=True)
    class Code:
        BZh1: str

    path = tmp_path / "codes.tsv"
    _ = path.write_text("BZh1\nabc\n")

    with TsvReader.from_path[Code](path) as reader:
        assert list(reader) == [Code("abc")]


def test_from_path_reads_a_file_that_starts_with_a_byte_order_mark(tmp_path: Path) -> None:
    """Test that a UTF-8 byte order mark, as Excel writes, is not read as part of the header."""
    _ = (tmp_path / "excel.csv").write_bytes(b"\xef\xbb\xbfname,text\nx,y\n")

    with CsvReader.from_path[Note](tmp_path / "excel.csv") as reader:
        assert list(reader) == [Note("x", "y")]


def test_from_path_keeps_line_breaks_inside_quoted_fields(tmp_path: Path) -> None:
    """Test that a quoted field keeps its own line breaks exactly, as the csv module requires."""
    _ = (tmp_path / "notes.csv").write_bytes(b'name,text\r\nx,"one\r\ntwo"\r\n')

    with CsvReader.from_path[Note](tmp_path / "notes.csv") as reader:
        assert list(reader) == [Note("x", "one\r\ntwo")]


def test_from_path_writes_utf8_without_translating_line_endings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the writer opens its file as UTF-8 with newline="", so no line ends twice."""
    options: list[dict[str, Any]] = []
    original_open: Callable[..., Any] = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        options.append(kwargs)
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", recording_open)

    with CsvWriter.from_path[Note](tmp_path / "notes.csv") as writer:
        writer.write(Note("x", "é"))

    assert options == [{"encoding": "utf-8", "newline": ""}]
    assert (tmp_path / "notes.csv").read_bytes() == f"x,é{linesep}".encode()


def test_reader_skips_a_byte_order_mark_on_a_stream_it_is_given() -> None:
    """Test that a byte order mark at the start of a stream the caller opened is skipped."""

    @dataclass
    class Row:
        x: str

    with TsvReader[Row](StringIO("﻿x\nvalue\n")) as reader:
        assert list(reader) == [Row("value")]


def test_reader_from_a_path_closes_its_file_once_read_to_the_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that list() on a reader from from_path reads every record and closes its file."""
    path = tmp_path / "samples.tsv"
    _ = path.write_text("name\treads\ntumor\t1200\n")
    opened: list[Any] = []
    original_open: Callable[..., Any] = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original_open(self, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", recording_open)

    assert list(TsvReader.from_path[Sample](path)) == [Sample("tumor", 1200)]
    assert opened
    assert all(handle.closed for handle in opened)


def test_reader_leaves_a_handle_it_was_given_open_once_read_to_the_end() -> None:
    """Test that a reader does not close a handle it was given, since the caller owns it."""
    handle = StringIO("name\treads\ntumor\t1200\n")

    assert list(TsvReader[Sample](handle)) == [Sample("tumor", 1200)]
    assert not handle.closed


def test_reader_from_a_path_closes_its_file_when_a_record_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that a reader from from_path closes its file when reading a record fails."""
    path = tmp_path / "samples.tsv"
    _ = path.write_text("name\treads\ntumor\t1200\nnormal\tmany\n")
    opened: list[Any] = []
    original_open: Callable[..., Any] = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original_open(self, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", recording_open)

    with pytest.raises(Exception, match="many"):
        _ = list(TsvReader.from_path[Sample](path))

    assert opened
    assert all(handle.closed for handle in opened)


@pytest.mark.parametrize("compress", [bytes, gzip.compress, bz2.compress, lzma.compress])
def test_from_path_reads_an_empty_file(tmp_path: Path, compress: Callable[[bytes], bytes]) -> None:
    """Test that an empty file, compressed or not, has no records."""
    path = tmp_path / "empty.tsv"
    _ = path.write_bytes(compress(b""))

    with TsvReader.from_path[Sample](path, header=False) as reader:
        assert list(reader) == []


def test_from_path_reads_a_file_shorter_than_a_compression_signature(tmp_path: Path) -> None:
    """Test that a plain file shorter than the longest compression signature is read whole."""

    @dataclass(frozen=True)
    class Code:
        BZh: str

    path = tmp_path / "codes.tsv"
    _ = path.write_bytes(b"BZh\nabc\n")

    with TsvReader.from_path[Code](path) as reader:
        assert list(reader) == [Code("abc")]


@pytest.mark.parametrize("contents", [b"", b"BZh\n", gzip.compress(TEXT.encode())])
def test_reader_from_a_path_closes_the_file_it_decompresses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, contents: bytes
) -> None:
    """Test that closing a reader from from_path closes its file, however short or compressed."""
    path = tmp_path / "samples.tsv"
    _ = path.write_bytes(contents)
    opened: list[Any] = []
    original_open: Callable[..., Any] = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original_open(self, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", recording_open)

    with TsvReader.from_path[Sample](path, header=False):
        assert len(opened) == 1
        assert not opened[0].closed

    assert opened[0].closed
