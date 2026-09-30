import bz2
import gzip
import lzma
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from typeline import TsvReader
from typeline import TsvWriter


@dataclass(frozen=True)
class Sample:
    """A small record for testing compressed files."""

    name: str
    reads: int


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
