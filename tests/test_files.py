import bz2
import gzip
import lzma
from collections.abc import Callable
from pathlib import Path

import pytest

import typeline
from typeline import open_for_reading

TEXT = "name\treads\ntumor\t1200\nnormal\t980\n"
COMPRESSORS: list[tuple[str, Callable[[bytes], bytes]]] = [
    ("plain", lambda data: data),
    ("gzip", gzip.compress),
    ("bzip2", bz2.compress),
    ("xz", lzma.compress),
]


def test_open_for_reading_is_public() -> None:
    """Test that the package exports open_for_reading."""
    assert "open_for_reading" in typeline.__all__
    assert typeline.open_for_reading is open_for_reading


@pytest.mark.parametrize("_name,compress", COMPRESSORS)
@pytest.mark.parametrize("file_name", ["samples.tsv", "samples.txt", "samples.gz", "samples.xz"])
def test_open_for_reading_reads_the_same_text_whatever_the_suffix(
    tmp_path: Path, _name: str, compress: Callable[[bytes], bytes], file_name: str
) -> None:
    """Test that plain, gzip, bzip2 and xz files open to the same text, by contents not suffix."""
    path = tmp_path / file_name
    _ = path.write_bytes(compress(TEXT.encode()))

    with open_for_reading(path) as handle:
        assert handle.read() == TEXT


def test_open_for_reading_accepts_a_string_path(tmp_path: Path) -> None:
    """Test that a path given as a string opens, and names the handle."""
    path = tmp_path / "samples.tsv"
    _ = path.write_bytes(TEXT.encode())

    with open_for_reading(str(path)) as handle:
        assert handle.name == str(path)
        assert handle.readline() == "name\treads\n"


def test_open_for_reading_expands_the_home_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that a leading ~ in the path is the home directory."""
    _ = (tmp_path / "samples.tsv").write_bytes(TEXT.encode())
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))

    with open_for_reading("~/samples.tsv") as handle:
        assert handle.read() == TEXT


@pytest.mark.parametrize("_name,compress", COMPRESSORS)
def test_open_for_reading_closes_its_file_when_closed(
    tmp_path: Path, _name: str, compress: Callable[[bytes], bytes]
) -> None:
    """Test that closing the text handle closes the file underneath it."""
    path = tmp_path / "samples.tsv"
    _ = path.write_bytes(compress(TEXT.encode()))

    handle = open_for_reading(path)
    source = handle.buffer
    _ = handle.readline()
    handle.close()

    assert handle.closed
    assert source.closed


def test_open_for_reading_raises_for_a_missing_file(tmp_path: Path) -> None:
    """Test that a missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        _ = open_for_reading(tmp_path / "missing.tsv")
