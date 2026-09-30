from dataclasses import dataclass
from io import StringIO
from os import linesep
from pathlib import Path
from typing import Any

import pytest

from typeline import CsvReader
from typeline import CsvWriter
from typeline import TsvReader


@dataclass
class Note:
    """A record with free text that can hold line breaks."""

    name: str
    text: str


def test_from_path_reads_a_file_that_starts_with_a_byte_order_mark(tmp_path: Path) -> None:
    """Test that a UTF-8 byte order mark, as Excel writes, is not read as part of the header."""
    (tmp_path / "excel.csv").write_bytes(b"\xef\xbb\xbfname,text\nx,y\n")

    with CsvReader.from_path[Note](tmp_path / "excel.csv") as reader:
        assert list(reader) == [Note("x", "y")]


def test_from_path_keeps_line_breaks_inside_quoted_fields(tmp_path: Path) -> None:
    """Test that a quoted field keeps its own line breaks exactly, as the csv module requires."""
    (tmp_path / "notes.csv").write_bytes(b'name,text\r\nx,"one\r\ntwo"\r\n')

    with CsvReader.from_path[Note](tmp_path / "notes.csv") as reader:
        assert list(reader) == [Note("x", "one\r\ntwo")]


def test_from_path_writes_utf8_without_translating_line_endings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that the writer opens its file as UTF-8 with newline="", so no line ends twice."""
    options: list[dict[str, Any]] = []
    original_open = Path.open

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
