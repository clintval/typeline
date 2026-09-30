from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from typeline import TsvReader


@dataclass(frozen=True)
class Sample:
    """A small record for testing when files are closed."""

    name: str
    reads: int


def test_reader_from_a_path_closes_its_file_once_read_to_the_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that list() on a reader from from_path reads every record and closes its file."""
    path = tmp_path / "samples.tsv"
    _ = path.write_text("name\treads\ntumor\t1200\n")
    opened: list[Any] = []
    original_open = Path.open

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
    original_open = Path.open

    def recording_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original_open(self, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", recording_open)

    with pytest.raises(Exception, match="many"):
        _ = list(TsvReader.from_path[Sample](path))

    assert opened
    assert all(handle.closed for handle in opened)
