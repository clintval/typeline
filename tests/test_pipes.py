import bz2
import errno
import gzip
import lzma
import os
import sys
import threading
from collections.abc import Callable
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

from typeline import TsvReader
from typeline import TsvWriter

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Windows has no named pipes")

TIMEOUT = 10.0


@dataclass(frozen=True)
class Sample:
    """A small record for testing pipes read and written by from_path."""

    name: str
    reads: int


TEXT = "name\treads\ntumor\t1200\nnormal\t980\n"
RECORDS = [Sample("tumor", 1200), Sample("normal", 980)]
COMPRESSORS: list[tuple[str, Callable[[bytes], bytes]]] = [
    ("plain", lambda data: data),
    ("gzip", gzip.compress),
    ("bzip2", bz2.compress),
    ("xz", lzma.compress),
]


def release(fifo: Path, flags: int, done: threading.Event) -> None:
    """Open and close the other end of a FIFO until done, so no open of it waits forever."""
    while not done.wait(0.05):
        try:
            os.close(os.open(fifo, flags | os.O_NONBLOCK))
        except OSError as error:
            if error.errno != errno.ENXIO:
                raise


@contextmanager
def running(target: Callable[[], None]) -> Generator[list[BaseException]]:
    """Run a function in a thread, collecting its errors, and wait a bounded time for it."""
    errors: list[BaseException] = []

    def run() -> None:
        try:
            target()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield errors
    finally:
        thread.join(TIMEOUT)
    assert not thread.is_alive()


@contextmanager
def feeding(fifo: Path, write: Callable[[], None]) -> Generator[None]:
    """Write into a FIFO from a thread, then keep any reader that reopens it from waiting."""
    done = threading.Event()

    def write_then_release() -> None:
        try:
            write()
        finally:
            release(fifo, os.O_WRONLY, done)

    with running(write_then_release) as errors:
        try:
            yield
        finally:
            done.set()
    assert not errors


@contextmanager
def writing_to_fifo(fifo: Path, *chunks: bytes) -> Generator[None]:
    """Write chunks into a FIFO from a thread, unbuffered so each arrives on its own."""

    def write() -> None:
        with fifo.open("wb", buffering=0) as handle:
            for chunk in chunks:
                _ = handle.write(chunk)

    with feeding(fifo, write):
        yield


@contextmanager
def reading_from_fifo(fifo: Path, into: bytearray) -> Generator[None]:
    """Read all of a FIFO from a thread into a buffer."""

    def read() -> None:
        with fifo.open("rb") as handle:
            into.extend(handle.read())

    with running(read) as errors:
        yield
    assert not errors


@pytest.fixture
def fifo(tmp_path: Path) -> Path:
    """A named pipe in a temporary directory."""
    path = tmp_path / "samples.tsv"
    os.mkfifo(path)
    return path


@pytest.mark.parametrize("_name,compress", COMPRESSORS)
def test_from_path_reads_a_fifo(fifo: Path, _name: str, compress: Callable[[bytes], bytes]) -> None:
    """Test that from_path reads plain and compressed data from a named pipe."""
    with writing_to_fifo(fifo, compress(TEXT.encode())):
        records = list(TsvReader.from_path[Sample](fifo))

    assert records == RECORDS


@pytest.mark.parametrize("_name,compress", COMPRESSORS)
def test_from_path_reads_a_fifo_whose_first_bytes_arrive_one_at_a_time(
    fifo: Path, _name: str, compress: Callable[[bytes], bytes]
) -> None:
    """Test that compression is detected when the first bytes of a pipe arrive in pieces."""
    data = compress(TEXT.encode())

    with writing_to_fifo(fifo, *(data[i : i + 1] for i in range(12)), data[12:]):
        records = list(TsvReader.from_path[Sample](fifo))

    assert records == RECORDS


@pytest.mark.parametrize("_name,compress", COMPRESSORS)
def test_from_path_reads_a_dev_fd_path(_name: str, compress: Callable[[bytes], bytes]) -> None:
    """Test that from_path reads plain and compressed data from the /dev/fd path of a pipe."""
    read_end, write_end = os.pipe()

    def write() -> None:
        with os.fdopen(write_end, "wb") as handle:
            _ = handle.write(compress(TEXT.encode()))

    try:
        with running(write) as errors:
            records = list(TsvReader.from_path[Sample](f"/dev/fd/{read_end}"))
    finally:
        os.close(read_end)

    assert not errors
    assert records == RECORDS


@pytest.mark.parametrize("suffix,decompress", [("", bytes), (".gz", gzip.decompress)])
def test_from_path_writes_a_fifo(
    tmp_path: Path, suffix: str, decompress: Callable[[bytes], bytes]
) -> None:
    """Test that from_path writes plain and compressed data into a named pipe."""
    fifo = tmp_path / f"samples.tsv{suffix}"
    os.mkfifo(fifo)
    received = bytearray()

    with reading_from_fifo(fifo, received), TsvWriter.from_path[Sample](fifo) as writer:
        writer.write_header()
        for record in RECORDS:
            writer.write(record)

    assert decompress(bytes(received)).decode() == TEXT.replace("\n", os.linesep)


@pytest.mark.parametrize("suffix,decompress", [("", bytes), (".gz", gzip.decompress)])
def test_from_path_writes_a_dev_fd_path(
    tmp_path: Path, suffix: str, decompress: Callable[[bytes], bytes]
) -> None:
    """Test that from_path writes plain and compressed data into the /dev/fd path of a pipe."""
    read_end, write_end = os.pipe()
    received = bytearray()

    def read() -> None:
        with os.fdopen(read_end, "rb") as handle:
            received.extend(handle.read())

    link = tmp_path / f"samples.tsv{suffix}"
    link.symlink_to(f"/dev/fd/{write_end}")
    with running(read) as errors:
        try:
            with TsvWriter.from_path[Sample](link if suffix else f"/dev/fd/{write_end}") as writer:
                writer.write_header()
                for record in RECORDS:
                    writer.write(record)
        finally:
            os.close(write_end)

    assert not errors
    assert decompress(bytes(received)).decode() == TEXT.replace("\n", os.linesep)


def test_a_refused_writer_does_not_open_a_fifo(fifo: Path) -> None:
    """Test that a writer refused for its options fails without waiting on a pipe's reader."""
    done = threading.Event()

    with running(lambda: release(fifo, os.O_RDONLY, done)):
        try:
            with pytest.raises(ValueError, match="comment_prefixes"):
                _ = TsvWriter.from_path[Sample](fifo, comment_prefixes=[])
        finally:
            done.set()


def test_records_round_trip_through_a_fifo(fifo: Path) -> None:
    """Test that a header, comments, and many records written into a pipe are all read back."""
    records = [Sample(f"sample{index}", index) for index in range(5_000)]
    comments: list[str] = []

    def write() -> None:
        with TsvWriter.from_path[Sample](fifo) as writer:
            writer.write_comment("# first comment")
            writer.write_header()
            for index, record in enumerate(records):
                if index % 1_000 == 0:
                    writer.write_comment(f"# at {index}")
                writer.write(record)

    with feeding(fifo, write):
        read = list(
            TsvReader.from_path[Sample](
                fifo, comment_prefixes=["#"], on_comment=lambda c: comments.append(c.text)
            )
        )

    assert read == records
    assert comments == ["# first comment", *(f"# at {index}" for index in range(0, 5_000, 1_000))]
