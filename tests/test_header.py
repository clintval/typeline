from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from time import perf_counter

import pytest

from typeline import CsvReader
from typeline import ExtraColumns
from typeline import TsvReader

from .records import SimpleMetric


@dataclass(frozen=True)
class Region:
    """A record with two named fields and any number of extra columns."""

    name: str
    start: int
    extra: ExtraColumns = ()


@dataclass(frozen=True)
class Tagged:
    """A record with one named field and any number of extra columns."""

    name: str
    extra: ExtraColumns = ()


def test_reader_matches_columns_to_fields_by_header_name(tmp_path: Path) -> None:
    """Test that a header's columns may come in any order."""
    _ = (tmp_path / "test.tsv").write_text("field3\tfield1\tfield2\n0.2\t1\tname\n")

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv") as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_keeps_unnamed_columns_in_extra_columns_in_file_order(tmp_path: Path) -> None:
    """Test that columns a record doesn't name go to its ExtraColumns field, in file order."""
    _ = (tmp_path / "test.tsv").write_text("score\tstart\tcall\tname\n0.9\t10\tHIGH\texon1\n")

    with TsvReader.from_path[Region](tmp_path / "test.tsv") as reader:
        assert list(reader) == [Region("exon1", 10, ("0.9", "HIGH"))]


def test_reader_refuses_a_header_that_repeats_a_column(tmp_path: Path) -> None:
    """Test that a header naming a column twice is refused, since the column would be ambiguous."""
    _ = (tmp_path / "test.tsv").write_text("field1\tfield2\tfield3\tfield1\n")

    message = r"^Columns of header repeat a name on line 1! .* Repeated in header: \['field1'\]\.$"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv")


def test_reader_still_counts_fields_on_each_row_of_a_reordered_file(tmp_path: Path) -> None:
    """Test that a row of a reordered file with the wrong number of fields is still reported."""
    _ = (tmp_path / "test.tsv").write_text("field3\tfield1\tfield2\n0.2\t1\n")

    message = r"^Expected 3 columns but found 2 on line 2 for record type: SimpleMetric\.$"
    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv") as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)


def test_extra_columns_may_repeat_a_name() -> None:
    """Test that columns no field takes may share a name, since they are kept by position."""
    with TsvReader[Tagged](StringIO("info\tname\tinfo\na\tx\tb\n")) as reader:
        assert list(reader) == [Tagged("x", ("a", "b"))]


def test_a_repeated_field_name_is_refused() -> None:
    """Test that a header naming a field twice is refused."""
    with pytest.raises(ValueError, match=r"Repeated in header: \['name'\]\.$"):
        _ = TsvReader[Tagged](StringIO("name\tinfo\tname\n"))


def test_a_wide_header_is_matched_quickly() -> None:
    """Test that matching a header of many extra columns takes linear time."""
    names = [f"sample{index}" for index in range(50_000)]
    header = "\t".join(["name", *names])

    start = perf_counter()
    _ = TsvReader[Tagged](StringIO(f"{header}\n"))

    assert perf_counter() - start < 1.0


@pytest.mark.parametrize(
    "header,detail",
    [
        pytest.param(
            "field1\tfield2",
            "Header: ['field1', 'field2']. Columns of SimpleMetric: ['field1', 'field2', 'field3']."
            + " Missing from header: ['field3'].",
            id="missing",
        ),
        pytest.param(
            "field1\tfield2\tfield3\tfield4",
            "Unexpected in header: ['field4'].",
            id="unexpected",
        ),
        pytest.param(
            "field1\tfield2\tfield4",
            "Missing from header: ['field3']. Unexpected in header: ['field4'].",
            id="missing-and-unexpected",
        ),
    ],
)
def test_reader_names_how_the_header_differs_from_the_dataclass(
    tmp_path: Path, header: str, detail: str
) -> None:
    """Test the header mismatch error shows both headers and how they differ."""
    _ = (tmp_path / "test.txt").write_text(f"{header}\n")

    with pytest.raises(ValueError) as exception:
        _ = TsvReader.from_path[SimpleMetric](tmp_path / "test.txt")

    assert str(exception.value).startswith("Columns of header do not match fields of ")
    assert detail in str(exception.value)


def test_a_header_mismatch_names_columns_fields_and_line() -> None:
    """Test that a header that does not match is reported in columns and fields, with its line."""
    message = r"^Columns of header do not match fields of SimpleMetric on line 2! Header: "
    with pytest.raises(ValueError, match=message):
        _ = CsvReader[SimpleMetric](StringIO("# a comment\nfield1,field2\n"))
