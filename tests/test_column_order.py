from dataclasses import dataclass
from pathlib import Path

import pytest

from typeline import ExtraColumns
from typeline import TsvReader

from .conftest import SimpleMetric


@dataclass(frozen=True)
class Region:
    """A record with two named fields and any number of extra columns."""

    name: str
    start: int
    extra: ExtraColumns = ()


def test_reader_matches_columns_to_fields_by_header_name(tmp_path: Path) -> None:
    """Test that a header's columns may come in any order."""
    (tmp_path / "test.tsv").write_text("field3\tfield1\tfield2\n0.2\t1\tname\n")

    with TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv") as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_keeps_unnamed_columns_in_extra_columns_in_file_order(tmp_path: Path) -> None:
    """Test that columns a record doesn't name go to its ExtraColumns field, in file order."""
    (tmp_path / "test.tsv").write_text("score\tstart\tcall\tname\n0.9\t10\tHIGH\texon1\n")

    with TsvReader.from_path[Region](tmp_path / "test.tsv") as reader:
        assert list(reader) == [Region("exon1", 10, ("0.9", "HIGH"))]


def test_reader_refuses_a_header_that_repeats_a_column(tmp_path: Path) -> None:
    """Test that a header naming a column twice is refused, since the column would be ambiguous."""
    (tmp_path / "test.tsv").write_text("field1\tfield2\tfield3\tfield1\n")

    message = r"^Fields of header repeat a name! Header: .* Repeated in header: \['field1'\]\.$"
    with pytest.raises(ValueError, match=message):
        _ = TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv")


def test_reader_still_counts_fields_on_each_row_of_a_reordered_file(tmp_path: Path) -> None:
    """Test that a row of a reordered file with the wrong number of fields is still reported."""
    (tmp_path / "test.tsv").write_text("field3\tfield1\tfield2\n0.2\t1\n")

    message = r"^Expected 3 fields but found 2 on line 2 for record type: SimpleMetric\.$"
    with (
        TsvReader.from_path[SimpleMetric](tmp_path / "test.tsv") as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)
