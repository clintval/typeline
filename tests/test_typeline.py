from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import pytest

from typeline import CsvStructReader
from typeline import CsvStructWriter
from typeline import TsvStructReader
from typeline import TsvStructWriter


@dataclass
class SimpleMetric:
    """A simple metric for unit testing purposes."""

    field1: int = field(kw_only=True)
    field2: str = field(kw_only=True)
    field3: float | None = field(kw_only=True)


@dataclass
class ComplexMetric:
    """A complex nested metric for unit testing purposes."""

    field1: int = field(kw_only=True)
    field2: str = field(kw_only=True)
    field3: float | None = field(kw_only=True)
    field4: list[int] = field(kw_only=True)
    field5: set[int] = field(kw_only=True)
    field6: tuple[int, ...] = field(kw_only=True)
    field7: dict[str, int] = field(kw_only=True)
    field8: SimpleMetric = field(kw_only=True)
    field9: dict[str, SimpleMetric] = field(kw_only=True)
    field10: bool | None = field(kw_only=True)
    field11: bool | None = field(kw_only=True)
    field12: int | float | None = field(kw_only=True)


def test_writer_raises_exception_on_non_dataclass(tmp_path: Path) -> None:
    """Test that the writer will raise an exception for non-dataclasses."""

    class MyTest:
        """A test metric."""

    with pytest.raises(AssertionError, match="record_type is not a dataclass but must be!"):
        CsvStructWriter.from_path(tmp_path / "test.txt", MyTest)


def test_csv_writer_is_set_to_use_comma(tmp_path: Path) -> None:
    """Test that the CSV writer is set to use a comma."""
    with CsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"

    with CsvStructWriter(open(tmp_path / "test.txt", "w"), SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"


def test_tsv_writer_is_set_to_use_tab(tmp_path: Path) -> None:
    """Test that the TSV writer is set to use a tab."""
    assert TsvStructWriter.delimiter == "\t"
    with TsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"

    with TsvStructWriter(open(tmp_path / "test.txt", "w"), SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"


def test_writer_will_write_a_header(tmp_path: Path) -> None:
    """Test that the writer will write a header when asked to."""
    with CsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
    assert (tmp_path / "test.txt").read_text() == "field1,field2,field3\n"


def test_writer_will_allow_a_custom_delimiter(tmp_path: Path) -> None:
    """Test that the writer will write with a tab delimiter."""
    with TsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
    assert (tmp_path / "test.txt").read_text() == "field1\tfield2\tfield3\n"


def test_writer_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the writer will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "1\t'my\tname'\t0.2\n"


def test_writer_will_write_a_complicated_record(tmp_path: Path) -> None:
    """Test that the writer will write a complicated record with nested fields."""
    metric = ComplexMetric(
        field1=1,
        field2="my\tname",
        field3=0.2,
        field4=[1, 2, 3],
        field5=set([3, 4, 5]),
        field6=(5, 6, 7),
        field7={"field1": 1, "field2": 2},
        field8=SimpleMetric(field1=10, field2="hi-mom", field3=None),
        field9={
            "first": SimpleMetric(field1=2, field2="hi-dad", field3=0.2),
            "second": SimpleMetric(field1=3, field2="hi-all", field3=0.3),
        },
        field10=True,
        field11=None,
        field12=0.2,
    )
    with TsvStructWriter.from_path(tmp_path / "test.txt", ComplexMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "\t".join([
        "1",
        "'my\tname'",
        "0.2",
        "[1, 2, 3]",
        "[3, 4, 5]",
        "[5, 6, 7]",
        '{"field1": 1, "field2": 2}',
        '{"field1": 10, "field2": "hi-mom", "field3": null}',
        ", ".join([
            r'{"first": {"field1": 2, "field2": "hi-dad", "field3": 0.2}',
            r'"second": {"field1": 3, "field2": "hi-all", "field3": 0.3}}',
        ]),
        "true",
        "null",
        "0.2\n",
    ])


def test_csv_reader_is_set_to_use_comma(tmp_path: Path) -> None:
    """Test that the CSV reader is set to use a comma."""
    with CsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "1,name,0.2\n",
    ])

    with CsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]

    with CsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "1,name,0.2\n",
    ])

    with CsvStructReader(open(tmp_path / "test.txt", "r"), SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_tsv_reader_is_set_to_use_tab(tmp_path: Path) -> None:
    """Test that the TSV reader is set to use a tab."""
    with TsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1\tfield2\tfield3",
        "1\tname\t0.2\n",
    ])

    with TsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]

    with TsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1\tfield2\tfield3",
        "1\tname\t0.2\n",
    ])

    with TsvStructReader(open(tmp_path / "test.txt", "r"), SimpleMetric) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="name", field3=0.2)]


def test_reader_raises_exception_on_non_dataclass(tmp_path: Path) -> None:
    """Test that the reader will raise an exception for non-dataclasses."""

    class MyTest:
        """A test metric."""

    (tmp_path / "test.txt").touch()

    with pytest.raises(AssertionError, match="record_type is not a dataclass but must be!"):
        CsvStructReader.from_path(tmp_path / "test.txt", MyTest)


def test_reader_raises_exception_when_header_is_wrong(tmp_path: Path) -> None:
    """Test that the reader will raise an exception when the header is wrong."""
    (tmp_path / "test.txt").write_text("field10,field11,field13\n")

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        CsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric)


def test_reader_will_escape_text_when_delimiter_is_used(tmp_path: Path) -> None:
    """Test that the reader will escape text when a delimiter is used in a field."""
    metric = SimpleMetric(field1=1, field2="my\tname", field3=0.2)
    with TsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "1\t'my\tname'\t0.2\n"

    with TsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric, has_header=False) as reader:
        assert list(reader) == [SimpleMetric(field1=1, field2="my\tname", field3=0.2)]


def test_reader_will_write_a_complicated_record(tmp_path: Path) -> None:
    """Test that the reader will write a complicated record with nested fields."""
    metric = ComplexMetric(
        field1=1,
        field2="my\tname",
        field3=0.2,
        field4=[1, 2, 3],
        field5=set([3, 4, 5]),
        field6=(5, 6, 7),
        field7={"field1": 1, "field2": 2},
        field8=SimpleMetric(field1=10, field2="hi-mom", field3=None),
        field9={
            "first": SimpleMetric(field1=2, field2="hi-dad", field3=0.2),
            "second": SimpleMetric(field1=3, field2="hi-all", field3=0.3),
        },
        field10=True,
        field11=None,
        field12=1,
    )
    with TsvStructWriter.from_path(tmp_path / "test.txt", ComplexMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.write(metric)
    assert (tmp_path / "test.txt").read_text() == "\t".join([
        "1",
        "'my\tname'",
        "0.2",
        "[1, 2, 3]",
        "[3, 4, 5]",
        "[5, 6, 7]",
        '{"field1": 1, "field2": 2}',
        '{"field1": 10, "field2": "hi-mom", "field3": null}',
        ", ".join([
            r'{"first": {"field1": 2, "field2": "hi-dad", "field3": 0.2}',
            r'"second": {"field1": 3, "field2": "hi-all", "field3": 0.3}}',
        ]),
        "true",
        "null",
        "1\n",
    ])

    with TsvStructReader.from_path(
        tmp_path / "test.txt", ComplexMetric, has_header=False
    ) as reader:
        assert list(reader) == [metric]


def test_csv_reader_ignores_comments_and_blank_lines(tmp_path: Path) -> None:
    """Test that the CSV reader is set to use a comma."""
    with CsvStructWriter.from_path(tmp_path / "test.txt", SimpleMetric) as writer:
        assert (tmp_path / "test.txt").read_text() == ""
        writer.writeheader()
        writer._handle.write("# this is a comment\n")
        writer._handle.write("#and this is a comment too!\n")
        writer.write(SimpleMetric(field1=1, field2="name", field3=0.2))
        writer._handle.write("\n")
        writer._handle.write("  \n")
        writer.write(SimpleMetric(field1=2, field2="name2", field3=0.3))
    assert (tmp_path / "test.txt").read_text() == "\n".join([
        "field1,field2,field3",
        "# this is a comment",
        "#and this is a comment too!",
        "1,name,0.2",
        "",
        "  ",
        "2,name2,0.3\n",
    ])

    with CsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader:
        assert list(reader) == [
            SimpleMetric(field1=1, field2="name", field3=0.2),
            SimpleMetric(field1=2, field2="name2", field3=0.3),
        ]


def test_reader_raises_exception_for_missing_fields(tmp_path: Path) -> None:
    """Test the reader raises an exception for missing fields."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\n",
            "1\tname\t0.2\n",
        ])
    )

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        TsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric)


def test_reader_raises_exception_for_extra_fields(tmp_path: Path) -> None:
    """Test the reader raises an exception for extra fields."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\tfield3\tfield4\n",
            "1\tname\t0.2\thi-five\n",
        ])
    )

    with pytest.raises(ValueError, match="Fields of header do not match fields of dataclass!"):
        TsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric)


def test_reader_raises_exception_for_failed_type_coercion(tmp_path: Path) -> None:
    """Test the reader raises an exception for failed type coercion."""
    (tmp_path / "test.txt").write_text(
        "\n".join([
            "field1\tfield2\tfield3\n",
            "1\tname\tBOMB\n",
        ])
    )

    with (
        TsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric) as reader,
        pytest.raises(ValueError, match=r"Expecting value: line 1 column 38 \(char 37\)"),
    ):
        list(reader)


def test_reader_can_read_empty_file_ok(tmp_path: Path) -> None:
    """Test the reader can read an empty file if asked to."""
    (tmp_path / "test.txt").touch()

    with (
        TsvStructReader.from_path(tmp_path / "test.txt", SimpleMetric, has_header=False) as reader,
    ):
        assert list(reader) == []
