"""Real world ways to use typeline, each written as it would appear in an application."""

import gzip
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from os import linesep
from pathlib import Path
from typing import TextIO
from typing import cast

import pytest
from typing_extensions import Unpack
from typing_extensions import override

from typeline import Codecs
from typeline import CsvReader
from typeline import CsvWriter
from typeline import DelimitedDataReader
from typeline import DelimitedDataWriter
from typeline import FieldCodec
from typeline import ReaderOptions
from typeline import RecordType
from typeline import TsvReader
from typeline import TsvWriter
from typeline.codecs import boolean
from typeline.codecs import delimited
from typeline.codecs import key_value
from typeline.codecs import nullable


@dataclass(frozen=True)
class Sample:
    """A row of a sequencing sample sheet."""

    name: str
    reads: int
    purity: float | None


SAMPLES: list[Sample] = [Sample("tumor", 1_200, 0.62), Sample("normal", 980, None)]


def test_sample_sheet_round_trips_through_a_tsv(tmp_path: Path) -> None:
    """Write a sample sheet with a header, then read it back."""
    with TsvWriter.from_path[Sample](tmp_path / "samples.tsv") as writer:
        writer.write_header()
        for sample in SAMPLES:
            writer.write(sample)

    assert (tmp_path / "samples.tsv").read_text() == (
        "name\treads\tpurity\ntumor\t1200\t0.62\nnormal\t980\t\n"
    )

    with TsvReader.from_path[Sample](tmp_path / "samples.tsv") as reader:
        assert list(reader) == SAMPLES


def test_gzipped_sample_sheet_is_read_from_an_open_handle(tmp_path: Path) -> None:
    """Read a gzipped sample sheet by handing the reader any open text stream."""
    with TsvWriter[Sample](gzip.open(tmp_path / "samples.tsv.gz", "wt", newline="")) as writer:
        writer.write_header()
        for sample in SAMPLES:
            writer.write(sample)

    with TsvReader[Sample](gzip.open(tmp_path / "samples.tsv.gz", "rt")) as reader:
        assert list(reader) == SAMPLES


def test_pipeline_output_without_a_header_with_comments_and_na(tmp_path: Path) -> None:
    """Read tool output that has no header, comment lines, and NA for missing values."""
    _ = (tmp_path / "raw.csv").write_text(
        "# produced by a pipeline\ntumor,1200,0.62\nnormal,980,NA\n"
    )

    with CsvReader.from_path[Sample](
        tmp_path / "raw.csv", header=False, comment_prefixes={"#"}, none_field="NA"
    ) as reader:
        assert list(reader) == SAMPLES


class Strand(str, Enum):
    """The strand of a transcript."""

    Plus = "+"
    Minus = "-"


@dataclass(frozen=True)
class Exon:
    """An exon of a transcript."""

    start: int
    end: int


@dataclass
class Transcript:
    """A transcript with structured fields, which are written as JSON by default."""

    name: str
    strand: Strand
    exons: list[Exon]
    tags: dict[str, int]
    aliases: set[str]


def test_transcript_table_stores_structured_fields_as_json(tmp_path: Path) -> None:
    """Store lists, dicts, sets, nested dataclasses, and enums with no configuration."""
    transcript = Transcript(
        "TP53-201", Strand.Minus, [Exon(1, 5), Exon(9, 12)], {"mane": 1}, {"p53"}
    )

    with CsvWriter.from_path[Transcript](tmp_path / "transcripts.csv") as writer:
        writer.write(transcript)

    assert (tmp_path / "transcripts.csv").read_text() == (
        'TP53-201,-,"[{""start"":1,""end"":5},{""start"":9,""end"":12}]","{""mane"":1}","[""p53""]"\n'
    )

    with CsvReader.from_path[Transcript](tmp_path / "transcripts.csv", header=False) as reader:
        assert list(reader) == [transcript]


@dataclass
class Visit:
    """A clinical visit whose fields use formats other than JSON."""

    patient: str
    seen: date
    cost: Decimal
    consented: bool
    blocks: list[int]


VISIT_CODECS: Codecs = {
    date: FieldCodec(from_text=date.fromisoformat, into_text=date.isoformat),
    Decimal: FieldCodec(from_text=Decimal, into_text=str),
    bool: boolean(true="Y", false="N"),
    list[int]: delimited(int, sep=";", trailing_sep=True),
}


def test_clinical_visits_use_codecs_for_dates_decimals_and_flags(tmp_path: Path) -> None:
    """Read and write dates, exact decimals, Y/N flags, and delimited lists with field codecs."""
    visit = Visit("P-001", date(2026, 9, 29), Decimal("120.50"), True, [3, 1, 4])

    with TsvWriter.from_path[Visit](tmp_path / "visits.tsv", codecs=VISIT_CODECS) as writer:
        writer.write(visit)

    assert (tmp_path / "visits.tsv").read_text() == "P-001\t2026-09-29\t120.50\tY\t3;1;4;\n"

    with TsvReader.from_path[Visit](
        tmp_path / "visits.tsv", header=False, codecs=VISIT_CODECS
    ) as reader:
        assert list(reader) == [visit]


class Interval:
    """A genomic interval that msgspec does not know how to convert on its own."""

    def __init__(self, start: int, end: int) -> None:
        """Build an interval from its start and end."""
        self.start: int = start
        self.end: int = end

    @override
    def __eq__(self, other: object) -> bool:
        """Compare intervals by their start and end."""
        return isinstance(other, Interval) and (self.start, self.end) == (other.start, other.end)

    @override
    def __hash__(self) -> int:
        """Hash an interval by its start and end."""
        return hash((self.start, self.end))


def encode_interval(obj: object) -> object:
    """Encode an interval as a two-element list, wherever it appears in a record."""
    if isinstance(obj, Interval):
        return [obj.start, obj.end]
    raise NotImplementedError(type(obj))


def decode_interval(type_: type, obj: object) -> object:
    """Decode an interval from a two-element list, wherever it appears in a record."""
    if type_ is Interval and isinstance(obj, list):
        start, end = cast(list[int], obj)
        return Interval(start, end)
    raise NotImplementedError(type_)


@dataclass
class CaptureTarget:
    """A gene targeted for capture, with intervals nested inside its fields."""

    gene: str
    intervals: list[Interval]
    by_exon: dict[str, Interval]


def test_capture_targets_use_hooks_for_nested_custom_types(tmp_path: Path) -> None:
    """Encode and decode a custom type nested in lists and dicts with msgspec hooks."""
    target = CaptureTarget("BRCA1", [Interval(1, 9)], {"e1": Interval(1, 4)})

    with TsvWriter.from_path[CaptureTarget](
        tmp_path / "targets.tsv", enc_hook=encode_interval
    ) as writer:
        writer.write(target)

    assert (tmp_path / "targets.tsv").read_text() == 'BRCA1\t[[1,9]]\t"{""e1"":[1,4]}"\n'

    with TsvReader.from_path[CaptureTarget](
        tmp_path / "targets.tsv", header=False, dec_hook=decode_interval
    ) as reader:
        assert list(reader) == [target]


class VcfLikeReader(TsvReader[RecordType]):
    """A reader with the defaults of a VCF-like format: meta lines, no header, and `.`."""

    @override
    def __init__(self, handle: TextIO, /, **options: Unpack[ReaderOptions]) -> None:
        """Build a reader with VCF-like defaults for any options not given."""
        _ = options.setdefault("header", False)
        _ = options.setdefault("comment_prefixes", {"##", "#CHROM"})
        _ = options.setdefault("none_field", ".")
        _ = options.setdefault("codecs", {dict[str, str]: key_value()})
        super().__init__(handle, **options)


@dataclass
class Site:
    """A variant site with an INFO field of key-value pairs."""

    chrom: str
    pos: int
    ident: str | None
    info: dict[str, str]


def test_vcf_sites_are_read_with_a_domain_reader(tmp_path: Path) -> None:
    """Read VCF-like sites with a reader subclass that sets the format's defaults."""
    _ = (tmp_path / "sites.tsv").write_text(
        "##fileformat=VCFv4.3\n"
        + "#CHROM\tPOS\tID\tINFO\n"
        + "chr1\t100\trs1\tDP=10;AF=0.5\n"
        + "chr2\t200\t.\tDP=3\n"
    )

    with VcfLikeReader.from_path[Site](tmp_path / "sites.tsv") as reader:
        assert list(reader) == [
            Site("chr1", 100, "rs1", {"DP": "10", "AF": "0.5"}),
            Site("chr2", 200, None, {"DP": "3"}),
        ]


@dataclass
class Gff3Feature:
    """A GFF3 feature, trimmed to the columns this example needs."""

    seqid: str
    type: str
    start: int
    end: int
    attributes: dict[str, str]


def test_gff3_attributes_are_read_into_a_dict(tmp_path: Path) -> None:
    """Read and write GFF3 attributes as a dict with the key-value codec."""
    codecs: Codecs = {dict[str, str]: key_value()}
    _ = (tmp_path / "genes.gff3").write_text(
        "##gff-version 3\nchr17\tgene\t7661779\t7687538\tID=g1;Name=TP53;\n"
    )

    with TsvReader.from_path[Gff3Feature](
        tmp_path / "genes.gff3", header=False, comment_prefixes={"##"}, codecs=codecs
    ) as reader:
        features = list(reader)

    assert features == [
        Gff3Feature("chr17", "gene", 7661779, 7687538, {"ID": "g1", "Name": "TP53"})
    ]

    with TsvWriter.from_path[Gff3Feature](tmp_path / "out.gff3", codecs=codecs) as writer:
        writer.write(features[0])

    assert (tmp_path / "out.gff3").read_text() == "chr17\tgene\t7661779\t7687538\tID=g1;Name=TP53\n"


@dataclass(frozen=True)
class Rgb:
    """A display color for a genomic feature."""

    r: int
    g: int
    b: int

    @classmethod
    def from_string(cls, text: str) -> "Rgb":
        """Read a color from its text, e.g. `101,2,32`."""
        r, g, b = map(int, text.split(","))
        return cls(r, g, b)

    @override
    def __str__(self) -> str:
        """Write a color into its text, e.g. `101,2,32`."""
        return f"{self.r},{self.g},{self.b}"


@dataclass
class ColoredFeature:
    """A genomic feature with a color, where `0` means no color, and block sizes."""

    chrom: str
    start: int
    end: int
    color: Rgb | None
    block_sizes: list[int]


def test_features_read_a_missing_color_and_block_sizes(tmp_path: Path) -> None:
    """Read a field's own missing marker with nullable and a trailing comma with delimited."""
    codecs: Codecs = {
        Rgb: nullable(FieldCodec(from_text=Rgb.from_string, into_text=str), missing="0"),
        list[int]: delimited(int),
    }
    _ = (tmp_path / "features.tsv").write_text("chr1\t10\t20\t255,0,0\t4,6,\nchr1\t30\t40\t0\t10\n")

    with TsvReader.from_path[ColoredFeature](
        tmp_path / "features.tsv", header=False, codecs=codecs
    ) as reader:
        assert list(reader) == [
            ColoredFeature("chr1", 10, 20, Rgb(255, 0, 0), [4, 6]),
            ColoredFeature("chr1", 30, 40, None, [10]),
        ]


class PipeReader(DelimitedDataReader[RecordType], delimiter="|"):
    """A reader of pipe-delimited data."""


class PipeWriter(DelimitedDataWriter[RecordType], delimiter="|"):
    """A writer of pipe-delimited data."""


def test_pipe_delimited_export_uses_a_new_delimiter(tmp_path: Path) -> None:
    """Define a reader and writer for a new delimiter with a class keyword."""
    with PipeWriter.from_path[Sample](tmp_path / "samples.psv") as writer:
        writer.write(SAMPLES[0])

    assert (tmp_path / "samples.psv").read_text() == "tumor|1200|0.62\n"

    with PipeReader.from_path[Sample](tmp_path / "samples.psv", header=False) as reader:
        assert list(reader) == [SAMPLES[0]]


def test_filtering_a_tsv_into_a_csv_as_a_stream(tmp_path: Path) -> None:
    """Stream records from one format to another, keeping only some of them."""
    with TsvWriter.from_path[Sample](tmp_path / "samples.tsv") as writer:
        writer.write_header()
        for sample in SAMPLES:
            writer.write(sample)

    with (
        TsvReader.from_path[Sample](tmp_path / "samples.tsv") as reader,
        CsvWriter.from_path[Sample](tmp_path / "pure.csv") as writer,
    ):
        writer.write_header()
        for sample in reader:
            if sample.purity is not None and sample.purity > 0.5:
                writer.write(sample)

    assert (tmp_path / "pure.csv").read_text() == "name,reads,purity\ntumor,1200,0.62\n"


def test_writing_a_report_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    """Write records to standard output, as a command line tool would."""
    import sys

    writer = TsvWriter[Sample](sys.stdout)
    writer.write_header()
    writer.write(SAMPLES[0])

    assert capsys.readouterr().out == (
        "name\treads\tpurity\ntumor\t1200\t0.62\n".replace("\n", linesep)
    )


def test_error_messages_explain_what_went_wrong(tmp_path: Path) -> None:
    """Show the errors a user sees for a header mismatch and for text a codec cannot read."""
    _ = (tmp_path / "short-header.tsv").write_text("name\treads\n")
    with pytest.raises(ValueError, match=r"Missing from header: \['purity'\]\.$"):
        _ = TsvReader.from_path[Sample](tmp_path / "short-header.tsv")

    _ = (tmp_path / "visits.tsv").write_text("P-001\tSeptember 29\t120.50\tY\t3;\n")
    message = r"^Could not read field 'seen' of type date from text 'September 29' on line 1!$"
    with (
        TsvReader.from_path[Visit](
            tmp_path / "visits.tsv", header=False, codecs=VISIT_CODECS
        ) as reader,
        pytest.raises(ValueError, match=message),
    ):
        _ = list(reader)
