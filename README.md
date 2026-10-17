# typeline

[![PyPi Release](https://badge.fury.io/py/typeline.svg)](https://badge.fury.io/py/typeline)
[![CI](https://github.com/clintval/typeline/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/clintval/typeline/actions/workflows/tests.yml?query=branch%3Amain)
[![Python Versions](https://img.shields.io/badge/python-3.11_|_3.12_|_3.13_|_3.14-blue)](https://github.com/clintval/typeline)
[![basedpyright](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com/latest/)
[![mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://docs.astral.sh/uv/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://docs.astral.sh/ruff/)

Write dataclasses to delimited text formats and read them back again.

Headers are matched to fields by name, values are typed by msgspec, and custom formats, comments, extra columns, and compressed files are supported.

## Installation

The package can be installed with `pip`:

```console
pip install typeline
```

## Quickstart

### Defining a Record

```python
from dataclasses import dataclass

@dataclass
class MyData:
    field1: int
    field2: str
    field3: float | None
```

### Writing

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from typeline import TsvWriter

temp_dir = TemporaryDirectory()
path = Path(temp_dir.name) / "data.tsv"
records = [MyData(10, "test1", 0.2), MyData(20, "test2", None)]

with TsvWriter.from_path[MyData](path, header=True) as writer:
    writer.write_all(records)

assert path.read_text() == "field1\tfield2\tfield3\n10\ttest1\t0.2\n20\ttest2\t\n"
```

`write_all` writes records in order, and `write` writes one.
With `header=True`, a writer writes its header once: before its first record, or alone when there are no records.
Without it, the header is written by calling `write_header`.

### Reading

```python
from typeline import TsvReader

with TsvReader.from_path[MyData](path) as reader:
    assert list(reader) == records
```

A reader expects a header by default, and matches its columns to fields by name, in any order.
Pass `header=False` to read columns in field order.
A reader from `from_path` closes its file once it is read to the end, so reading everything into a list needs no `with` block.

```python
assert list(TsvReader.from_path[MyData](path)) == records
```

### Any Text Stream

To use an open text stream instead of a path, subscript the reader or writer class itself.

Leaving the `with` block closes the stream.

```python
from io import StringIO

with TsvReader[MyData](StringIO("10\ttest1\t0.2\n"), header=False) as reader:
    assert list(reader) == [MyData(10, "test1", 0.2)]
```

### Decoding a Line

A reader's `decode` reads one record from a line of text, just as the reader reads each record of its file, with its delimiter and other options.
It reads nothing from the reader's stream, so one reader can decode many lines, which is much faster than building a reader for each.
A reader built on an empty stream has no header, so it decodes columns in field order.
A blank line, a comment, or text holding more than one record is refused with a `ValueError`.

```python
reader = TsvReader[MyData](StringIO())

assert reader.decode("10\ttest1\t0.2\n") == MyData(10, "test1", 0.2)
```

### Files

`from_path` reads and writes UTF-8, skips a byte order mark, and ends lines with `os.linesep`.
It reads gzip, bzip2, and xz files, which it recognizes by their contents, and writes them when the path ends in `.gz`, `.bz2`, or `.xz`.
It opens a path only once, so it also reads and writes pipes such as FIFOs, `/dev/stdin`, and `/dev/stdout`.

```python
with TsvWriter.from_path[MyData](f"{path}.gz") as writer:
    writer.write(MyData(10, "test1", 0.2))

with TsvReader.from_path[MyData](f"{path}.gz", header=False) as reader:
    assert list(reader) == [MyData(10, "test1", 0.2)]
```

### Missing Values

`None` is written as the `none_field`, an empty field by default.
When read, an empty field is `None` if the field allows `None`, and `""` if it is a `str`.
Set `none_field`, e.g. to `"NA"`, when an optional text field must tell `""` and `None` apart.
A codec's `missing` text, as `typeline.codecs.nullable` sets, takes the place of `none_field` for its fields.

### Comments

A reader skips lines that start with any of its `comment_prefixes`, and hands each one to `on_comment` as a `Comment` with its line number.
A writer writes comments with `write_comment`, so comments can be passed straight from a reader to a writer and keep their places.
Comments written before the header stay above it.
Readers and writers take lines starting with `#` as comments by default.

```python
path.write_text("# made by a tool\nfield1\tfield2\tfield3\n10\ttest1\t0.2\n")

with (
    TsvWriter.from_path[MyData](f"{path}.copy", header=True) as writer,
    TsvReader.from_path[MyData](path, on_comment=writer.write_comment) as reader,
):
    writer.write_all(reader)

assert Path(f"{path}.copy").read_text() == path.read_text()
```

### Extra Columns

A record can end with an `ExtraColumns` field, which holds as text the columns no other field takes, in file order.
Readers fill it, writers write it back, and a header only needs to name the other fields.

```python
from typeline import ExtraColumns

@dataclass
class Region:
    name: str
    start: int
    extra: ExtraColumns = ()

path.write_text("exon1\t10\n\nexon2\t20\t0.9\tHIGH\n")

with TsvReader.from_path[Region](path, header=False) as reader:
    assert list(reader) == [Region("exon1", 10), Region("exon2", 20, ("0.9", "HIGH"))]
```

### Counter Columns

A `CounterColumns[E]` field is a `Counter[E]` held in one column per member of the enum `E`, each named after its member's value, which must be text.
A record may have several such fields, as long as no two columns share a name.
Writers write every member's count in enum order, where the field sits among the other fields.
Readers find the member columns by name in a header, wherever they sit, or in enum order at the field's place when there is no header.
Every member needs a column, and every count must be a non-negative integer.

```python
from collections import Counter
from enum import StrEnum

from typeline import CounterColumns

class Base(StrEnum):
    A = "A"
    C = "C"
    G = "G"
    T = "T"

@dataclass
class Pileup:
    position: int
    counts: CounterColumns[Base]

with TsvWriter.from_path[Pileup](path) as writer:
    writer.write_header()
    writer.write(Pileup(100, Counter({Base.A: 12, Base.G: 3})))

assert path.read_text() == "position\tA\tC\tG\tT\n100\t12\t0\t3\t0\n"

path.write_text("position\tT\tG\tC\tA\n101\t2\t0\t0\t9\n")

with TsvReader.from_path[Pileup](path) as reader:
    assert list(reader) == [Pileup(101, Counter({Base.A: 9, Base.T: 2, Base.C: 0, Base.G: 0}))]
```

With an `ExtraColumns` field, the columns that are neither fields nor member columns are kept as extra columns.

### Column Names

A column is named after its field, unless `rename` gives it another name.
Writers can write these names in the header, and readers will match a pre-existing header to them, in any column order.
A column name can be any text, like `%GC` or `mean depth`, and can be made at runtime too.

```python
@dataclass
class Coverage:
    sample: str
    gc: float
    frac_below_min: float

min_depth = 20
rename = {"gc": "%GC", "frac_below_min": f"frac_below_{min_depth}x"}

with TsvWriter.from_path[Coverage](path, rename=rename) as writer:
    writer.write_header()
    writer.write(Coverage("s1", 41.2, 0.03))

assert path.read_text() == "sample\t%GC\tfrac_below_20x\ns1\t41.2\t0.03\n"

with TsvReader.from_path[Coverage](path, rename=rename) as reader:
    assert list(reader) == [Coverage("s1", 41.2, 0.03)]
```

A field has one column, so once it is given another name, that name replaces the field's name and the old one is retired.
Names are checked when a reader or writer is built.
If you name a column for something that is not a field, for an `ExtraColumns` or `CounterColumns` field, or give two columns the same name, then an exception is raised.
Without a header, columns are read in field order and `rename` has no effect.
A name starting with a comment prefix, like `#chrom`, is quoted when it starts a header, unless the reader and writer are given `comment_prefixes` that it does not start with, like `["##"]`.

### Turning Off Quoting

Readers and writers quote fields with `"` as CSV does, so text can hold the delimiter and line breaks.
For formats without quoting, `quoting=False` reads and writes `"` as ordinary text.
Without quoting, a writer refuses text that holds the delimiter or a line break with a `ValueError`.

```python
with TsvWriter.from_path[Region](path, quoting=False) as writer:
    writer.write(Region('"exon1', 10, ('say "hi"',)))

assert path.read_text() == '"exon1\t10\tsay "hi"\n'

with TsvReader.from_path[Region](path, header=False, quoting=False) as reader:
    assert list(reader) == [Region('"exon1', 10, ('say "hi"',))]
```

### Custom Field Formats

Lists, tuples, sets, dicts, and nested dataclasses are written as JSON by default, and dates, enums, and the like as their usual text.
For any other text format, a `FieldCodec` reads a field from its text and writes it back, chosen by the field's type.
Helpers in `typeline.codecs` cover common formats.

```python
from datetime import date

from typeline import Codecs
from typeline.codecs import boolean, delimited

@dataclass
class Visit:
    patient: str
    seen: date
    consented: bool
    blocks: list[int]

codecs: Codecs = {
    bool: boolean(true="Y", false="N"),
    list[int]: delimited(int, sep=";"),
}

with TsvWriter.from_path[Visit](path, codecs=codecs) as writer:
    writer.write(Visit("P-001", date(2026, 9, 29), True, [3, 1, 4]))

assert path.read_text() == "P-001\t2026-09-29\tY\t3;1;4\n"

with TsvReader.from_path[Visit](path, header=False, codecs=codecs) as reader:
    assert list(reader) == [Visit("P-001", date(2026, 9, 29), True, [3, 1, 4])]
```

Custom types nested anywhere inside a field, like a `list[Interval]`, are handled by `enc_hook`, which turns such an object into builtin values, and `dec_hook`, which builds it back from them.
Each hook raises `NotImplementedError` for types it does not handle.

```python
class Interval:
    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Interval) and (self.start, self.end) == (other.start, other.end)

def enc_hook(obj: object) -> object:
    if isinstance(obj, Interval):
        return [obj.start, obj.end]
    raise NotImplementedError

def dec_hook(kind: type, obj: object) -> object:
    if kind is Interval:
        return Interval(*obj)
    raise NotImplementedError

@dataclass
class Target:
    gene: str
    intervals: list[Interval]

with TsvWriter.from_path[Target](path, enc_hook=enc_hook) as writer:
    writer.write(Target("BRCA1", [Interval(1, 9), Interval(20, 25)]))

assert path.read_text() == "BRCA1\t[[1,9],[20,25]]\n"

with TsvReader.from_path[Target](path, header=False, dec_hook=dec_hook) as reader:
    assert list(reader) == [Target("BRCA1", [Interval(1, 9), Interval(20, 25)])]
```

### Your Own Format

Subclass a reader to give a format its own defaults.

```python
from typing import TextIO

from typing_extensions import Unpack

from typeline import ReaderOptions, RecordType
from typeline.codecs import key_value

class VcfLikeReader(TsvReader[RecordType]):
    def __init__(self, handle: TextIO, /, **options: Unpack[ReaderOptions]) -> None:
        defaults: ReaderOptions = {
            "codecs": {dict[str, str]: key_value()},
            "header": False,
            "none_field": ".",
        }
        super().__init__(handle, **(defaults | options))

@dataclass
class Site:
    chrom: str
    pos: int
    ident: str | None
    info: dict[str, str]

path.write_text("#CHROM\tPOS\tID\tINFO\nchr1\t100\t.\tDP=10;AF=0.5\n")

with VcfLikeReader.from_path[Site](path) as reader:
    assert list(reader) == [Site("chr1", 100, None, {"DP": "10", "AF": "0.5"})]
```

Type checkers see `VcfLikeReader.from_path[Site](...)` as a `TsvReader[Site]`, its closest built-in reader.
A subclass can add its own constructors that take a record type the same way by decorating a classmethod with `SubscriptableClassmethod`.

A reader fixed to one record type can add `FixedRecordType` to its bases, and is then built without a subscript.

```python
from typeline import FixedRecordType

class SiteReader(VcfLikeReader[Site], FixedRecordType):
    pass

with SiteReader.from_path(path) as reader:
    assert list(reader) == [Site("chr1", 100, None, {"DP": "10", "AF": "0.5"})]
```

More examples, from sample sheets to GFF3 and colored genomic features, are in [`tests/test_real_world_examples.py`](./tests/test_real_world_examples.py).

## Development and Testing

See the [contributing guide](./CONTRIBUTING.md) for more information.
