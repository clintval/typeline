# typeline

[![PyPi Release](https://badge.fury.io/py/typeline.svg)](https://badge.fury.io/py/typeline)
[![CI](https://github.com/clintval/typeline/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/clintval/typeline/actions/workflows/tests.yml?query=branch%3Amain)
[![Python Versions](https://img.shields.io/badge/python-3.11_|_3.12_|_3.13_|_3.14-blue)](https://github.com/clintval/typeline)
[![basedpyright](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com/latest/)
[![mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://docs.astral.sh/uv/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://docs.astral.sh/ruff/)

Write dataclasses to delimited text formats and read them back again.

Features type-safe parsing, optional field support, and an intuitive API for working with structured data.

## Installation

The package can be installed with `pip`:

```console
pip install typeline
```

## Quickstart

### Building a Test Dataclass

```pycon
>>> from dataclasses import dataclass
>>>
>>> @dataclass
... class MyData:
...     field1: int
...     field2: str
...     field3: float | None

```

### Writing

```pycon
>>> from tempfile import NamedTemporaryFile
>>> from typeline import TsvWriter
>>> 
>>> temp_file = NamedTemporaryFile(mode="w+t", suffix=".tsv")
>>>
>>> with TsvWriter.from_path[MyData](temp_file.name) as writer:
...     writer.write_header()
...     writer.write(MyData(10, "test1", 0.2))
...     writer.write(MyData(20, "test2", None))

```

### Reading

```pycon
>>> from typeline import TsvReader
>>> 
>>> with TsvReader.from_path[MyData](temp_file.name) as reader:
...     for record in reader:
...         print(record)
MyData(field1=10, field2='test1', field3=0.2)
MyData(field1=20, field2='test2', field3=None)

```

A reader from `from_path` closes its file once it is read to the end, so reading everything into a list needs no `with` block.

```pycon
>>> records = list(TsvReader.from_path[MyData](temp_file.name))

```

### Any Text Stream

To use an open text stream instead of a path, subscript the reader or writer class itself.

```pycon
>>> import gzip
>>>
>>> with TsvWriter[MyData](gzip.open(f"{temp_file.name}.gz", "wt")) as writer:
...     writer.write(MyData(10, "test1", 0.2))
>>>
>>> with TsvReader[MyData](gzip.open(f"{temp_file.name}.gz", "rt"), header=False) as reader:
...     print(list(reader))
[MyData(field1=10, field2='test1', field3=0.2)]

```

### Compressed Files

`from_path` reads gzip, bzip2, and xz files, which it recognizes by their contents.
It writes them when the path ends in `.gz`, `.bz2`, or `.xz`.

```pycon
>>> with TsvWriter.from_path[MyData](f"{temp_file.name}.gz") as writer:
...     writer.write(MyData(10, "test1", 0.2))
>>>
>>> with TsvReader.from_path[MyData](f"{temp_file.name}.gz", header=False) as reader:
...     print(list(reader))
[MyData(field1=10, field2='test1', field3=0.2)]

```

### Missing Values

`None` is written as an empty field.
When read, an empty field is `None` if the field allows `None`, and `""` if it is a `str`.
Set `none_field`, e.g. to `"NA"`, when an optional text field must tell `""` and `None` apart.

### Comments

A reader skips lines that start with any of its `comment_prefixes`, and hands each one to `on_comment` as a `Comment` with its line number.
A writer writes comments with `write_comment`, so comments can be passed straight from a reader to a writer and keep their places.

```pycon
>>> _ = open(temp_file.name, "w").write("# made by a tool\nfield1\tfield2\tfield3\n10\ttest1\t0.2\n")
>>>
>>> with (
...     TsvWriter.from_path[MyData](f"{temp_file.name}.copy") as writer,
...     TsvReader.from_path[MyData](temp_file.name, comment_prefixes={"#"}, on_comment=writer.write_comment) as reader,
... ):
...     writer.write_header()
...     for record in reader:
...         writer.write(record)
>>>
>>> print(open(f"{temp_file.name}.copy").read(), end="")
# made by a tool
field1	field2	field3
10	test1	0.2

```

### Extra Columns

A record can end with an `ExtraColumns` field, which holds any columns past its other fields as text.
Readers fill it, writers write it back, and a header only needs to name the other fields.

```pycon
>>> from typeline import ExtraColumns
>>>
>>> @dataclass
... class Region:
...     name: str
...     start: int
...     extra: ExtraColumns = ()
>>>
>>> _ = open(temp_file.name, "w").write("exon1\t10\n\nexon2\t20\t0.9\tHIGH\n")
>>>
>>> with TsvReader.from_path[Region](temp_file.name, header=False) as reader:
...     print(list(reader))
[Region(name='exon1', start=10, extra=()), Region(name='exon2', start=20, extra=('0.9', 'HIGH'))]

```

### Turning Off Quoting

Readers and writers quote fields with `"` as CSV does, so text can hold the delimiter and line breaks.
For formats without quoting, `quoting=False` reads and writes `"` as ordinary text.
Without quoting, a writer refuses text that holds the delimiter or a line break with a `ValueError`.

```pycon
>>> with TsvWriter.from_path[Region](temp_file.name, quoting=False) as writer:
...     writer.write(Region('"exon1', 10, ('say "hi"',)))
>>>
>>> print(open(temp_file.name).read(), end="")
"exon1	10	say "hi"
>>>
>>> with TsvReader.from_path[Region](temp_file.name, header=False, quoting=False) as reader:
...     print(list(reader))
[Region(name='"exon1', start=10, extra=('say "hi"',))]

```

### Counter Columns

A `CounterColumns[E]` field is a `Counter[E]` held in one column per member of the enum `E`, each named after its member's value, which must be text.
A record may have several such fields, as long as no two columns share a name.
Writers write every member's count in enum order, where the field sits among the other fields.
Readers find the member columns by name in a header, wherever they sit, or in enum order at the field's place when there is no header.
Every member needs a column, and every count must be a non-negative integer.

```pycon
>>> from collections import Counter
>>> from enum import StrEnum
>>> from typeline import CounterColumns
>>>
>>> class Base(StrEnum):
...     A = "A"
...     C = "C"
...     G = "G"
...     T = "T"
>>>
>>> @dataclass
... class Pileup:
...     position: int
...     counts: CounterColumns[Base]
>>>
>>> with TsvWriter.from_path[Pileup](temp_file.name) as writer:
...     writer.write_header()
...     writer.write(Pileup(100, Counter({Base.A: 12, Base.G: 3})))
>>>
>>> print(open(temp_file.name).read(), end="")
position	A	C	G	T
100	12	0	3	0
>>>
>>> _ = open(temp_file.name, "w").write("position\tT\tG\tC\tA\n101\t2\t0\t0\t9\n")
>>>
>>> with TsvReader.from_path[Pileup](temp_file.name) as reader:
...     print(list(reader))
[Pileup(position=101, counts=Counter({<Base.A: 'A'>: 9, <Base.T: 'T'>: 2, <Base.C: 'C'>: 0, <Base.G: 'G'>: 0}))]

```

With an `ExtraColumns` field, the columns that are neither fields nor member columns are kept as extra columns.

### Custom Field Formats

Lists, dicts, sets, enums, and nested dataclasses are written as JSON by default.
For any other text format, a `FieldCodec` reads a field from its text and writes it back, chosen by the field's type.
Helpers in `typeline.codecs` cover common formats.

```pycon
>>> from datetime import date
>>> from typeline import Codecs, FieldCodec
>>> from typeline.codecs import boolean, delimited
>>>
>>> @dataclass
... class Visit:
...     patient: str
...     seen: date
...     consented: bool
...     blocks: list[int]
>>>
>>> codecs: Codecs = {
...     date: FieldCodec(from_text=date.fromisoformat, into_text=date.isoformat),
...     bool: boolean(true="Y", false="N"),
...     list[int]: delimited(int, sep=";"),
... }
>>>
>>> with TsvWriter.from_path[Visit](temp_file.name, codecs=codecs) as writer:
...     writer.write(Visit("P-001", date(2026, 9, 29), True, [3, 1, 4]))
>>>
>>> print(open(temp_file.name).read(), end="")
P-001	2026-09-29	Y	3;1;4
>>>
>>> with TsvReader.from_path[Visit](temp_file.name, header=False, codecs=codecs) as reader:
...     print(list(reader))
[Visit(patient='P-001', seen=datetime.date(2026, 9, 29), consented=True, blocks=[3, 1, 4])]

```

Custom types nested anywhere inside a field, like a `list[Interval]`, are handled by `enc_hook`, which turns such an object into builtin values, and `dec_hook`, which builds it back from them.
Each hook raises `NotImplementedError` for types it does not handle.

```pycon
>>> class Interval:
...     def __init__(self, start: int, end: int) -> None:
...         self.start = start
...         self.end = end
...
...     def __repr__(self) -> str:
...         return f"Interval({self.start}, {self.end})"
>>>
>>> def enc_hook(obj: object) -> object:
...     if isinstance(obj, Interval):
...         return [obj.start, obj.end]
...     raise NotImplementedError
>>>
>>> def dec_hook(kind: type, obj: object) -> object:
...     if kind is Interval:
...         return Interval(*obj)
...     raise NotImplementedError
>>>
>>> @dataclass
... class Target:
...     gene: str
...     intervals: list[Interval]
>>>
>>> with TsvWriter.from_path[Target](temp_file.name, enc_hook=enc_hook) as writer:
...     writer.write(Target("BRCA1", [Interval(1, 9), Interval(20, 25)]))
>>>
>>> print(open(temp_file.name).read(), end="")
BRCA1	[[1,9],[20,25]]
>>>
>>> with TsvReader.from_path[Target](temp_file.name, header=False, dec_hook=dec_hook) as reader:
...     print(list(reader))
[Target(gene='BRCA1', intervals=[Interval(1, 9), Interval(20, 25)])]

```

### Your Own Format

Subclass a reader to give a format its own defaults.

```pycon
>>> from typing import TextIO
>>> from typing_extensions import Unpack
>>> from typeline import ReaderOptions, RecordType
>>> from typeline.codecs import key_value
>>>
>>> class VcfLikeReader(TsvReader[RecordType]):
...     def __init__(self, handle: TextIO, /, **options: Unpack[ReaderOptions]) -> None:
...         _ = options.setdefault("header", False)
...         _ = options.setdefault("comment_prefixes", {"#"})
...         _ = options.setdefault("none_field", ".")
...         _ = options.setdefault("codecs", {dict[str, str]: key_value()})
...         super().__init__(handle, **options)
>>>
>>> @dataclass
... class Site:
...     chrom: str
...     pos: int
...     ident: str | None
...     info: dict[str, str]
>>>
>>> _ = open(temp_file.name, "w").write("#CHROM\tPOS\tID\tINFO\nchr1\t100\t.\tDP=10;AF=0.5\n")
>>>
>>> with VcfLikeReader.from_path[Site](temp_file.name) as reader:
...     print(list(reader))
[Site(chrom='chr1', pos=100, ident=None, info={'DP': '10', 'AF': '0.5'})]

```

Type checkers see `VcfLikeReader.from_path[Site](...)` as a `TsvReader[Site]`, its closest built-in reader.

A reader fixed to one record type can add `FixedRecordType` to its bases, and is then built without a subscript.

```pycon
>>> from typeline import FixedRecordType
>>>
>>> class SiteReader(VcfLikeReader[Site], FixedRecordType):
...     pass
>>>
>>> with SiteReader.from_path(temp_file.name) as reader:
...     print(list(reader))
[Site(chrom='chr1', pos=100, ident=None, info={'DP': '10', 'AF': '0.5'})]

```

More examples, from sample sheets to GFF3 and colored genomic features, are in [`tests/test_real_world_examples.py`](./tests/test_real_world_examples.py).

## Development and Testing

See the [contributing guide](./CONTRIBUTING.md) for more information.
