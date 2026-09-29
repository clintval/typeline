# typeline

[![PyPi Release](https://badge.fury.io/py/typeline.svg)](https://badge.fury.io/py/typeline)
[![CI](https://github.com/clintval/typeline/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/clintval/typeline/actions/workflows/tests.yml?query=branch%3Amain)
[![Python Versions](https://img.shields.io/badge/python-3.11_|_3.12_|_3.13_|_3.14-blue)](https://github.com/clintval/typeline)
[![basedpyright](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com/latest/)
[![mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
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

### Missing Values

`None` is written as an empty field. When read, an empty field is `None` if the field allows `None`, and `""` if it is a `str`.
Set `none_field`, e.g. to `"NA"`, when an optional text field must tell `""` and `None` apart.

### Any Text Stream

Subscript the class instead of `from_path` to read or write any open text stream.

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

Custom types nested anywhere inside a field, like a `list[Interval]`, are handled by `dec_hook` and `enc_hook`, with the same meaning as in msgspec.

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

More examples, from sample sheets to GFF3 and BED-like data, are in [`tests/test_real_world_examples.py`](./tests/test_real_world_examples.py).

## Development and Testing

See the [contributing guide](./CONTRIBUTING.md) for more information.
