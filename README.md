# typeline

[![PyPi Release](https://badge.fury.io/py/typeline.svg)](https://badge.fury.io/py/typeline)
[![CI](https://github.com/clintval/typeline/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/clintval/typeline/actions/workflows/tests.yml?query=branch%3Amain)
[![Python Versions](https://img.shields.io/badge/python-3.10_|_3.11_|_3.12-blue)](https://github.com/clintval/typeline)
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
>>> from typeline import TsvRecordWriter
>>> 
>>> temp_file = NamedTemporaryFile(mode="w+t", suffix=".txt")
>>>
>>> with TsvRecordWriter.from_path(temp_file.name, MyData) as writer:
...     writer.write_header()
...     writer.write(MyData(10, "test1", 0.2))
...     writer.write(MyData(20, "test2", None))

```

### Reading

```pycon
>>> from typeline import TsvRecordReader
>>> 
>>> with TsvRecordReader.from_path(temp_file.name, MyData) as reader:
...     for record in reader:
...         print(record)
MyData(field1=10, field2='test1', field3=0.2)
MyData(field1=20, field2='test2', field3=None)

```

## Development and Testing

See the [contributing guide](./CONTRIBUTING.md) for more information.
