from dataclasses import Field
from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import TypeVar


class DataclassInstance(Protocol):
    """A protocol for objects that are dataclass instances."""

    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


RecordType = TypeVar("RecordType", bound=DataclassInstance)
"""The type variable for the records we will be reading and writing from delimited text data."""
