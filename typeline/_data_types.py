from dataclasses import Field
from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import TypeAlias
from typing import TypeVar
from typing import runtime_checkable


@runtime_checkable
class DataclassInstance(Protocol):
    """A protocol for objects that are dataclass instances."""

    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


JsonType: TypeAlias = dict[str, "JsonType"] | list["JsonType"] | str | int | float | bool | None
"""A JSON-like data type."""

RecordType = TypeVar("RecordType", bound=DataclassInstance)
"""A type variable for the type of record (of dataclass type) for reading and writing."""
