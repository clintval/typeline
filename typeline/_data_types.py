from dataclasses import Field
from types import UnionType
from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import TypeAlias
from typing import TypeVar
from typing import (  # type: ignore[attr-defined]
    _UnionGenericAlias,  # pyright: ignore[reportAttributeAccessIssue,reportUnknownVariableType]
)
from typing import cast


class DataclassInstance(Protocol):
    """A protocol for objects that are dataclass instances."""

    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


JsonType: TypeAlias = dict[str, "JsonType"] | list["JsonType"] | str | int | float | bool | None
"""A JSON-like data type."""

RecordType = TypeVar("RecordType", bound=DataclassInstance)
"""The type variable for the records we will be reading and writing from delimited text data."""


def build_union(*types: type) -> type | UnionType:
    """Pass-through a singular type or build a union type if multiple types are provided."""
    if len(types) == 1:
        return types[0]
    union: UnionType | type = types[0]
    for t in types[1:]:
        union |= t
    return cast(UnionType, union)


def is_union(_type: type | str | Any) -> bool:
    """Return if this type is a union of types or not."""
    return isinstance(_type, (UnionType, _UnionGenericAlias))
