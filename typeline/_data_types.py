from dataclasses import Field
from dataclasses import fields
from types import NoneType
from types import UnionType
from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import TypeVar
from typing import Union  # pyright: ignore[reportDeprecated]
from typing import get_args
from typing import get_origin
from typing import get_type_hints


class DataclassInstance(Protocol):
    """A protocol for objects that are dataclass instances."""

    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


RecordType = TypeVar("RecordType", bound=DataclassInstance)
"""The type variable for the records we will be reading and writing from delimited text data."""


def strip_optional(annotation: Any) -> Any:
    """Return the type an optional annotation wraps, e.g. `int` for `int | None`, else unchanged."""
    if get_origin(annotation) not in (Union, UnionType):  # pyright: ignore[reportDeprecated]
        return annotation
    members = tuple(arg for arg in get_args(annotation) if arg is not NoneType)
    return members[0] if len(members) == 1 else annotation


def type_name(annotation: Any) -> str:
    """Return a readable name for a type annotation, e.g. `Color` or `list[int]`."""
    if isinstance(annotation, type) and not get_args(annotation):
        return annotation.__name__
    return repr(annotation).removeprefix("typing.")


def accepts_none(annotation: Any) -> bool:
    """Return whether a type annotation allows None, e.g. `int | None`."""
    is_union = get_origin(annotation) in (Union, UnionType)  # pyright: ignore[reportDeprecated]
    return is_union and NoneType in get_args(annotation)


def field_types(record_type: type[DataclassInstance]) -> dict[str, Any]:
    """Return the type of each field, resolving annotations written as strings when possible."""
    try:
        hints = get_type_hints(record_type)
    except (NameError, TypeError):
        hints = {}
    return {field.name: hints.get(field.name, field.type) for field in fields(record_type)}
