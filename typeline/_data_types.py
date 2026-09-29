from collections.abc import Mapping
from dataclasses import Field
from dataclasses import fields
from types import NoneType
from types import UnionType
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Protocol
from typing import TypeVar
from typing import Union  # pyright: ignore[reportDeprecated]
from typing import get_args
from typing import get_origin
from typing import get_type_hints


class DataclassInstance(Protocol):
    """A protocol for objects that are dataclass instances."""

    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


if TYPE_CHECKING:
    from .codecs import FieldCodec

RecordType = TypeVar("RecordType", bound=DataclassInstance)
"""The type variable for the records we will be reading and writing from delimited text data."""


def strip_annotated(annotation: Any) -> Any:
    """Return the type an `Annotated` annotation wraps, e.g. `str` for `Annotated[str, ...]`."""
    return get_args(annotation)[0] if get_origin(annotation) is Annotated else annotation


def strip_optional(annotation: Any) -> Any:
    """Return the type an optional annotation wraps, e.g. `int` for `int | None`, else unchanged."""
    if get_origin(annotation) not in (Union, UnionType):  # pyright: ignore[reportDeprecated]
        return annotation
    members = tuple(arg for arg in get_args(annotation) if arg is not NoneType)
    return members[0] if len(members) == 1 else annotation


def value_type(annotation: Any) -> Any:
    """Return the type of the value an annotation holds, without `Annotated` or `| None`."""
    return strip_annotated(strip_optional(strip_annotated(annotation)))


def is_text(annotation: Any) -> bool:
    """Return whether an annotation holds text, like `str`, a str enum, or a `Literal` of text."""
    annotation = value_type(annotation)
    while hasattr(annotation, "__supertype__"):
        annotation = annotation.__supertype__
    if get_origin(annotation) is Literal:
        return all(isinstance(arg, str) for arg in get_args(annotation))
    return isinstance(annotation, type) and issubclass(annotation, str)


def type_name(annotation: Any) -> str:
    """Return a readable name for a type annotation, e.g. `Color` or `list[int]`."""
    annotation = value_type(annotation)
    if isinstance(annotation, type) and not get_args(annotation):
        return annotation.__name__
    return repr(annotation).removeprefix("typing.")


def accepts_none(annotation: Any) -> bool:
    """Return whether a type annotation allows None, e.g. `int | None`."""
    annotation = strip_annotated(annotation)
    is_union = get_origin(annotation) in (Union, UnionType)  # pyright: ignore[reportDeprecated]
    return is_union and NoneType in get_args(annotation)


def field_types(record_type: type[DataclassInstance]) -> dict[str, Any]:
    """Return the type of each field, resolving annotations written as strings when possible."""
    try:
        hints = get_type_hints(record_type, include_extras=True)
    except (NameError, TypeError):
        hints = {}
    return {field.name: hints.get(field.name, field.type) for field in fields(record_type)}


def find_codec(
    annotation: Any, codecs: Mapping[Any, "FieldCodec[Any]"]
) -> "FieldCodec[Any] | None":
    """Find the codec for a field, trying its `Annotated` type before the type it annotates."""
    for candidate in (strip_optional(annotation), value_type(annotation)):
        try:
            if candidate in codecs:
                return codecs[candidate]
        except TypeError:
            continue
    return None
