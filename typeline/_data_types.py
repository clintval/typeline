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
from typing import TypeAlias
from typing import TypeVar
from typing import Union  # pyright: ignore[reportDeprecated]
from typing import get_args
from typing import get_origin
from typing import get_type_hints

from typing_extensions import override


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


class _ExtraColumnsMarker:
    """Marks the field of a record that holds the columns no other field takes."""

    @override
    def __repr__(self) -> str:
        return "ExtraColumns"


EXTRA_COLUMNS_MARKER = _ExtraColumnsMarker()
"""The marker in `ExtraColumns` that readers and writers look for."""

ExtraColumns: TypeAlias = Annotated[tuple[str, ...], EXTRA_COLUMNS_MARKER]
"""The type of a record's last field that holds, as text, the columns no other field takes.

Example:
    ```python
    @dataclass
    class Region:
        name: str
        start: int
        extra: ExtraColumns = ()
    ```
"""


class _NamedExtraColumnsMarker:
    """Marks the field of a record that holds, by name, the columns no other field takes."""

    @override
    def __repr__(self) -> str:
        return "NamedExtraColumns"


NAMED_EXTRA_COLUMNS_MARKER = _NamedExtraColumnsMarker()
"""The marker in `NamedExtraColumns` that readers and writers look for."""

ColumnValueType = TypeVar("ColumnValueType")
"""The type variable for the value of each column a `NamedExtraColumns` field holds."""

NamedExtraColumns: TypeAlias = Annotated[dict[str, ColumnValueType], NAMED_EXTRA_COLUMNS_MARKER]
"""The type of a record's last field that holds the columns no other field takes, by name.

The columns are named after a header, in file order, and each is read and written as a field of
the value type would be.

Example:
    ```python
    @dataclass
    class Bin:
        size: int
        counts: NamedExtraColumns[int] = field(default_factory=dict)
    ```
"""


def _extra_columns_marker(annotation: Any) -> object | None:
    """Return the marker of an `ExtraColumns` or `NamedExtraColumns` annotation, else None."""
    if get_origin(annotation) is not Annotated:
        return None
    args = get_args(annotation)
    return next((m for m in (EXTRA_COLUMNS_MARKER, NAMED_EXTRA_COLUMNS_MARKER) if m in args), None)


def extra_columns_field(record_type: type[Any], field_type_map: dict[str, Any]) -> str | None:
    """Return the name of the field that holds the columns no other field takes, if any.

    Raises:
        TypeError: if more than one field would hold them, or if the field is not the last field.
    """
    markers = {
        name: marker
        for name, field_type in field_type_map.items()
        if (marker := _extra_columns_marker(field_type)) is not None
    }
    if not markers:
        return None
    record = record_type.__name__
    if len(markers) > 1:
        raise TypeError(
            f"{record} holds the columns no other field takes in more than one field:"
            + f" {list(markers)}!"
        )
    ((name, marker),) = markers.items()
    if name != list(field_type_map)[-1]:
        raise TypeError(
            f"The {marker!r} field of {record} must be its last field, but '{name}' is not!"
        )
    return name


def named_extra_columns_type(annotation: Any) -> Any | None:
    """Return the value type of a `NamedExtraColumns` annotation, e.g. `int`, else None."""
    if _extra_columns_marker(annotation) is not NAMED_EXTRA_COLUMNS_MARKER:
        return None
    return get_args(get_args(annotation)[0])[1]
