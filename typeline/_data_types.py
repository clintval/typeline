from collections import Counter
from collections.abc import Mapping
from dataclasses import Field
from dataclasses import fields
from enum import Enum
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
    """Marks the field of a record that holds the columns past its other fields."""

    @override
    def __repr__(self) -> str:
        return "ExtraColumns"


EXTRA_COLUMNS_MARKER = _ExtraColumnsMarker()
"""The marker in `ExtraColumns` that readers and writers look for."""

ExtraColumns: TypeAlias = Annotated[tuple[str, ...], EXTRA_COLUMNS_MARKER]
"""The type of a record's last field that holds any columns past its other fields, as text.

Example:
    ```python
    @dataclass
    class Region:
        name: str
        start: int
        extra: ExtraColumns = ()
    ```
"""


def extra_columns_field(record_type: type[Any], field_type_map: dict[str, Any]) -> str | None:
    """Return the name of a record's `ExtraColumns` field, which must be its last field."""
    names = [
        name
        for name, field_type in field_type_map.items()
        if get_origin(field_type) is Annotated and EXTRA_COLUMNS_MARKER in get_args(field_type)
    ]
    if not names:
        return None
    last = list(field_type_map)[-1]
    misplaced = [name for name in names if name != last]
    if misplaced:
        raise TypeError(
            f"The ExtraColumns field of {record_type.__name__} must be its last field,"
            + f" but '{misplaced[0]}' is not!"
        )
    return last


class _CounterColumnsMarker:
    """Marks the field of a record that counts the members of an enum, one column per member."""

    @override
    def __repr__(self) -> str:
        return "CounterColumns"


COUNTER_COLUMNS_MARKER = _CounterColumnsMarker()
"""The marker in `CounterColumns` that readers and writers look for."""

MemberType = TypeVar("MemberType", bound=Enum)
"""The type variable for the enum whose members a `CounterColumns` field counts."""

CounterColumns: TypeAlias = Annotated[Counter[MemberType], COUNTER_COLUMNS_MARKER]
"""The type of a record's field that counts each member of an enum in a column named after it.

The enum's values must be text, as with a `StrEnum`, and name the columns.

Example:
    ```python
    @dataclass
    class Pileup:
        name: str
        counts: CounterColumns[Base]
    ```
"""


def counter_columns_fields(
    record_type: type[Any], field_type_map: dict[str, Any]
) -> dict[str, type[Enum]]:
    """Return each of a record's `CounterColumns` fields, by name, with the enum it counts."""
    counters: dict[str, type[Enum]] = {}
    owners: dict[str, str] = {}
    for name, field_type in field_type_map.items():
        if get_origin(field_type) is not Annotated or COUNTER_COLUMNS_MARKER not in get_args(
            field_type
        ):
            continue
        members = get_args(strip_annotated(field_type))
        enum = members[0] if len(members) == 1 else None
        if (
            not isinstance(enum, type)
            or not issubclass(enum, Enum)
            or not all(isinstance(member.value, str) for member in enum)
        ):
            raise TypeError(
                f"The CounterColumns field '{name}' of {record_type.__name__}"
                + " must count members of an Enum whose values are text!"
            )
        for member in enum:
            column: str = member.value
            if column in field_type_map:
                raise TypeError(
                    f"The CounterColumns field '{name}' of {record_type.__name__}"
                    + f" has a column '{column}' named like a field!"
                )
            if column in owners:
                raise TypeError(
                    f"The CounterColumns fields '{owners[column]}' and '{name}'"
                    + f" of {record_type.__name__} both have a column '{column}'!"
                )
            owners[column] = name
        counters[name] = enum
    return counters
