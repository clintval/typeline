from collections import Counter
from collections.abc import Mapping
from enum import Enum
from typing import Annotated
from typing import Any
from typing import TypeAlias
from typing import TypeVar
from typing import get_args
from typing import get_origin

from typing_extensions import override

from ._data_types import strip_annotated
from ._data_types import strip_optional


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
"""The type of a record's field that counts each member of an enum, in one column per member.

The enum's values must be text, as with a `StrEnum`, and name the columns.

Example:
    ```python
    @dataclass
    class Pileup:
        name: str
        counts: CounterColumns[Base]
    ```
"""


def _is_counter(field_type: Any) -> bool:
    """Return whether a field's type is a `CounterColumns` type."""
    return get_origin(field_type) is Annotated and COUNTER_COLUMNS_MARKER in get_args(field_type)


class CounterFields:
    """The `CounterColumns` fields of a record, and how their member columns are read and written.

    Each member of a field's enum is held in a column named after its value.
    """

    def __init__(self, record_type: type[Any], field_type_map: dict[str, Any]) -> None:
        """Find the `CounterColumns` fields of a record type, and check their member columns."""
        self._enums: dict[str, type[Enum]] = {}
        self._by_column: dict[str, tuple[str, Enum]] = {}
        for name, field_type in field_type_map.items():
            if _is_counter(strip_optional(field_type)) and not _is_counter(field_type):
                raise TypeError(
                    f"The CounterColumns field '{name}' of {record_type.__name__}"
                    + " may not be optional!"
                )
            if _is_counter(field_type):
                self._enums[name] = self._enum_of(record_type, name, field_type)
                for member in self._enums[name]:
                    self._claim(record_type, name, member, field_type_map)

    def __contains__(self, name: object) -> bool:
        """Return whether a field is a `CounterColumns` field."""
        return name in self._enums

    @staticmethod
    def _enum_of(record_type: type[Any], name: str, field_type: Any) -> type[Enum]:
        """Return the enum a field counts, which must be an Enum whose values are text."""
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
        return enum

    def _claim(
        self, record_type: type[Any], name: str, member: Enum, field_type_map: dict[str, Any]
    ) -> None:
        """Claim a member's column for a field, unless a field or another member has it."""
        column: str = member.value
        if column in field_type_map:
            raise TypeError(
                f"The CounterColumns field '{name}' of {record_type.__name__}"
                + f" has a column '{column}' named like a field!"
            )
        if column in self._by_column:
            raise TypeError(
                f"The CounterColumns fields '{self._by_column[column][0]}' and '{name}'"
                + f" of {record_type.__name__} both have a column '{column}'!"
            )
        self._by_column[column] = (name, member)

    def columns_of(self, name: str, column_of: Mapping[str, str]) -> list[str]:
        """Return the columns a field is held in: its own, or one per member of a Counter field."""
        enum = self._enums.get(name)
        return [column_of[name]] if enum is None else [member.value for member in enum]

    def field_with_column(self, column: str) -> str | None:
        """Return the `CounterColumns` field that has a member column of this name, if any."""
        found = self._by_column.get(column)
        return None if found is None else found[0]

    def locate(self, layout: list[str]) -> list[tuple[str, list[tuple[Enum, str, int]]]]:
        """Return each field's members, with the name and index of each member's column."""
        index_of = {column: index for index, column in enumerate(layout)}
        return [
            (name, [(member, member.value, index_of[member.value]) for member in enum])
            for name, enum in self._enums.items()
        ]

    def written(self, name: str) -> tuple[dict[object, int], list[str], str]:
        """Return how a field's counts are written.

        That is the index of each member, by the member and by its value, the column of each
        member in enum order, and the name of the enum.
        """
        enum = self._enums[name]
        index_of: dict[object, int] = {
            key: index for index, member in enumerate(enum) for key in (member, member.value)
        }
        return index_of, [member.value for member in enum], enum.__name__
