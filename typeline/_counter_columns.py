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
        self._by_key: dict[str, dict[object, Enum]] = {
            name: {key: member for member in enum for key in (member, member.value)}
            for name, enum in self._enums.items()
        }
        self._positions: dict[str, list[tuple[Enum, int]]] = {name: [] for name in self._enums}
        self._zeros: dict[str, dict[Enum, int]] = {
            name: dict.fromkeys(enum, 0) for name, enum in self._enums.items()
        }

    def __bool__(self) -> bool:
        """Return whether the record has any `CounterColumns` field."""
        return bool(self._enums)

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

    def columns_of(self, name: str) -> list[str]:
        """Return the columns a field is held in: one per member for a `CounterColumns` field."""
        enum = self._enums.get(name)
        return [name] if enum is None else [member.value for member in enum]

    def locate(self, layout: list[str]) -> None:
        """Find where each member's column is in the columns of the data."""
        self._positions = {name: [] for name in self._enums}
        for index, column in enumerate(layout):
            if column in self._by_column:
                name, member = self._by_column[column]
                self._positions[name].append((member, index))

    def read(self, row: list[str], line_number: int | None) -> dict[str, Counter[Enum]]:
        """Read the count of each member of each field from its column in a row."""
        counters: dict[str, Counter[Enum]] = {}
        for name, zeros in self._zeros.items():
            counts: Counter[Enum] = Counter(zeros)
            for member, index in self._positions[name]:
                text = row[index]
                if not (text.isascii() and text.isdigit()):
                    on_line = "" if line_number is None else f" on line {line_number}"
                    raise ValueError(
                        f"Could not read column '{member.value}' of field '{name}' as a count"
                        + f" from '{text}'{on_line}!"
                    )
                counts[member] = int(text)
            counters[name] = counts
        return counters

    def write(self, name: str, counts: Mapping[Any, object]) -> list[str]:
        """Write the count of each member of a field in enum order, 0 when absent."""
        enum = self._enums[name]
        by_key = self._by_key[name]
        by_member: dict[Enum, object] = {}
        for key, count in counts.items():
            member = by_key.get(key)
            if member is None:
                raise ValueError(
                    f"Could not write field '{name}', which counts '{key}'"
                    + f" that is not a member of {enum.__name__}!"
                )
            if member in by_member:
                raise ValueError(
                    f"Could not write field '{name}', which counts '{member.value}' twice!"
                )
            by_member[member] = count
        texts: list[str] = []
        for member in enum:
            count = by_member.get(member, 0)
            if type(count) is not int or count < 0:
                raise ValueError(
                    f"Could not write field '{name}', which counts {count!r} of '{member.value}'"
                    + "; counts must be non-negative integers!"
                )
            texts.append(str(count))
        return texts
