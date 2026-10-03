from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from ._counter_columns import CounterFields

NO_COLUMNS: Mapping[str, str] = MappingProxyType({})
"""No column names: every field's column is named after the field."""


def name_columns(
    record_type: type[Any],
    field_type_map: dict[str, Any],
    extra_field: str | None,
    counters: CounterFields,
    columns: Mapping[str, object],
) -> dict[str, str]:
    """Return the column of each field held in one column, named in `columns` or after the field.

    Raises:
        TypeError: if a column is not named by a string.
        ValueError: if `columns` names the column of something that is not a field held in one
            column, or if two columns of the record would share a name.
    """
    record = record_type.__name__
    for name in columns:
        if name not in field_type_map:
            raise ValueError(
                f"Cannot name a column for '{name}', which is not a field of {record}!"
                + f" Fields of {record}: {list(field_type_map)}."
            )
        if name == extra_field:
            raise ValueError(
                f"Cannot name a column for the ExtraColumns field '{name}' of {record},"
                + " which holds the columns no other field takes!"
            )
        if name in counters:
            raise ValueError(
                f"Cannot name a column for the CounterColumns field '{name}' of {record},"
                + " whose columns are named after the values of its members!"
            )
    column_of: dict[str, str] = {}
    field_of: dict[str, str] = {}
    for name in field_type_map:
        if name == extra_field or name in counters:
            continue
        column = columns.get(name, name)
        if not isinstance(column, str):
            raise TypeError(
                f"The column of field '{name}' of {record} must be named by a string,"
                + f" not {column!r}!"
            )
        if column in field_of:
            raise ValueError(
                f"The fields '{field_of[column]}' and '{name}' of {record}"
                + f" both have a column '{column}'!"
            )
        counter = counters.field_with_column(column)
        if counter is not None:
            raise ValueError(
                f"The field '{name}' and the CounterColumns field '{counter}' of {record}"
                + f" both have a column '{column}'!"
            )
        column_of[name] = column
        field_of[column] = name
    return column_of


def in_column(name: str, column: str) -> str:
    """Name a field's column in an error, or nothing when the column is named after the field."""
    return "" if column == name else f" in column '{column}'"
