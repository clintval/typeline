from dataclasses import dataclass
from dataclasses import field


@dataclass
class SimpleMetric:
    """A simple metric for unit testing purposes.

    Attributes:
        field1: integer value for testing basic numeric types.
        field2: string value for testing text handling.
        field3: optional float for testing nullable numeric types.
    """

    field1: int = field(kw_only=True)
    field2: str = field(kw_only=True)
    field3: float | None = field(kw_only=True)


@dataclass
class ComplexMetric:
    """A complex nested metric for unit testing purposes."""

    field1: int = field(kw_only=True)
    field2: str = field(kw_only=True)
    field3: float | None = field(kw_only=True)
    field4: list[int] = field(kw_only=True)
    field5: set[int] = field(kw_only=True)
    field6: tuple[int, ...] = field(kw_only=True)
    field7: dict[str, int] = field(kw_only=True)
    field8: SimpleMetric = field(kw_only=True)
    field9: dict[str, SimpleMetric] = field(kw_only=True)
    field10: bool | None = field(kw_only=True)
    field11: bool | None = field(kw_only=True)
    field12: int | float | None = field(kw_only=True)
