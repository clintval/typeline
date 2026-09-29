from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from typing import Generic
from typing import TypeVar

ValueType = TypeVar("ValueType")
"""The type variable for the value a field codec reads from and writes into text."""

ItemType = TypeVar("ItemType")
"""The type variable for the items of a delimited field."""


@dataclass(frozen=True)
class FieldCodec(Generic[ValueType]):
    """How to read a field's text into a value, and write the value back into text.

    Example:
        ```pycon
        >>> codec = FieldCodec(from_text=int, into_text=str)
        >>> codec.from_text("42")
        42
        >>> codec.into_text(42)
        '42'

        ```
    """

    from_text: Callable[[str], ValueType]
    """Read the text of a field into a value."""

    into_text: Callable[[ValueType], str]
    """Write a value into the text of a field."""


def delimited(
    item: Callable[[str], ItemType],
    sep: str = ",",
    trailing_sep: bool = False,
    container: Callable[[list[ItemType]], Collection[ItemType]] = list,
) -> FieldCodec[Any]:
    """Build a codec for a field that holds delimited items, e.g. `1,2,3`.

    Args:
        item: read each item from its text, e.g. `int`.
        sep: the separator between items.
        trailing_sep: whether to write a separator after the last item, e.g. `1,2,3,`. One is
            accepted when reading either way.
        container: the collection to build from the items, e.g. `tuple`.

    Example:
        ```pycon
        >>> blocks = delimited(int, trailing_sep=True)
        >>> blocks.from_text("1,2,3,")
        [1, 2, 3]
        >>> blocks.into_text([1, 2, 3])
        '1,2,3,'

        ```
    """

    def from_text(text: str) -> Collection[ItemType]:
        stripped = text.removesuffix(sep)
        return container([item(part) for part in stripped.split(sep)] if stripped else [])

    def into_text(value: Collection[Any]) -> str:
        text = sep.join(map(str, value))
        return f"{text}{sep}" if trailing_sep and text else text

    return FieldCodec(from_text=from_text, into_text=into_text)


NO_CODECS: Mapping[Any, FieldCodec[Any]] = MappingProxyType({})
"""No field codecs: every field is read from and written into text by default."""
