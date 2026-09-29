from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from typing import Generic
from typing import TypeAlias
from typing import TypeVar
from typing import overload

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


Codecs: TypeAlias = Mapping[Any, FieldCodec[Any]]
"""Field codecs by the type of field they read and write, e.g. `{date: ..., list[int]: ...}`."""

NO_CODECS: Codecs = MappingProxyType({})
"""No field codecs: every field is read from and written into text by default."""


@overload
def key_value(sep: str = ";", assign: str = "=") -> FieldCodec[dict[str, str]]: ...


@overload
def key_value(
    sep: str = ";", assign: str = "=", *, value: Callable[[str], ItemType]
) -> FieldCodec[dict[str, ItemType]]: ...


def key_value(
    sep: str = ";", assign: str = "=", *, value: Callable[[str], Any] = str
) -> FieldCodec[dict[str, Any]]:
    """Build a codec for a field of key-value pairs, e.g. GFF3 attributes like `ID=g1;Name=TP53`.

    Args:
        sep: the separator between pairs. One after the last pair is accepted when reading.
        assign: the separator between a key and its value.
        value: read each value from its text, e.g. `float`.

    Example:
        ```pycon
        >>> attributes = key_value()
        >>> attributes.from_text("ID=g1;Name=TP53")
        {'ID': 'g1', 'Name': 'TP53'}
        >>> attributes.into_text({"ID": "g1", "Name": "TP53"})
        'ID=g1;Name=TP53'

        ```
    """

    def from_text(text: str) -> dict[str, Any]:
        pairs: dict[str, Any] = {}
        for item in text.removesuffix(sep).split(sep) if text else []:
            key, found, raw = item.partition(assign)
            if not found:
                raise ValueError(f"Expected key{assign}value but found '{item}'!")
            pairs[key] = value(raw)
        return pairs

    def into_text(pairs: dict[str, Any]) -> str:
        return sep.join(f"{key}{assign}{item}" for key, item in pairs.items())

    return FieldCodec(from_text=from_text, into_text=into_text)


def boolean(true: str = "Y", false: str = "N") -> FieldCodec[bool]:
    """Build a codec for a field that holds one of two texts for true and false, e.g. `Y` or `N`.

    Example:
        ```pycon
        >>> flag = boolean(true="T", false="F")
        >>> flag.from_text("T")
        True
        >>> flag.into_text(False)
        'F'

        ```
    """

    def from_text(text: str) -> bool:
        if text not in (true, false):
            raise ValueError(f"Expected '{true}' or '{false}' but found '{text}'!")
        return text == true

    return FieldCodec(from_text=from_text, into_text=lambda flag: true if flag else false)


def nullable(codec: FieldCodec[ValueType], missing: str) -> FieldCodec[ValueType | None]:
    """Build a codec that reads a field's own missing marker as None, e.g. `0` for a BED color.

    Example:
        ```pycon
        >>> count = nullable(FieldCodec(from_text=int, into_text=str), missing="-")
        >>> count.from_text("-") is None
        True
        >>> count.from_text("7")
        7

        ```
    """

    def from_text(text: str) -> ValueType | None:
        return None if text == missing else codec.from_text(text)

    def into_text(value: ValueType | None) -> str:
        return missing if value is None else codec.into_text(value)

    return FieldCodec(from_text=from_text, into_text=into_text)
