from collections.abc import Collection
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Comment:
    """A comment line that a reader skipped, and the line it was on."""

    line_number: int
    """The line number of the comment in its file, counting from 1."""

    text: str
    """The comment line, with its prefix and without its line ending."""


def comment_prefixes_of(prefixes: Collection[str]) -> tuple[str, ...]:
    """Return comment prefixes, refusing one string, or an empty prefix every line starts with."""
    if isinstance(prefixes, str):
        raise TypeError(
            f"comment_prefixes must be a collection of strings, not the string {prefixes!r}!"
        )
    if "" in prefixes:
        raise ValueError(
            "comment_prefixes must not hold an empty prefix, which every line starts with!"
        )
    return tuple(prefixes)
