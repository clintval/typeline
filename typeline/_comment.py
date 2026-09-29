from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Comment:
    """A comment line that a reader skipped, and the line it was on."""

    line_number: int
    """The line number of the comment in its file, counting from 1."""

    text: str
    """The comment line, with its prefix and without its line ending."""
