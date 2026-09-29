import csv
from abc import ABC
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import AbstractContextManager
from csv import DictReader
from dataclasses import Field
from dataclasses import fields as fields_of
from dataclasses import is_dataclass
from functools import partial
from functools import update_wrapper
from io import TextIOWrapper
from os import linesep
from pathlib import Path
from types import TracebackType
from types import new_class
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Concatenate
from typing import Generic
from typing import ParamSpec
from typing import TypeVar
from typing import cast
from typing import overload

from msgspec import DecodeError
from msgspec import ValidationError
from msgspec import convert
from msgspec.json import Decoder as JSONDecoder
from typing_extensions import Never
from typing_extensions import NoReturn
from typing_extensions import Self
from typing_extensions import override

from ._data_types import RecordType

DEFAULT_COMMENT_PREFIXES: set[str] = set()
"""The default line prefixes that will tell the reader to skip those lines."""

JSON_LITERAL_KEYWORDS: frozenset[str] = frozenset({"null", "true", "false"})
"""JSON literal keywords that require parsing."""

_PARAMETERIZED_READERS: dict[tuple[type[Any], type[Any]], type[Any]] = {}
"""A cache of reader subclasses bound to a concrete record type."""

OwnerType = TypeVar("OwnerType", covariant=True)
"""The type variable for the class a reader constructor is accessed on."""

ConstructorParams = ParamSpec("ConstructorParams")
"""The parameters of a reader constructor, after the record type is bound."""


class _BoundSubscriptableClassmethod(Generic[OwnerType, ConstructorParams]):
    """A subscriptable classmethod bound to its class, awaiting a record type."""

    def __init__(self, owner: Any, func: Callable[..., Any], name: str) -> None:
        self._owner: Any = owner
        self._func: Callable[..., Any] = func
        _ = update_wrapper(self, func)
        self.__name__: str = name

    @overload
    def __getitem__(
        self: "_BoundSubscriptableClassmethod[type[TsvReader[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, TsvReader[RecordType]]": ...

    @overload
    def __getitem__(
        self: "_BoundSubscriptableClassmethod[type[CsvReader[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, CsvReader[RecordType]]": ...

    @overload
    def __getitem__(
        self, record_type: type[RecordType]
    ) -> "Callable[ConstructorParams, DelimitedDataReader[RecordType]]": ...

    def __getitem__(self, record_type: object) -> Callable[..., Any]:
        """Bind the record type, returning a constructor for a reader of that record type."""
        self._refuse_bound_owner()
        if not isinstance(record_type, type) or not is_dataclass(record_type):
            raise TypeError(
                f"{self._usage} must be subscripted with a dataclass, not {record_type}!"
            )
        return partial(self._func, self._owner[record_type])

    def __call__(self, *_args: Never, **_kwargs: Never) -> NoReturn:
        """Refuse to construct a reader without a record type."""
        self._refuse_bound_owner()
        raise TypeError(
            f"{self._usage} must be subscripted with a dataclass, e.g. {self._usage}[MyData]!"
        )

    @property
    def _usage(self) -> str:
        """The unsubscripted reader and method name, e.g. `TsvReader.from_path`."""
        unbound = next(c for c in self._owner.__mro__ if c._parameterized_record_type is None)
        return f"{unbound.__name__}.{self.__name__}"

    def _refuse_bound_owner(self) -> None:
        """Refuse access through a reader class that already has a record type."""
        if self._owner._parameterized_record_type is not None:
            raise TypeError(
                f"{self._owner.__name__} already has a record type!"
                + f" Use {self._usage}[MyData] instead."
            )


class _SubscriptableClassmethod(Generic[ConstructorParams]):
    """Turn a classmethod into a constructor that is subscripted with a record type before calling.

    Example: `TsvReader.from_path[MyData](path)`.
    """

    def __init__(
        self,
        method: "classmethod[Any, ConstructorParams, Any]"
        | Callable[Concatenate[Any, ConstructorParams], Any],
    ) -> None:
        self._func: Callable[..., Any] = (
            method.__func__ if isinstance(method, classmethod) else method
        )
        self._name: str = ""

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = name

    def __get__(
        self, obj: object, owner: type[OwnerType]
    ) -> _BoundSubscriptableClassmethod[type[OwnerType], ConstructorParams]:
        return _BoundSubscriptableClassmethod(owner, self._func, self._name)


class DelimitedDataReader(
    AbstractContextManager["DelimitedDataReader[RecordType]"],
    Iterable[RecordType],
    ABC,
    Generic[RecordType],
):
    """A reader for reading delimited text data into dataclasses."""

    delimiter: str
    """The delimiter used to separate fields in the delimited data."""

    _parameterized_record_type: ClassVar[type[Any] | None] = None
    """The record type bound by subscripting the class, e.g. `CsvReader[MyData]`."""

    def __init__(
        self,
        handle: TextIOWrapper,
        /,
        header: bool = True,
        comment_prefixes: Collection[str] = DEFAULT_COMMENT_PREFIXES,
        none_field: str = "",
        dec_hook: Callable[[type, Any], Any] | None = None,
    ):
        """Instantiate a new delimited data reader.

        Args:
            handle: a file-like object to read delimited data from.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
            none_field: the string that is used in place of None for a field.
            dec_hook: a custom decoder hook for the JSON decoder.
        """
        if self._parameterized_record_type is None:
            name = type(self).__name__
            raise TypeError(f"{name} must be subscripted with a dataclass, e.g. {name}[MyData]!")
        record_type = cast(type[RecordType], self._parameterized_record_type)

        # Initialize and save internal attributes of this class.
        self._handle: TextIOWrapper = handle
        self._line_count: int = 0
        self._record_type: type[RecordType] = record_type
        self._comment_prefixes: Collection[str] = set(comment_prefixes)
        self._none_field: str = none_field
        self._dec_hook: Callable[[type, Any], Any] | None = dec_hook

        # Build a JSON decoder for parsing string values into Python objects
        self._json_decoder: JSONDecoder[Any] = JSONDecoder()

        # Inspect the record type and save the fields, field names, and field types.
        self._fields: tuple[Field[Any], ...] = fields_of(record_type)
        self._header: list[str] = [field.name for field in self._fields]
        self._field_types: list[type | Any | str] = [field.type for field in self._fields]
        self._field_type_map: dict[str, type | Any | str] = {f.name: f.type for f in self._fields}

        # Build the delimited dictionary reader, filtering out any comment lines along the way.
        self._reader: DictReader[Any] = DictReader(
            self._filter_out_comments(handle),
            delimiter=self.delimiter,
            fieldnames=self._header if not header else None,
            lineterminator=linesep,
            quotechar="'",
            quoting=csv.QUOTE_MINIMAL,
        )

        # Protect the user from the case where a header was specified, but a data line was found!
        if self._reader.fieldnames is not None and self._reader.fieldnames != self._header:
            found: list[str] = list(self._reader.fieldnames)
            missing: list[str] = [name for name in self._header if name not in found]
            unexpected: list[str] = [name for name in found if name not in self._header]
            raise ValueError(
                "Fields of header do not match fields of dataclass!"
                + f" Header: {found}."
                + f" Fields of {record_type.__name__}: {self._header}."
                + (f" Missing from header: {missing}." if missing else "")
                + (f" Unexpected in header: {unexpected}." if unexpected else "")
                + ("" if missing or unexpected else " The fields are out of order.")
            )

    @override
    def __init_subclass__(cls, delimiter: str | None = None, **kwargs: object) -> None:
        """Define a delimiter upon the subclass using metaclass programming."""
        if delimiter is not None:
            cls.delimiter = delimiter
        super().__init_subclass__(**kwargs)

    def __class_getitem__(cls, item: Any) -> Any:
        """Parameterize the reader, binding a concrete record type so classmethods can see it."""
        alias = super().__class_getitem__(item)  # type: ignore[misc]  # pyright: ignore[reportAttributeAccessIssue]
        if not isinstance(item, type) or not is_dataclass(item):
            return alias
        key = (cls, item)
        if key not in _PARAMETERIZED_READERS:
            _ = _PARAMETERIZED_READERS.setdefault(
                key,
                new_class(
                    f"{cls.__name__}[{item.__name__}]",
                    (alias,),
                    exec_body=lambda ns: ns.update({
                        "__module__": cls.__module__,
                        "__qualname__": f"{cls.__qualname__}[{item.__qualname__}]",
                        "_parameterized_record_type": item,
                    }),
                ),
            )
        return _PARAMETERIZED_READERS[key]

    def with_decoder(self, dec_hook: Callable[[type, Any], Any]) -> Self:
        """Chain an additional decoder hook.

        This allows building up multiple transformations::

            reader = (CsvReader.from_path[MyData]("data.csv")
                .with_decoder(custom_decoder_1)
                .with_decoder(custom_decoder_2))

        Args:
            dec_hook: A function that takes (field_type, value) and returns the decoded value.

        Returns:
            Self for method chaining.
        """
        old_hook = self._dec_hook
        if old_hook is None:
            self._dec_hook = dec_hook
        else:
            self._dec_hook = lambda typ, val: old_hook(typ, dec_hook(typ, val))
        return self

    @override
    def __enter__(self) -> Self:
        """Enter this context."""
        _ = super().__enter__()
        return self

    @override
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        """Exit this context while closing all open resources."""
        self.close()
        return None

    def _filter_out_comments(self, lines: Iterator[str]) -> Iterator[str]:
        """Yield only lines in an iterator that do not start with a comment prefix."""
        for line in lines:
            self._line_count += 1
            if not line or not (stripped := line.strip()):
                continue
            elif any(stripped.startswith(prefix) for prefix in self._comment_prefixes):
                continue
            yield line

    def _decode(self, field_type: type[Any] | str | Any, item: Any) -> Any:
        """Decode a field value before parsing.

        This method can be overridden in subclasses to provide custom decoding logic
        for specific types. The method receives the field type and the raw string value,
        and should return a value that can be parsed by msgspec (typically a string in
        JSON format for complex types, or the value itself for simple types).

        Args:
            field_type: The type annotation for this field.
            item: The raw value read from the delimited file.

        Returns:
            A value ready for msgspec to parse (often a JSON-formatted string).

        Example:
            >>> def _decode(self, field_type, item):
            ...     # Convert comma-separated to JSON array format
            ...     if get_origin(field_type) is list:
            ...         return f"[{item.replace(',', ',')}]"
            ...     return item
        """
        if self._dec_hook is not None:
            return self._dec_hook(field_type, item)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # ty: ignore[invalid-argument-type]
        return item

    def _preprocess(self, field_name: str, value: Any) -> Any:
        """Preprocess a field value using two-phase parsing.

        Phase 1: Transform the raw string using _decode() to make it parseable.
        Phase 2: Parse JSON-like strings into Python objects.

        This allows subclasses to override _decode() to handle custom types and formats
        while keeping the JSON parsing logic consistent.

        Args:
            field_name: The name of the field being processed.
            value: The raw value read from the delimited file.

        Returns:
            A Python object ready for msgspec.convert().
        """
        if value == self._none_field:
            return None

        if isinstance(value, str):
            field_type = self._field_type_map.get(field_name, Any)
            processed_value = self._decode(field_type, value)

            if isinstance(processed_value, str):
                stripped = processed_value.strip()
                if stripped in JSON_LITERAL_KEYWORDS or (stripped and stripped[0] in "{["):
                    try:
                        parsed: Any = self._json_decoder.decode(processed_value.encode("utf-8"))
                        return parsed
                    except DecodeError:
                        return processed_value
                else:
                    return processed_value
            else:
                return processed_value

        return value

    @override
    def __iter__(self) -> Iterator[RecordType]:
        """Yield converted records from the delimited data file."""
        for record in self._reader:
            if None in record or None in record.values():
                row: dict[Any, Any] = record  # DictReader keys overflow under None
                extra: list[str] = row.get(None, [])
                present: int = sum(v is not None for k, v in row.items() if k is not None)
                found: int = present + len(extra)
                raise ValueError(
                    f"Expected {len(self._header)} fields but found {found} on line"
                    + f" {self._line_count} for record type: {self._record_type.__name__}."
                )
            preprocessed = {key: self._preprocess(key, value) for key, value in record.items()}
            try:
                yield convert(
                    preprocessed,
                    self._record_type,
                    strict=False,
                    str_keys=True,
                )
            except ValidationError as exception:
                raise ValidationError(
                    "Could not parse JSON-like object into requested structure:"
                    + f" {preprocessed}."
                    + f" Requested structure: {self._record_type.__name__}."
                    + f" Original exception: {exception}"
                ) from exception

    def close(self) -> None:
        """Close all opened resources."""
        self._handle.close()
        return None

    @_SubscriptableClassmethod
    @classmethod
    def from_path(
        cls,
        path: Path | str,
        /,
        header: bool = True,
        comment_prefixes: Collection[str] = DEFAULT_COMMENT_PREFIXES,
        none_field: str = "",
        dec_hook: Callable[[type, Any], Any] | None = None,
    ) -> Self:
        """Construct a delimited data reader from a file path.

        Args:
            path: the path to the file to read delimited data from.
            header: whether we expect the first line to be a header or not.
            comment_prefixes: skip lines that have any of these string prefixes.
            none_field: the string that is used in place of None for a field.
            dec_hook: a custom decoder hook for the underlying JSON decoder.
        """
        handle = Path(path).expanduser().open("r")
        try:
            return cls(
                handle,
                header=header,
                comment_prefixes=comment_prefixes,
                none_field=none_field,
                dec_hook=dec_hook,
            )
        except BaseException:
            handle.close()
            raise


class CsvReader(DelimitedDataReader[RecordType], delimiter=","):
    r"""A reader for reading comma-delimited data into dataclasses.

    Example:
        ```pycon
        >>> from pathlib import Path
        >>> from dataclasses import dataclass
        >>> from tempfile import NamedTemporaryFile
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import CsvReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1,field2\nmy-name,0.2\n")
        ...     _ = tmpfile.flush()
        ...     with CsvReader.from_path[MyData](tmpfile.name) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """


class TsvReader(DelimitedDataReader[RecordType], delimiter="\t"):
    r"""A reader for reading tab-delimited data into dataclasses.

    Example:
        ```pycon
        >>> from pathlib import Path
        >>> from dataclasses import dataclass
        >>> from tempfile import NamedTemporaryFile
        >>>
        >>> @dataclass
        ... class MyData:
        ...     field1: str
        ...     field2: float | None
        >>>
        >>> from typeline import TsvReader
        >>>
        >>> with NamedTemporaryFile(mode="w+t") as tmpfile:
        ...     _ = tmpfile.write("field1\tfield2\nmy-name\t0.2\n")
        ...     _ = tmpfile.flush()
        ...     with TsvReader.from_path[MyData](tmpfile.name) as reader:
        ...         for record in reader:
        ...             print(record)
        MyData(field1='my-name', field2=0.2)

        ```
    """
