from dataclasses import is_dataclass
from functools import partial
from functools import update_wrapper
from types import new_class
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Concatenate
from typing import Generic
from typing import ParamSpec
from typing import TypeVar
from typing import cast
from typing import overload

from typing_extensions import Never
from typing_extensions import NoReturn

from ._data_types import RecordType

if TYPE_CHECKING:
    from ._reader import CsvReader
    from ._reader import DelimitedDataReader
    from ._reader import TsvReader
    from ._writer import CsvWriter
    from ._writer import DelimitedDataWriter
    from ._writer import TsvWriter

_BOUND_CLASSES: dict[tuple[type[Any], type[Any]], type[Any]] = {}
"""A cache of reader and writer subclasses bound to a concrete record type."""

OwnerType = TypeVar("OwnerType", covariant=True)
"""The type variable for the class a subscriptable classmethod is accessed on."""

ConstructorParams = ParamSpec("ConstructorParams")
"""The parameters of a subscriptable classmethod, after the record type is bound."""


FixedType = TypeVar("FixedType", bound="FixedRecordType")
"""The type variable for a reader or writer class fixed to one record type."""


class FixedRecordType:
    """Mark a reader or writer fixed to one record type, so `from_path` needs no subscript.

    Example:
        ```python
        class GeneReader(TsvReader[Gene], FixedRecordType): ...

        reader = GeneReader.from_path("genes.tsv")
        ```
    """


class DelimitedData:
    """A reader or writer of delimited data, bound to a record type by subscripting its class."""

    delimiter: ClassVar[str]
    """The delimiter used to separate fields in the delimited data."""

    _parameterized_record_type: ClassVar[type[Any] | None] = None
    """The record type bound by subscripting the class, e.g. `CsvReader[MyData]`."""

    def __init_subclass__(cls, delimiter: str | None = None, **kwargs: object) -> None:
        """Define a delimiter upon the subclass, e.g. `class PipeReader(..., delimiter="|")`."""
        if delimiter is not None:
            cls.delimiter = delimiter
        super().__init_subclass__(**kwargs)

    def __class_getitem__(cls, item: Any) -> Any:
        """Parameterize the class, binding a concrete record type so classmethods can see it."""
        if cls._parameterized_record_type is not None:
            raise TypeError(f"{cls.__name__} already has a record type!")
        if isinstance(item, tuple):
            items: tuple[Any, ...] = item  # pyright: ignore[reportUnknownVariableType]
            if len(items) != 1:
                raise TypeError(f"{cls.__name__} takes one record type, but got {len(items)}!")
            item = items[0]
        alias: Any = cast(Any, super()).__class_getitem__(item)
        if not isinstance(item, type) or not is_dataclass(item):
            return alias
        key = (cls, item)
        bound = _BOUND_CLASSES.get(key)
        if bound is None:
            bound = _BOUND_CLASSES[key] = new_class(
                f"{cls.__name__}[{item.__name__}]",
                (alias,),
                exec_body=lambda ns: ns.update({
                    "__module__": cls.__module__,
                    "__qualname__": f"{cls.__qualname__}[{item.__qualname__}]",
                    "_parameterized_record_type": item,
                    "_is_subscripted": True,
                }),
            )
        return bound

    def _bound_record_type(self) -> type[Any]:
        """Return the record type bound to this class, refusing a class that cannot be built."""
        if not hasattr(type(self), "delimiter"):
            name = unbound_name(type(self))
            raise TypeError(
                f"{name} has no delimiter! Subclass it with one,"
                + f" e.g. class MyFormat({name}[RecordType], delimiter='|')."
            )
        if self._parameterized_record_type is None:
            name = type(self).__name__
            raise TypeError(f"{name} must be subscripted with a dataclass, e.g. {name}[MyData]!")
        return self._parameterized_record_type


def unbound_name(cls: type[Any]) -> str:
    """Return the name of the first class, from the given one up, without a record type."""
    return next(
        c for c in cls.__mro__ if getattr(c, "_parameterized_record_type", 1) is None
    ).__name__


class BoundSubscriptableClassmethod(Generic[OwnerType, ConstructorParams]):
    """A subscriptable classmethod bound to its class, awaiting a record type."""

    def __init__(self, owner: Any, func: Callable[..., Any], name: str) -> None:
        self._owner: Any = owner
        self._func: Callable[..., Any] = func
        _ = update_wrapper(self, func)
        self.__name__: str = name

    @overload
    def __getitem__(
        self: "BoundSubscriptableClassmethod[type[TsvReader[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, TsvReader[RecordType]]": ...

    @overload
    def __getitem__(
        self: "BoundSubscriptableClassmethod[type[CsvReader[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, CsvReader[RecordType]]": ...

    @overload
    def __getitem__(
        self: "BoundSubscriptableClassmethod[type[TsvWriter[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, TsvWriter[RecordType]]": ...

    @overload
    def __getitem__(
        self: "BoundSubscriptableClassmethod[type[CsvWriter[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, CsvWriter[RecordType]]": ...

    @overload
    def __getitem__(
        self: "BoundSubscriptableClassmethod[type[DelimitedDataReader[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, DelimitedDataReader[RecordType]]": ...

    @overload
    def __getitem__(
        self: "BoundSubscriptableClassmethod[type[DelimitedDataWriter[Any]], ConstructorParams]",
        record_type: type[RecordType],
    ) -> "Callable[ConstructorParams, DelimitedDataWriter[RecordType]]": ...

    def __getitem__(self, record_type: object) -> Callable[..., Any]:
        """Bind the record type, returning a constructor for that record type."""
        self._refuse_bound_owner()
        if not isinstance(record_type, type) or not is_dataclass(record_type):
            raise TypeError(
                f"{self._usage} must be subscripted with a dataclass, not {record_type}!"
            )
        return partial(self._func, self._owner[record_type])

    @overload
    def __call__(self, *args: Never, **kwargs: Never) -> NoReturn: ...

    @overload
    def __call__(
        self: "BoundSubscriptableClassmethod[type[FixedType], ConstructorParams]",
        *args: ConstructorParams.args,
        **kwargs: ConstructorParams.kwargs,
    ) -> FixedType: ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Build a class fixed to one record type, and refuse to build without a record type."""
        owner = self._owner
        if owner._parameterized_record_type is not None and issubclass(owner, FixedRecordType):
            return self._func(owner, *args, **kwargs)
        self._refuse_bound_owner()
        raise TypeError(
            f"{self._usage} must be subscripted with a dataclass, e.g. {self._usage}[MyData]!"
        )

    @property
    def _usage(self) -> str:
        """The unsubscripted class and method name, e.g. `TsvReader.from_path`."""
        return f"{unbound_name(self._owner)}.{self.__name__}"

    def _refuse_bound_owner(self) -> None:
        """Refuse to bind a record type through a class that already has one."""
        owner = self._owner
        if owner._parameterized_record_type is None:
            return
        if owner.__dict__.get("_is_subscripted", False):
            advice = f"Use {self._usage}[MyData] instead."
        elif issubclass(owner, FixedRecordType):
            advice = f"Call {owner.__name__}.{self.__name__}(...) without a subscript."
        else:
            advice = (
                f"Add FixedRecordType to its bases to call {owner.__name__}.{self.__name__}(...)."
            )
        raise TypeError(f"{owner.__name__} already has a record type! {advice}")


class SubscriptableClassmethod(Generic[ConstructorParams]):
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
    ) -> BoundSubscriptableClassmethod[type[OwnerType], ConstructorParams]:
        return BoundSubscriptableClassmethod(owner, self._func, self._name)
