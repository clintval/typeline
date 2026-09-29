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
        alias = super().__class_getitem__(item)  # type: ignore[misc]  # pyright: ignore[reportAttributeAccessIssue]  # ty: ignore[unresolved-attribute]
        if not isinstance(item, type) or not is_dataclass(item):
            return alias
        key = (cls, item)
        if key not in _BOUND_CLASSES:
            _ = _BOUND_CLASSES.setdefault(
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
        return _BOUND_CLASSES[key]

    def _bound_record_type(self) -> type[Any]:
        """Return the record type bound to this class, refusing an unsubscripted class."""
        if self._parameterized_record_type is None:
            name = type(self).__name__
            raise TypeError(f"{name} must be subscripted with a dataclass, e.g. {name}[MyData]!")
        return self._parameterized_record_type


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

    def __call__(self, *_args: Never, **_kwargs: Never) -> NoReturn:
        """Refuse to construct without a record type."""
        self._refuse_bound_owner()
        raise TypeError(
            f"{self._usage} must be subscripted with a dataclass, e.g. {self._usage}[MyData]!"
        )

    @property
    def _usage(self) -> str:
        """The unsubscripted class and method name, e.g. `TsvReader.from_path`."""
        unbound = next(c for c in self._owner.__mro__ if c._parameterized_record_type is None)
        return f"{unbound.__name__}.{self.__name__}"

    def _refuse_bound_owner(self) -> None:
        """Refuse access through a class that already has a record type."""
        if self._owner._parameterized_record_type is not None:
            raise TypeError(
                f"{self._owner.__name__} already has a record type!"
                + f" Use {self._usage}[MyData] instead."
            )


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
