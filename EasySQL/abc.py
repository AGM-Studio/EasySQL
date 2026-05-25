from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union, Generic, TypeVar, Iterable

if TYPE_CHECKING:
    from .classes import SQLColumnExpr

D = TypeVar("D", bound="SQLData")
DB = TypeVar("DB", bound="ABCDatabase")

class ABCSQLTable(ABC, Generic[D, DB]):
    """Abstract Base Class for Sync & Async SQL table."""
    @abstractmethod
    def insert(self, _: D = None, __update=True, **kwargs): ...

    @abstractmethod
    def insert_with_no_update(self, _: D = None, **kwargs): ...

    @abstractmethod
    def select(
            self, where = None, *,
            order: Union[Iterable, "SQLColumnExpr", str] = None, descending: bool = False,
            limit: int = None, get_one: bool = None,
            offset: int = None
    ): ...

    @abstractmethod
    def delete(self, where): ...

    @abstractmethod
    def update(self, _: D = None, **kwargs): ...

    @abstractmethod
    def update_where(self, where, _: D = None, **kwargs): ...
