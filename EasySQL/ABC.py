from abc import ABC
from typing import Union, TypeVar, List, Set, Tuple


class SQLType(ABC):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def default(self):
        raise NotImplementedError

    def parse(self, value) -> str:
        raise NotImplementedError

    def cast(self, value):
        raise NotImplementedError


class SQLExecutable(ABC):
    def execute(self, operation, params=()):
        raise NotImplementedError


class SQLCommand(ABC):
    def get_value(self, *args, **kwargs) -> str:
        raise NotImplementedError


class SQLCommandExecutable(SQLCommand, ABC):
    def execute(self, *args, **kwargs):
        raise NotImplementedError


# Extra Typings
T = TypeVar('T')
Collection = Union[List[T], Set[T], Tuple[T]]
ListOrSingle = Union[T, Collection[T]]


def is_collection(value):
    return isinstance(value, (list, set, tuple))
