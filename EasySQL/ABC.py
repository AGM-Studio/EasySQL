from abc import ABC
from typing import Union, TypeVar, List, Set, Tuple, Callable, Any

from EasySQL import SQLTypeException


class SQLType:
    def __init__(self, name, *args, caster: Callable[[Any], Any] = None, get_caster: Callable[["SQLType"], Callable[[Any], Any]] = None, default: Any = None, parser: Callable[[Any], str] = None, modifiable: bool = False):
        self._name = name
        self._args = args

        if caster is None and get_caster is not None:
            caster = get_caster(self)

        if caster is None:
            raise NotImplementedError('cast method is not implemented')

        try:
            caster(default)
        except Exception:
            raise NotImplementedError('cast method is not implemented correctly')

        self._caster = caster
        self._default = default

        self._parser = parser if parser else str
        self._modifiable = modifiable

    def __call__(self, *args):
        if self._modifiable or not args:
            return SQLType(self._name, *args, caster=self._caster, default=self._default)
        raise SQLTypeException('this sql type is not accepting new arguments')

    def __eq__(self, other):
        try:
            return other.name == self.name
        except Exception:
            return False

    @property
    def name(self):
        return f'{self._name}({",".join(self._args)})' if self._args else self._name

    def cast(self, value):
        return self._caster(value)

    def parse(self, value):
        return self._parser(self.cast(value))

    @property
    def default(self):
        return self._default

    @property
    def args(self):
        return self._args


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
