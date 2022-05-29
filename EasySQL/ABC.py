from abc import ABC
from typing import Callable, Any


class SQLType:
    def __init__(self, name, *args, caster: Callable[[Any], Any] = None, get_caster: Callable[["SQLType"], Callable[[Any], Any]] = None, default: Any = None, parser: Callable[[Any], str] = None, modifiable: bool = False):
        self._name = name
        self._args = args

        self._modify_args = dict(caster=caster, get_caster=get_caster, default=default, parser=parser)

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

        self._parser = parser if parser is not None else lambda value: 'null' if value is None else str(value)
        self._modifiable = modifiable

    def __call__(self, *args):
        if self._modifiable or not args:
            return SQLType(self._name, *args, **self._modify_args, modifiable=self._modifiable)

        from EasySQL import SQLTypeException
        raise SQLTypeException('this sql type is not accepting new arguments')

    def __eq__(self, other):
        try:
            return other.name == self.name and other.args == self.args
        except Exception:
            return False

    def __hash__(self):
        return hash((self.name, self.args))

    def __repr__(self):
        return f'<SQLTYPE "{self.name}">'

    @property
    def name(self):
        return f'{self._name}({",".join([str(arg) for arg in self._args])})' if self._args else self._name

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

    @property
    def modifiable(self):
        return self._modifiable


class CHARSET:
    def __init__(self, name, collation, maxlen=1, description=None):
        self._name = name
        self.__doc__ = description
        self._collation = collation
        self._maxlen = maxlen

    def __repr__(self):
        return f'<CHARSET "{self._name}">'

    def __str__(self):
        return self._name

    @property
    def name(self):
        return self._name

    @property
    def collation(self):
        return self._collation

    @property
    def maxlen(self):
        return self._maxlen


class SQLExecutable(ABC):
    def execute(self, operation, params=()):
        raise NotImplementedError


class SQLCommand(ABC):
    def get_value(self, *args, **kwargs) -> str:
        raise NotImplementedError


class SQLCommandExecutable(SQLCommand, ABC):
    def execute(self, *args, **kwargs):
        raise NotImplementedError


def make_collection(value):
    return value if is_collection(value) else [value]


def is_collection(value):
    return isinstance(value, (list, set, tuple))
