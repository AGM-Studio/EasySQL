from abc import ABC
from inspect import currentframe
from typing import TypeVar

from .logger import logger

__all__ = [
    "SQLObject", "SQLCommandExecutable", "make_collection", "is_collection",
    "Where", "WhereIsEqual", "WhereIsNotEqual",
    "WhereIsGreater", "WhereIsLesser", "WhereIsGreaterEqual", "WhereIsLesserEqual",
    "WhereIsLike", "WhereIsIn", "WhereIsBetween"
]

T = TypeVar("T")


class SQLObject(ABC):
    def _set(self: T, **kwargs) -> T:
        for key, value in kwargs.items():
            setattr(self, f"_{key}", value)
            
        return self

    def sql(self, *args, **kwargs) -> str:
        raise NotImplementedError


class SQLCommandExecutable(SQLObject, ABC):
    _executed = False
    def __del__(self):
        if self._executed: return
        caller_frame = currentframe().f_back
        if caller_frame:
            logger.warning(f"Command is created without being executed!\n\tLine #{caller_frame.f_lineno}: {caller_frame.f_code.co_filename}")
        else:
            logger.warning(f"One command is created without being executed!")

    def execute(self, *args, **kwargs):
        raise NotImplementedError


def make_collection(value):
    return value if is_collection(value) else [value]


def is_collection(value):
    return isinstance(value, (list, set, tuple))


# noinspection PyPep8Naming
class Where(SQLObject):
    def __init__(self, sql_string):
        self.value = sql_string

    def sql(self) -> str:
        return f"WHERE {self.value}"

    def AND(self, other):
        if not isinstance(other, Where): raise TypeError(f"Unsupported type \"{type(other)}\"")
        return Where(f"({self.value} AND {other.value})")

    def OR(self, other):
        if not isinstance(other, Where): raise TypeError(f"Unsupported type \"{type(other)}\"")
        return Where(f"({self.value} OR {other.value})")

    def NOT(self):
        return Where(f"NOT {self.value}")

    def __repr__(self):
        return self.sql()

    def __and__(self, other):
        if isinstance(other, Where):
            return self.AND(other)

        raise TypeError(f"unsupported operand type(s) for &: \"{type(self)}\" and \"{type(other)}\"")

    def __or__(self, other):
        if isinstance(other, Where):
            return self.OR(other)

        raise TypeError(f"unsupported operand type(s) for |: \"{type(self)}\" and \"{type(other)}\"")

    def __invert__(self):
        return self.NOT()


class WhereIsEqual(Where):
    def __init__(self, column, value):
        self.value = value
        self.column = column
        super().__init__(f"{column.name} = {column.parse(value)}")

    def __invert__(self):
        return WhereIsNotEqual(self.column, self.value)


class WhereIsNotEqual(Where):
    def __init__(self, column, value):
        self.value = value
        self.column = column
        super().__init__(f"{column.name} <> {column.parse(value)}")

    def __invert__(self):
        return WhereIsEqual(self.column, self.value)


class WhereIsGreater(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} > {column.parse(value)}")


class WhereIsGreaterEqual(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} >= {column.parse(value)}")


class WhereIsLesser(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} < {column.parse(value)}")


class WhereIsLesserEqual(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} <= {column.parse(value)}")


class WhereIsLike(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} LIKE {column.parse(value).replace("%", "%%")}")


class WhereIsIn(Where):
    def __init__(self, column, values):
        values = (column.parse(value) for value in values)
        super().__init__(f"{column.name} IN {values}")


class WhereIsBetween(Where):
    def __init__(self, column, a, b):
        values = (column.parse(a), column.parse(b))
        super().__init__(f"{column.name} = {values}")
