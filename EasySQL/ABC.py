from abc import ABC
from typing import TypeVar
from inspect import currentframe

from .Logging import logger

__all__ = ['SQLCommand', 'SQLCommandExecutable', 'SQLExecutable',
           'make_collection', 'is_collection']

T = TypeVar('T')


class SQLExecutable(ABC):
    def execute(self, operation, params=()):
        raise NotImplementedError


class SQLCommand(ABC):
    def _set(self: T, **kwargs) -> T:
        for key, value in kwargs.items():
            setattr(self, f"_{key}", value)
        return self

    def get_value(self, *args, **kwargs) -> str:
        raise NotImplementedError


class SQLCommandExecutable(SQLCommand, ABC):
    _executed = False

    def __del__(self):
        if not self._executed:
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
