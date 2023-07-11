from typing import Type, TypeVar, Union

from .Classes import EasyTable, EasyDatabase

__all__ = ['auto_init']

T = TypeVar('T', bound=Union[EasyDatabase, EasyTable])


def auto_init(cls: Type[T]) -> T:
    assert issubclass(cls, EasyDatabase) or issubclass(cls, EasyTable)

    return cls()
