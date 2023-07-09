from typing import Optional

from .ABC import CHARSET
from .Classes import EasyDatabase, EasyColumn
from .Types import *

__all__ = ['EasyLocalHost', 'EasyInt64Column', 'EasyInt32Column', 'EasyInt24Column', 'EasyInt16Column', 'EasyInt08Column', 'EasyCharColumn',
           'EasyBitColumn', 'EasyBoolColumn', 'EasyFloatColumn', 'EasyDoubleColumn', 'EasyDecimalColumn', 'EasyStringColumn']


class EasyLocalHost(EasyDatabase):
    _host = "127.0.0.1"
    _port = 3306
    _user = "root"
    _password = ""

    def __init__(self, database, charset: Optional[CHARSET]):
        self._database = database
        self._charset = charset if charset is not None else None

        super().__init__()


class EasyInt64Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, INT64)


class EasyInt32Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, INT32)


class EasyInt24Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, INT24)


class EasyInt16Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, INT16)


class EasyInt08Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, INT8)


class EasyBitColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, BIT)


class EasyBoolColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, BOOL)


class EasyFloatColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, FLOAT)


class EasyDoubleColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, DOUBLE)


class EasyDecimalColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, DECIMAL)


class EasyStringColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, BIGINT)


class EasyCharColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, BIGINT)
