from typing import Optional

from .Classes import EasyColumn
from .Constants import Charset, Types
from .Database import EasyDatabase

__all__ = ['EasyLocalHost', 'EasyInt64Column', 'EasyInt32Column', 'EasyInt24Column', 'EasyInt16Column', 'EasyInt08Column', 'EasyCharColumn',
           'EasyBitColumn', 'EasyBoolColumn', 'EasyFloatColumn', 'EasyDoubleColumn', 'EasyDecimalColumn', 'EasyStringColumn']


class EasyLocalHost(EasyDatabase):
    _host = "127.0.0.1"
    _port = 3306
    _user = "root"
    _password = ""

    def __init__(self, database, charset: Optional[Charset]):
        self._database = database
        self._charset = charset if charset is not None else None

        super().__init__()


class EasyInt64Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.INT64)


class EasyInt32Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.INT32)


class EasyInt24Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.INT24)


class EasyInt16Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.INT16)


class EasyInt08Column(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.INT8)


class EasyBitColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.BIT)


class EasyBoolColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.BOOL)


class EasyFloatColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.FLOAT)


class EasyDoubleColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.DOUBLE)


class EasyDecimalColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.DECIMAL)


class EasyStringColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.BIGINT)


class EasyCharColumn(EasyColumn):
    def __init__(self, name: str):
        super().__init__(name, Types.BIGINT)
