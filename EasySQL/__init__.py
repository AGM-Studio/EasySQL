from .ABC import *
from .Classes import *
from .Database import EasyDatabase, AsyncEasyDatabase
from .Where import *
from .Exceptions import *
from .Constants import *

from .Logging import enable_debug, disable_debug
from .Decorators import auto_init

from . import EasyInstances


def quick_database(name: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: Charset = None) -> EasyDatabase:
    class QuickDatabase(EasyDatabase):
        _database: str = name
        _password: str = password
        _host: str = host
        _port: int = port
        _user: str = user

        _charset: Charset = charset

    database = QuickDatabase()
    database.__class = QuickDatabase
    return database


def quick_async_database(name: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: Charset = None) -> AsyncEasyDatabase:
    class QuickDatabase(AsyncEasyDatabase):
        _database: str = name
        _password: str = password
        _host: str = host
        _port: int = port
        _user: str = user

        _charset: Charset = charset

    database = QuickDatabase()
    database.__class = QuickDatabase
    return database