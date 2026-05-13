from .ABC import *
from .Classes import *
from .Where import *
from .Exceptions import *
from .Constraints import *

from .Logging import enable_debug, disable_debug
from .Decorators import auto_init

from . import EasyInstances
from . import Types
from . import Characters as Charsets


def quick_database(name: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: CHARSET = None):
    class QuickDatabase(EasyDatabase):
        _database: str = name
        _password: str = password
        _host: str = host
        _port: int = port
        _user: str = user

        _charset: CHARSET = charset

    database = QuickDatabase()
    database.__class = QuickDatabase
    return database