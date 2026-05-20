import asyncio
import threading
import time
from typing import TYPE_CHECKING

import asyncmy

from .ABC import SQLCommandExecutable
from .Constants import Charset, NOT_NULL
from .Exceptions import DatabaseConnectionError
from .Logging import logger

if TYPE_CHECKING:
    from .Classes import EasyTable


__all__ = ["Database", "AsyncDB", "SyncedDB", "SyncedDatabase", "AsyncDatabase"]


def _ordinal(i: int):
    if 10 < i % 100 < 20:
        return f'{i}th'
    if i % 10 == 1:
        return f'{i}st'
    if i % 10 == 2:
        return f'{i}nd'
    if i % 10 == 3:
        return f'{i}rd'
    return f'{i}th'


class BackgroundEventLoop:
    def __init__(self):
        self._loop = None
        self._thread = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()
            asyncio.set_event_loop(None)

    def start(self):
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run_loop, daemon=True)
                self._thread.start()
                while self._loop is None:
                    time.sleep(0.01)

    def stop(self):
        with self._lock:
            if self._loop and self._thread and self._thread.is_alive():
                self._loop.call_soon_threadsafe(self._loop.stop)
                self._thread.join(timeout=5)
                if self._thread.is_alive():
                    logger.warn("Warning: Background event loop thread did not stop gracefully.")
                self._thread = None
                self._loop = None

    def run_coro_in_loop(self, coro):
        if not self._loop or self._loop.is_closed():
            self.start()
            if not self._loop or self._loop.is_closed():
                raise RuntimeError("Background event loop could not be started or is closed.")

            time.sleep(0.1)
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
            return future.result(timeout=10)
        except asyncio.TimeoutError:
            raise TimeoutError("Coroutine execution timed out.")
        except Exception as e:
            raise e


background_loop_manager = BackgroundEventLoop()
background_loop_manager.start()


def _run_sync(coro):
    return background_loop_manager.run_coro_in_loop(coro)


_db_keywords = (('database', str), ('password', str), ('host', str), ('port', int), ('user', str), ('charset', Charset))
def _get_and_assert_type(obj, data, key, value_type):
    value = data.pop(key, None) or getattr(obj, f'_{key}')
    if isinstance(value, value_type): return value
    raise TypeError(f"{key} must be of type {value_type}")


class Database:
    """This is actually a dataclass, Needs to be created to an async or a sync database to be usable."""
    database: str = None
    password: str = None
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"

    charset: Charset = None

    def __init_subclass__(cls, **kwargs):
        for key, value_type in _db_keywords:
            setattr(cls, f'_{key}', _get_and_assert_type(cls, kwargs, key, value_type))

    def __init__(self):
        self.__async = None
        self.__sync = None

    def create_synced(self) -> "SyncedDB":
        if self.__sync is None:
            self.__sync = self.create().get_synced()

        return self.__sync

    def create(self) -> "AsyncDB":
        if self.__async is None:
            self.__async = AsyncDB(self.database, self.password, self.host, self.port, self.user, self.charset)

        return self.__async


class AsyncDB:
    """The heart of the Database which supports async calls"""
    def __init__(self, database: str, password: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", charset: Charset = None):
        if database is None:
            raise ValueError('database argument is required.')
        if password is None:
            raise ValueError('password is not provided.')
        if self._charset is not None and not isinstance(self._charset, Charset):
            raise TypeError(f'charset must be type of "CHARSET" or "NONE", not "{type(self._charset)}"')

        self._database = self.name = database
        self._password = password
        self._host = host
        self._port = port
        self._user = user

        self._charset = charset
        self._charset_set = False

        self._connection = None
        self._safe = True
        self._sync = None

    @property
    def safe(self):
        return self._safe

    def remove_safety(self, *, confirm: bool):
        self._safe = not confirm

    @property
    def charset(self):
        return self._charset

    def get_synced(self):
        if self._sync is None:
            self._sync = SyncedDB(self)

        return self._sync

    async def connect(self):
        config = dict(
            database=self._database, password=self._password, host=self._host, port=self._port, user=self._user
        )
        if self._charset is not None:
            config['charset'] = self._charset.name
            config['collation'] = self._charset.name

        retries = 5
        while retries > 0:
            try:
                logger.info(f'Attempting to make a connection to database \'{self._database}\' on \'{self._host}\'({_ordinal(6 - retries)} attempt)')
                self._connection = await asyncmy.connect(**config)
                if self.charset is not None:
                    self._connection.set_charset_collation(self._charset.name, self._charset.collation)

                if self._connection.connected:
                    logger.info(f'Connection was successful')
                    break
                else:
                    raise Exception('unknown reason...')

            except Exception as e:
                logger.warn(f'Connection failed due {e}')
            finally:
                retries -= 1

        if not self._charset_set and self._connection is not None:
            await self.set_charset(self._charset)
            self._charset_set = True

    async def get_connection(self):
        if self._connection is None or not self._connection.connected:
            await self.connect()
        if self._connection is None or not self._connection.connected:
            raise DatabaseConnectionError('Database is not connected')

        return self._connection

    async def execute(self, sql: SQLCommandExecutable, params=(), auto_commit=True):
        result = await self.execute_command(sql.get_value(), params, auto_commit)
        setattr(sql, '_executed', True)
        return result

    async def execute_command(self, operation, params=(), auto_commit=True):
        connection = await self.get_connection()
        logger.debug(
            f'SQL command has been requested to be executed:\n'
            f'\tCommand: "{operation}"\n'
            f'\tParameters: {params}\n'
            f'\tCommit: {auto_commit}'
        )

        async with connection.cursor() as cursor:
            await cursor.execute(operation, params)
            if auto_commit: await connection.commit()
            if operation.strip().upper().startswith(("SELECT", "DESCRIBE", "SHOW")):
                result = await cursor.fetchall()
                return result
            if operation.strip().upper().startswith("INSERT"):
                return cursor.lastrowid

            return cursor.rowcount

    async def describe_table(self, table: 'EasyTable'):
        from .Constants import Types
        from .Classes import EasyColumn

        result = await self.execute_command(f'DESCRIBE {self.name}.{table.name};')
        columns = []
        for column in result:
            sqltype = Types.from_string(column[1])
            if sqltype is None:
                raise TypeError(f'Unable to recognize name "{column[1]}" as a SQLType')

            tags = []
            prim = []
            if column[2] == 'NO': tags.append(NOT_NULL)
            if column[3] == 'PRI': prim.append(column[0])
            columns.append(EasyColumn(column[0], sqltype, *tags, default=column[4]))

        return tuple(columns)

    async def set_charset(self, charset: Charset):
        if charset is not None:
            try:
                try:
                    command = f'SELECT DEFAULT_COLLATION_NAME, DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE information_schema.SCHEMATA.SCHEMA_NAME = \'{self.name}\''
                    result = await self.execute_command(command, auto_commit=False)
                    col, cha = result.fetchall()[0]
                except Exception:
                    col, cha = (None, None)

                if charset.name != cha or charset.collation != col:
                    command = f'ALTER DATABASE {self._database} CHARACTER SET {charset.name} COLLATE {charset.collation};'
                    self._connection.set_charset_collation(self._charset.name, self._charset.collation)
                    await self.execute_command(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of database failed due {e}")


class SyncedDB:
    """This class is actually a wrapper where runs an async database in background"""
    def __init__(self, async_db):
        self.__adb = async_db

    @property
    def safe(self):
        return self.__adb.safe

    def remove_safety(self, *, confirm: bool):
        self.__adb.remove_safety(confirm=confirm)

    @property
    def charset(self):
        return self.__adb.charset

    @property
    def name(self):
        return self.__adb.name

    def connect(self):
        return _run_sync(self.__adb.connect())

    def get_connection(self):
        return _run_sync(self.__adb.get_connection())

    def execute(self, sql: SQLCommandExecutable, params=(), auto_commit=True):
        return _run_sync(self.__adb.execute(sql, params, auto_commit=auto_commit))

    def execute_command(self, operation, params=(), auto_commit=True):
        return _run_sync(self.__adb.execute_command(operation, params, auto_commit=auto_commit))

    def describe_table(self, table: 'EasyTable'):
        return _run_sync(self.__adb.describe_table(table))

    def set_charset(self, charset: Charset):
        return _run_sync(self.__adb.set_charset(charset))


# noinspection PyPep8Naming
def SyncedDatabase(name: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: Charset = None) -> SyncedDB:
    """Creates a clean SyncDB without Database Dataclass"""
    return AsyncDB(name, password, host, port, user, charset).get_synced()


# noinspection PyPep8Naming
def AsyncDatabase(name: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: Charset = None) -> AsyncDB:
    """Creates a clean AsyncDB without Database Dataclass"""
    return AsyncDB(name, password, host, port, user, charset)