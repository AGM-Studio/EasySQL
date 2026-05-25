import asyncio
import threading
from abc import ABC, abstractmethod
from typing import Union, Coroutine

import asyncmy

from .constants import Charset
from .exceptions import DatabaseConnectionError
from .logger import logger


__all__ = ["AsyncDB", "AsyncDatabase", "SyncedDB", "SyncedDatabase"]


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


_db_keywords = (('database', str), ('password', str), ('host', str), ('port', int), ('user', str), ('charset', Charset))
def _get_and_assert_type(obj, data, key, value_type):
    value = data.pop(key, None) or getattr(obj, f'_{key}')
    if isinstance(value, value_type): return value
    raise TypeError(f"{key} must be of type {value_type}")


# noinspection PyUnusedLocal
class ABCDatabase(ABC):
    """Abstract Base Class for Sync & Async database."""
    @abstractmethod
    def __init__(self, database: str, password: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", charset: "Charset" = None): ...

    @property
    @abstractmethod
    def charset(self) -> "Charset": ...

    @abstractmethod
    def connect(self): ...

    @abstractmethod
    def get_connection(self): ...

    @abstractmethod
    def execute(self, command: str, params=(), auto_commit=True): ...

    @abstractmethod
    def set_charset(self, charset: "Charset"): ...

    @abstractmethod
    def add_to_prepare(self, prepare: Coroutine): ...

    @abstractmethod
    def prepare(self): ...


class AsyncDB(ABCDatabase):
    """The heart of the Database which supports async calls"""
    def __init__(self, database: str, password: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", charset: Charset = None):
        if database is None:
            raise ValueError('database argument is required.')
        if password is None:
            raise ValueError('password is not provided.')
        if charset is not None and not isinstance(charset, Charset):
            raise TypeError(f'charset must be type of "CHARSET" or "NONE", not "{type(charset)}"')

        self._database = self.name = database
        self._password = password
        self._host = host
        self._port = port
        self._user = user

        self._charset = charset
        self._charset_set = False

        self._connection = None
        self._sync = None
        self._prepares = []

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

    async def execute(self, command: str, params=(), auto_commit=True):
        connection = await self.get_connection()
        logger.debug(
            f'SQL command has been requested to be executed:\n'
            f'\tCommand: "{command}"\n'
            f'\tParameters: {params}\n'
            f'\tCommit: {auto_commit}'
        )

        async with connection.cursor() as cursor:
            await cursor.execute(command, params)
            if auto_commit: await connection.commit()
            if command.strip().upper().startswith(("SELECT", "DESCRIBE", "SHOW")):
                result = await cursor.fetchall()
                return result
            if command.strip().upper().startswith("INSERT"):
                return cursor.lastrowid

            return cursor.rowcount

    async def set_charset(self, charset: Charset):
        if charset is not None:
            try:
                try:
                    command = f'SELECT DEFAULT_COLLATION_NAME, DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE information_schema.SCHEMATA.SCHEMA_NAME = \'{self.name}\''
                    result = await self.execute(command, auto_commit=False)
                    col, cha = result.fetchall()[0]
                except Exception:
                    col, cha = (None, None)

                if charset.name != cha or charset.collation != col:
                    command = f'ALTER DATABASE {self._database} CHARACTER SET {charset.name} COLLATE {charset.collation};'
                    self._connection.set_charset_collation(self._charset.name, self._charset.collation)
                    await self.execute(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of database failed due {e}")

    def add_to_prepare(self, prepare: Coroutine):
        self._prepares.append(prepare)

    async def prepare(self):
        while self._prepares:
            await self._prepares.pop(0)


class SyncedDB(ABCDatabase):
    """This class is actually a wrapper where runs an async database in background"""
    def __init__(self, async_db):
        self.async_db = async_db
        self._loop = None
        self._thread = None
        self._lock = threading.Lock()
        self._start_loop()

    def _start_loop(self):
        with self._lock:
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(target=self._run_forever, daemon=True)
                self._thread.start()

    def _run_forever(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_sync(self, coro):
        """Helper to safely run coroutines on the background thread."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    @property
    def charset(self):
        return self.async_db.charset

    @property
    def name(self):
        return self.async_db.name

    def connect(self):
        return self._run_sync(self.async_db.connect())

    def get_connection(self):
        return self._run_sync(self.async_db.get_connection())

    def execute(self, command: str, params=(), auto_commit=True):
        return self._run_sync(self.async_db.execute(command, params, auto_commit=auto_commit))

    def set_charset(self, charset: Charset):
        return self._run_sync(self.async_db.set_charset(charset))

    def add_to_prepare(self, prepare: Coroutine):
        self.async_db.add_to_prepare(prepare)

    def prepare(self):
        self._run_sync(self.async_db.prepare())


# noinspection PyPep8Naming
def SyncedDatabase(database: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: Charset = None) -> SyncedDB:
    return AsyncDB(database, password, host, port, user, charset).get_synced()
# noinspection PyPep8Naming
def AsyncDatabase(database: str, host: str = "127.0.0.1", port: int = 3306, user: str = "root", password: str = None, charset: Charset = None) -> AsyncDB:
    return AsyncDB(database, password, host, port, user, charset)