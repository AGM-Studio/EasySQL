from typing import TYPE_CHECKING

from . import Charset, DatabaseConnectionError, SQLCommandExecutable, NOT_NULL
from .Logging import logger
if TYPE_CHECKING:
    from .Classes import EasyColumn


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


class EasyDatabase:
    _database: str = None
    _password: str = None
    _host: str = "127.0.0.1"
    _port: int = 3306
    _user: str = "root"

    _charset: Charset = None

    def __init_subclass__(cls, **kwargs):
        for key in ('database', 'password', 'host', 'port', 'user', 'charset'):
            setattr(cls, f'_{key}', kwargs.pop(key, None) or getattr(cls, f'_{key}'))

    def __init__(self, *, _force=False):
        if self.__class__ in (EasyDatabase, AsyncEasyDatabase) and not _force:
            raise TypeError('Version 3: Unable to instance \'EasyDatabase\' directly, Create a subclass')

        if self._database is None:
            raise ValueError('database argument is required.')
        if self._password is None:
            raise ValueError('password is not provided.')
        if self._charset is not None and not isinstance(self._charset, Charset):
            raise TypeError(f'charset must be type of "CHARSET" or "NONE", not "{type(self._charset)}"')

        self._config = dict(
            host=self._host, port=self._port, database=self._database, user=self._user, password=self._password
        )
        if self.charset is not None:
            self._config['charset'] = self.charset.name
            self._config['collation'] = self.charset.collation

        self._charset_set = False
        self._connection = None
        self._safe = True

    @property
    def safe(self):
        return self._safe

    def remove_safety(self, *, confirm: bool):
        self._safe = not confirm

    @property
    def charset(self):
        return self._charset

    @property
    def name(self):
        return self._database

    def connect(self):
        import mysql.connector

        retries = 5
        while retries > 0:
            try:
                logger.info(f'Attempting to make a connection to database \'{self._database}\' on \'{self._host}\'({_ordinal(6 - retries)} attempt)')
                self._connection = mysql.connector.connect(**self._config)
                if self.charset is not None:
                    self._connection.set_charset_collation(self._charset.name, self._charset.collation)

                if self._connection.is_connected():
                    logger.info(f'Connection was successful')
                    break
                else:
                    raise Exception('unknown reason...')

            except Exception as e:
                logger.warn(f'Connection failed due {e}')
            finally:
                retries -= 1

        if not self._charset_set and self._connection is not None:
            self.set_charset(self._charset)
            self._charset_set = True

    def get_connection(self):
        if self._connection is None or not self._connection.is_connected():
            self.connect()
        if self._connection is None or not self._connection.is_connected():
            raise DatabaseConnectionError('Database is not connected')

        return self._connection

    def execute(self, sql: SQLCommandExecutable, params=(), buffered=False, auto_commit=True):
        result = self.execute_command(sql.get_value(), params, buffered, auto_commit)
        setattr(sql, '_executed', True)
        return result

    def execute_command(self, operation, params=(), buffered=False, auto_commit=True):
        connection = self.get_connection()
        cursor = connection.cursor(buffered=buffered)

        logger.debug(f'SQL command has been requested to be executed:\n\tCommand: "{operation}"\n\tParameters: {params}\n\tCommit: {auto_commit}\tBuffered: {buffered}')
        cursor.execute(operation, params)
        if auto_commit: connection.commit()

        return cursor

    def describe_table(self, table: 'EasyTable'):
        from .Types import string_to_type
        from .Classes import EasyColumn

        result = self.execute_command(f'DESCRIBE {self.name}.{table.name};', buffered=True).fetchall()
        columns = []
        for column in result:
            sqltype = string_to_type(column[1])
            if sqltype is None:
                raise TypeError(f'Unable to recognize name "{column[1]}" as a SQLType')

            tags = []
            prim = []
            if column[2] == 'NO': tags.append(NOT_NULL)
            if column[3] == 'PRI': prim.append(column[0])
            columns.append(EasyColumn(column[0], sqltype, *tags, default=column[4]))

        return tuple(columns)

    def set_charset(self, charset: Charset):
        if charset is not None:
            try:
                try:
                    command = f'SELECT DEFAULT_COLLATION_NAME, DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE information_schema.SCHEMATA.SCHEMA_NAME = \'{self.name}\''
                    col, cha = self.execute_command(command, auto_commit=False).fetchall()[0]
                except Exception:
                    col, cha = (None, None)

                if charset.name != cha or charset.collation != col:
                    command = f'ALTER DATABASE {self._database} CHARACTER SET {charset.name} COLLATE {charset.collation};'
                    self.execute_command(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of database failed due {e}")


class AsyncEasyDatabase(EasyDatabase):
    async def connect(self):
        import aiomysql

        retries = 5
        while retries > 0:
            try:
                logger.info(
                    f'Attempting to make a connection to database \'{self._database}\' on \'{self._host}\'({_ordinal(6 - retries)} attempt)')
                self._connection = await aiomysql.connect(**self._config)
                if self.charset is not None:
                    self._connection.set_charset_collation(self._charset.name, self._charset.collation)

                if self._connection.is_connected():
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
        if self._connection is None or not self._connection.is_connected():
            await self.connect()
        if self._connection is None or not self._connection.is_connected():
            raise DatabaseConnectionError('Database is not connected')

        return self._connection

    async def execute(self, sql: SQLCommandExecutable, params=(), buffered=False, auto_commit=True):
        result = await self.execute_command(sql.get_value(), params, buffered, auto_commit)
        setattr(sql, '_executed', True)
        return result

    async def execute_command(self, operation, params=(), buffered=False, auto_commit=True):
        connection = await self.get_connection()
        cursor = connection.cursor(buffered=buffered)

        logger.debug(
            f'SQL command has been requested to be executed:\n\tCommand: "{operation}"\n\tParameters: {params}\n\tCommit: {auto_commit}\tBuffered: {buffered}')
        cursor.execute(operation, params)
        if auto_commit: connection.commit()

        return cursor

    async def describe_table(self, table: 'EasyTable'):
        from .Types import string_to_type
        from .Classes import EasyColumn

        result = await self.execute_command(f'DESCRIBE {self.name}.{table.name};', buffered=True)
        columns = []
        for column in result.fetchall():
            sqltype = string_to_type(column[1])
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
                    await self.execute_command(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of database failed due {e}")
