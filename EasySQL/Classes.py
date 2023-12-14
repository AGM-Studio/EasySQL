from itertools import zip_longest
from time import sleep
from typing import Optional, Union, Any, Sequence, TypeVar, Tuple, List

import mysql.connector

from .ABC import SQLType, CHARSET, SQLConstraints
from .Constraints import NOT_NULL, Unique, UNIQUE, PRIMARY
from .Exceptions import DatabaseConnectionException
from .Logging import logger
from .Where import Where

__all__ = ['EasyDatabase', 'EasyTable', 'EasyColumn', 'EasyForeignColumn']


def _safe_pop(d: dict, k):
    try:
        return d.pop(k)
    except KeyError:
        return None


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


class EasyColumn:
    def __init__(self, name: str, sql_type: SQLType, *tags: SQLConstraints, default: Any = None, order: int = None):
        self.name = name
        self.sql_type = sql_type
        self.tags = tags
        self.default = default if default else sql_type.default if NOT_NULL in self.tags else None
        self.order = order

        # if PRIMARY in self.tags and NOT_NULL in self.tags:
        #    self.tags = (tag for tag in self.tags if tag != NOT_NULL)

        self.table = None

    def prepare(self, table):
        self.table = table

    def __hash__(self):
        return hash((self.name, self.sql_type))

    def __repr__(self):
        return f'<EasyColumn "{self.name}" of "{self.table}", type={self.sql_type.name}>'

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(other, EasyColumn):
            return self.name == other.name and self.sql_type == other.sql_type
        return False

    def get_sql(self):
        value = f'{self.name} {self.sql_type.name}'
        for tag in self.sql_type.tags:
            value += ' ' + tag
        for tag in self.tags:
            value += ' ' + tag.value
        if self.default is not None:
            value += f' DEFAULT {self.sql_type.parse(self.default)}'
        return value

    def parse(self, value):
        return self.sql_type.parse(value)

    def cast(self, value):
        return self.sql_type.cast(value)


class EasyForeignColumn(EasyColumn):
    @staticmethod
    def of(column: EasyColumn, name: str = None, *tags: SQLConstraints, default: Any = None):
        tags = (NOT_NULL, ) if NOT_NULL in tags else ()
        name = f'{column.name} of {column.table.name}' if name is None else name
        return EasyForeignColumn(name, column.table, column, *tags, default=default)

    def __init__(self, name: str, table: 'EasyTable', reference: EasyColumn, *tags: SQLConstraints, default: Any = None, cascade: bool = True):
        self.refer_table = table
        self.refer_column = reference
        self.cascade = cascade

        tags = (NOT_NULL,) if NOT_NULL in tags else ()
        super().__init__(name, reference.sql_type, *tags, default=default)

    def prepare(self, table: 'EasyTable'):
        super(EasyForeignColumn, self).prepare(table)
        
        if self.refer_table is None:
            self.refer_table = table

        column = self.refer_table.get_column(self.refer_column)
        if not isinstance(column, EasyColumn):
            raise ValueError(f"Unable to find column \"{self.refer_column}\" in table \"{self.refer_table}\"")

    def __repr__(self):
        return f'<EasyForeignColumn "{self.name}" reference={self.refer_table.name}({self.refer_column.name})>'

    def get_sql(self):
        return EasyColumn.get_sql(self)


class EasyDatabase:
    _database: str = None
    _password: str = None
    _host: str = "127.0.0.1"
    _port: int = 3306
    _user: str = "root"

    _charset: CHARSET = None

    _auto_connect: bool = True
    _auto_connect_delay: int = 5
    
    def __init_subclass__(cls, **kwargs):
        for key in ('database', 'password', 'host', 'port', 'user', 'charset', 'auto_connect', 'auto_connect_delay'):
            setattr(cls, f'_{key}', _safe_pop(kwargs, key) or getattr(cls, f'_{key}'))

    def __init__(self, *, _force=False):
        if type(self) == EasyDatabase and not _force:
            raise TypeError('Version 3: Unable to instance \'EasyDatabase\' directly, Create a subclass')

        if self._database is None:
            raise ValueError('database argument is required.')
        if self._password is None:
            raise ValueError('password is not provided.')
        if self._charset is not None and not isinstance(self._charset, CHARSET):
            raise TypeError(f'charset must be type of "CHARSET" or "NONE", not "{type(self._charset)}"')

        self._connection = None
        self._safe = True

        self.set_charset(self._charset)

    def _connect(self, *, attempt=1):
        while self._auto_connect or attempt == 1:
            try:
                logger.info(f'Attempting to make a connection to database \'{self._database}\' on \'{self._host}\'({_ordinal(attempt)} attempt)')
                if self.charset is not None:
                    self._connection = mysql.connector.connect(host=self._host, port=self._port, database=self._database, user=self._user, password=self._password, charset=self._charset.name, collation=self._charset.collation)
                    self._connection.set_charset_collation(self._charset.name, self._charset.collation)
                else:
                    self._connection = mysql.connector.connect(host=self._host, port=self._port, database=self._database, user=self._user, password=self._password)

                if self._connection.is_connected():
                    logger.info(f'Connection was successful')
                    break
                else:
                    raise Exception('unknown reason...')

            except Exception as e:
                logger.warn(f'Connection failed due {e}')

                if self._auto_connect:
                    sleep(self._auto_connect_delay)
            finally:
                attempt += 1

    @property
    def safe(self):
        return self._safe

    def remove_safety(self, *, confirm: bool):
        self._safe = not confirm

    @property
    def connection(self):
        if self._connection is None or not self._connection.is_connected():
            self._connect()

        if self._connection is None or not self._connection.is_connected():
            raise DatabaseConnectionException('Database is not connected')

        return self._connection

    @property
    def cursor(self):
        self._cursor = self.connection.cursor()
        return self._cursor

    @property
    def buffered_cursor(self):
        self._cursor = self.connection.cursor(buffered=True)
        return self._cursor

    @property
    def charset(self):
        return self._charset

    @property
    def name(self):
        return self._database

    def execute(self, operation, params=(), buffered=False, auto_commit=True):
        cursor = self.buffered_cursor if buffered else self.cursor

        logger.debug(f'SQL command has been requested to be executed:\n\tCommand: "{operation}"\n\tParameters: {params}\n\tCommit: {auto_commit}\tBuffered: {buffered}')
        cursor.execute(operation, params)
        if auto_commit:
            self.commit()

        return cursor

    def commit(self):
        return self.connection.commit()

    def describe_table(self, table: 'EasyTable'):
        from EasySQL.Types import string_to_type

        result = self.execute(f'DESCRIBE {self.name}.{table.name};', buffered=True).fetchall()
        columns = []
        for column in result:
            sqltype = string_to_type(column[1])
            if sqltype is None:
                raise TypeError(f'Unable to recognize name "{column[1]}" as a SQLType')

            tags = []
            prim = []
            if column[2] == 'NO':
                tags.append(NOT_NULL)
            if column[3] == 'PRI':
                prim.append(column[0])

            columns.append(EasyColumn(column[0], sqltype, *tags, default=column[4]))

        return tuple(columns)

    def set_charset(self, charset: CHARSET):
        if charset is not None:
            try:
                try:
                    command = f'SELECT DEFAULT_COLLATION_NAME, DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE information_schema.SCHEMATA.SCHEMA_NAME = \'{self.name}\''
                    col, cha = self.execute(command, auto_commit=False).fetchall()[0]
                except Exception:
                    col, cha = (None, None)

                if charset.name != cha or charset.collation != col:
                    command = f'ALTER DATABASE {self._database} CHARACTER SET {charset.name} COLLATE {charset.collation};'
                    self.execute(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of database failed due {e}")


T = TypeVar('T')
SOS = Union[T, Sequence[T]]
ECOS = Union[EasyColumn, str]
SOS_ECOS = SOS[ECOS]


class EasyTable:
    _database: EasyDatabase = NotImplemented
    _name: str = NotImplemented
    _columns: tuple = ()

    _charset: CHARSET = None

    PRIMARY: List[EasyColumn] = None
    UNIQUES: List[Unique] = None

    def __init_subclass__(cls, **kwargs):
        for key in ('database', 'name', 'charset'):
            setattr(cls, f'_{key}', _safe_pop(kwargs, key) or getattr(cls, f'_{key}'))

        cls.PRIMARY = [] if cls.PRIMARY is None else cls.PRIMARY
        cls.UNIQUES = [] if cls.UNIQUES is None else cls.UNIQUES

        if cls._charset is None:
            cls._charset = cls._database.charset

        columns: List[EasyColumn] = [value for value in cls.__dict__.values() if isinstance(value, EasyColumn)]
        for column in columns:
            if UNIQUE in column.tags:
                cls.UNIQUES.append(Unique(column))
            if PRIMARY in column.tags:
                cls.PRIMARY.append(column)

            column.tags = tuple([tag for tag in column.tags if tag != UNIQUE and tag != PRIMARY])

        cls._columns: Tuple[EasyColumn] = tuple(columns)

    def __init__(self, auto_prepare: bool = True, *, _force=False):
        if type(self) == EasyTable and not _force:
            raise TypeError('Version 3: Unable to instance \'EasyTable\' directly, Create a subclass')
        
        if not isinstance(self._database, EasyDatabase):
            raise TypeError('Version 3: Database is not implemented')

        if not isinstance(self._name, str):
            raise TypeError('Version 3: Name is not implemented')
            
        self.__prepared = False

        if auto_prepare:
            self.prepare()

    def assert_columns(self, columns: SOS_ECOS) -> Optional[Sequence[EasyColumn]]:
        if columns is None or columns == '*':
            return None
        if not isinstance(columns, Sequence):
            columns = (columns,)

        return tuple(self.get_column(column, force=True) for column in columns)

    def prepare(self, alter_columns=True):
        command = f'SHOW TABLES FROM {self._database.name} WHERE Tables_in_{self._database.name} = \'{self._name}\';'
        exists = bool(self._database.execute(command, buffered=True).fetchall())
        if not exists:
            if self._columns:
                command = ', '.join([column.get_sql() for column in self._columns])
                if len(self.PRIMARY) > 0:
                    command += f", PRIMARY KEY({', '.join(column.name for column in self.PRIMARY)})"

                for column in self._columns:
                    column.prepare(self)

                    if isinstance(column, EasyForeignColumn):
                        command += f", FOREIGN KEY ({column.name}) REFERENCES {column.refer_table.name}({column.refer_column.name})"
                        if column.cascade:
                            command += " ON DELETE CASCADE"

                for unique in self.UNIQUES:
                    command += f", {unique.value}"

                command = f"CREATE TABLE {self._name} ({command});"
                self._database.execute(command)
            else:
                raise ValueError('No columns where specified and table does not exist')
        else:
            columns = self._database.describe_table(self)
            if self._columns is None or len(self._columns) == 0:
                self._columns = columns
            else:
                c1 = set(self._columns)
                c2 = set(columns)

                if c1 != c2:
                    lc1 = [column.__repr__() for column in c1 - c2]
                    lc2 = [column.__repr__() for column in c2 - c1]
                    lc = zip_longest(lc1, lc2, "")
                    length = len(max(['Provided: '] + lc1, key=lambda col: len(col)))

                    logger.warn(f'Columns specified do not match with existing ones:\n\tProvided:{" " * (length - 10)}\t\tExisting:\n\t' +
                                '\n\t'.join([f'{lci[0]}{" " * (length - len(str(lci[0])))}\t\t{lci[1]}' for lci in lc]))
                    raise ValueError('Existing table does not match with specified columns.')

            for column in self._columns:
                column.prepare(self)

        self.set_charset(self.charset)

        self.__prepared = True

    @property
    def columns(self):
        return self._columns

    @property
    def name(self):
        return self._name

    @property
    def charset(self):
        return self._charset
    
    @property
    def prepared(self):
        return self.__prepared

    def set_charset(self, charset):
        if charset is not None:
            try:
                try:
                    command = f'SELECT TABLE_COLLATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \'{self.name}\''
                    col = self._database.execute(command, auto_commit=False).fetchall()[0]
                except Exception:
                    col = None

                if charset.collation != col:
                    command = f'ALTER TABLE {self.name} CONVERT TO CHARACTER SET {charset.name} COLLATE {charset.collation};'
                    self._database.execute(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of table failed due {e}")

    def count_rows(self):
        return int(self._database.execute(f"SELECT COUNT(*) FROM {self.name};", buffered=True).fetchone()[0])

    def get_column(self, target: Union[ECOS], *, force=False) -> Optional[EasyColumn]:
        if target in self._columns:
            return target
        for column in self._columns:
            if column.name == target:
                return column

        if not force:
            return None
        raise ValueError(f'"{target}" is not implemented in the table({self.name}).')

    def select(self, columns: SOS_ECOS = None, where: Where = None, limit: int = None, offset: int = None, order: SOS_ECOS = None, descending: bool = False, force_one=False):
        from .Commands import Select

        assert self.prepared, 'Unable to perform action before preparing the table'
        return Select(self._database, self, self.assert_columns(columns) if columns is not None else None, where, limit, offset, self.assert_columns(order), descending, force_one).execute()

    def insert(self, columns: SOS_ECOS, values: SOS[Any], update_on_dup: bool = False):
        from .Commands import Insert

        assert self.prepared, 'Unable to perform action before preparing the table'
        return Insert(self._database, self, self.assert_columns(columns) if columns is not None or columns == '*' else self._columns, values, update_on_dup).execute()

    def update(self, columns: SOS_ECOS, values: SOS[Any], where: Where = None):
        from .Commands import Update

        assert self.prepared, 'Unable to perform action before preparing the table'
        return Update(self._database, self, self.assert_columns(columns) if columns is not None else self._columns, values, where).execute()

    def delete(self, where: Where = None):
        from .Commands import Delete

        assert self.prepared, 'Unable to perform action before preparing the table'
        return Delete(self._database, self, where).execute()

    def set(self, columns: SOS_ECOS, values: SOS[Any], where: Where = None):
        selection = self.select(columns, where)
        if selection is not None and len(selection) > 0:
            self.update(columns, values, where)
        else:
            self.insert(self.columns if columns is None or columns == '*' else columns, values)
