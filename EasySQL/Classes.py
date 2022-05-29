from itertools import zip_longest
from typing import List, Optional, Union, Any, Sequence, TypeVar

import mysql.connector

from .ABC import SQLType, CHARSET
from .Logging import logger
from .Where import Where

__all__ = ['EasyDatabase', 'EasyTable', 'EasyColumn', 'EasyForeignColumn']


class EasyColumn:
    def __init__(self, name: str, sql_type: SQLType, default: Any = None, primary: bool = False, not_null: bool = False, auto_increment: bool = False):
        self.name = name
        self.sql_type = sql_type
        self.default = default if default else sql_type.default if not_null else None
        self.primary = primary
        self.auto_increment = auto_increment
        self.not_null = not_null

    def __hash__(self):
        return hash((self.name, self.sql_type))

    def __repr__(self):
        return f'<EasyColumn "{self.name}" type={self.sql_type.name}>'

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(other, EasyColumn):
            return self.name == other.name and self.sql_type == other.sql_type
        return False

    def get_sql(self):
        value = f'{self.name} {self.sql_type.name}'
        if self.primary:
            value += ' PRIMARY KEY'
        elif self.not_null:
            value += ' NOT NULL'
        if self.auto_increment:
            value += ' AUTO_INCREMENT'
        if self.default is not None:
            value += f' DEFAULT {self.sql_type.parse(self.default)}'
        return value

    def parse(self, value):
        return self.sql_type.parse(value)

    def cast(self, value):
        return self.sql_type.cast(value)


class EasyForeignColumn(EasyColumn):
    def __init__(self, name: str, table: 'EasyTable', reference: Union[EasyColumn, str], default: Any = None, not_null: bool = False):
        column = table.get_column(reference)
        if column is None:
            raise ValueError(f'Unable to find `{reference}` in the table')

        self.refer_table = table
        self.refer_column = column

        super().__init__(name, column.sql_type, default, False, not_null, False)

    def __repr__(self):
        return f'<EasyForeignColumn "{self.name}" reference={self.refer_table.name}({self.refer_column.name})>'

    def get_sql(self):
        return EasyColumn.get_sql(self) + f' REFERENCES {self.refer_table.name}({self.refer_column.name})'


class EasyDatabase:
    def __init__(self, host="127.0.0.1", port=3306, database=None, user="root", password=None, charset: Optional[CHARSET] = None):
        if database is None:
            raise ValueError('database argument is required.')
        if password is None:
            raise ValueError('password is not provided.')
        if charset is not None and not isinstance(charset, CHARSET):
            raise TypeError(f'charset must be type of "CHARSET" or "NONE", not "{type(charset)}"')

        self._database = database
        self._password = password
        self._host = host
        self._port = port
        self._user = user
        self._charset = charset

        self._connection = None
        self._safe = True
        self.prepare()

        if charset is not None:
            try:
                command = f'ALTER DATABASE {self._database} CHARACTER SET = {charset.name} COLLATE = {charset.collation};'
                self.execute(command)
            except Exception as e:
                logger.warn(f"Altering the charset of database failed due {str(e)}")

    @property
    def safe(self):
        return self._safe

    def remove_safety(self, *, confirm: bool):
        self._safe = not confirm

    @property
    def connection(self):
        try:
            self._connection.ping()
        except Exception as e:
            if self._connection is None:
                e = 'the initializing connection'
            logger.info(f'Connecting to the database called {self._database} due {e}')
            self._connection = mysql.connector.connect(host=self._host, port=self._port, database=self._database, user=self._user, password=self._password)
        return self._connection

    def prepare(self):
        return self.connection.is_connected()

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

    def table(self, name: str, columns: List[EasyColumn], auto_prepare=True):
        return EasyTable(self, name, columns, auto_prepare)

    def describe_table(self, table: 'EasyTable'):
        from EasySQL import string_to_type

        result = self.execute(f'DESCRIBE {self.name}.{table.name};', buffered=True).fetchall()
        columns = []
        for column in result:
            sqltype = string_to_type(column[1])
            if sqltype is None:
                raise TypeError(f'Unable to recognize name "{column[1]}" as a SQLType')

            columns.append(EasyColumn(column[0], sqltype, not_null=column[2] == 'NO', primary=column[3] == 'PRI', default=column[4]))

        return tuple(columns)


T = TypeVar('T')
SOS = Union[T, Sequence[T]]
ECOS = Union[EasyColumn, str]
SOS_ECOS = SOS[ECOS]


class EasyTable:
    def __init__(self, database: EasyDatabase, name: str, columns: List[EasyColumn], auto_prepare=True):
        self._database = database
        self._name = name
        self._columns = tuple(columns)

        if auto_prepare:
            self.prepare()

    def assert_columns(self, columns: SOS_ECOS) -> Optional[Sequence[EasyColumn]]:
        if columns is None or columns == '*':
            return None
        if not isinstance(columns, Sequence):
            columns = (columns, )

        return tuple(self.get_column(column, force=True) for column in columns)

    def prepare(self, alter_columns=True):
        command = f'SHOW TABLES FROM {self._database.name} WHERE Tables_in_{self._database.name} = \'{self._name}\';'
        exists = bool(self._database.execute(command, buffered=True).fetchall())
        if not exists:
            if self._columns:
                command = f"CREATE TABLE IF NOT EXISTS {self._name} ({', '.join([column.get_sql() for column in self._columns])});"
                self._database.execute(command)
            else:
                raise ValueError('Unable to create the table since columns are not specified.')
        else:
            columns = self._database.describe_table(self)
            if self._columns is None:
                self._columns = columns
            else:
                c1 = set(self._columns)
                c2 = set(columns)

                if c1 != c2:
                    lc1 = [column.__repr__() for column in c1 - c2]
                    lc2 = [column.__repr__() for column in c2 - c1]
                    lc = zip_longest(lc1, lc2, "")
                    length = len(max(lc1, key=lambda col: len(col)))

                    logger.warn(f'Columns specified do not match with existing ones:\n\tProvided:{" " * (length - 10)}\t\tExisting:\n\t' +
                                '\n\t'.join([f'{lci[0]}{" " * (length - len(str(lci[0])))}\t\t{lci[1]}' for lci in lc]))
                    raise ValueError('Existing table does not match with specified columns.')

    @property
    def columns(self):
        return self._columns

    @property
    def name(self):
        return self._name

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

    def select(self, columns: SOS_ECOS = None, where: Where = None, limit: int = None, offset: int = None, order: SOS_ECOS = None, descending: bool = False):
        from .Commands import Select

        return Select(self._database, self, self.assert_columns(columns) if columns is not None else None, where, limit, offset, self.assert_columns(order), descending).execute()

    def insert(self, columns: SOS_ECOS, values: SOS[Any]):
        from .Commands import Insert

        return Insert(self._database, self, self.assert_columns(columns) if columns is not None else self._columns, values).execute()

    def update(self, columns: SOS_ECOS, values: SOS[Any], where: Where = None):
        from .Commands import Update

        return Update(self._database, self, self.assert_columns(columns) if columns is not None else self._columns, values, where).execute()

    def delete(self, where: Where = None):
        from .Commands import Delete

        return Delete(self._database, self, where).execute()

    def set(self, columns: SOS_ECOS, values: SOS[Any], where: Where = None):
        selection = self.select(columns, where)
        if selection:
            self.update(columns, values, where)
        else:
            self.insert(columns, values)
