from typing import List, Optional, Union, Any, Dict

import mysql.connector

from .ABC import ListOrSingle, SQLCommandExecutable, Collection, SQLType
from .Exceptions import MissingArgumentException, MisMatchException, DatabaseSafetyException
from .Logging import logger
from .Where import Where

__all__ = ['EasyDatabase', 'EasyTable', 'EasyColumn', 'Select', 'Insert', 'Update', 'Delete']


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


class EasyDatabase:
    def __init__(self, host="127.0.0.1", port=3306, database=None, user="root", password=None):
        if database is None:
            raise MissingArgumentException('Database argument is required.')
        if password is None:
            raise MissingArgumentException('Password is not provided.')

        self._database = database
        self._password = password
        self._host = host
        self._port = port
        self._user = user

        self._connection = None
        self._safe = True
        self.prepare()

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

        logger.debug(f'SQL command has been requested to be executed:\n\tCommand: "{operation}"\n\tParameters: {params}\n\tCommit: {auto_commit}')
        cursor.execute(operation, params)
        if auto_commit:
            self.commit()

        return cursor

    def commit(self):
        return self.connection.commit()


class EasyTable:
    def __init__(self, database: EasyDatabase, name: str, columns: List[EasyColumn], auto_prepare=True):
        self._database = database
        self._name = name
        self._columns = tuple(columns)

        if auto_prepare:
            self.prepare()

    def assert_columns(self, columns):
        if columns is None or columns == '*':
            return
        for column in columns:
            if column not in self._columns:
                raise MisMatchException(f'Column "{column}" is not implemented in this table.')

    def manage_values(self, values: Union[Collection[Any], Dict[Union[EasyColumn, str], Any]]) -> Dict[EasyColumn, Any]:
        if isinstance(values, dict):
            columns = [self.get_column(column) for column in values.keys()]
            values = list(values.values())
        else:
            columns = self._columns
            values = list(values)

        self.assert_columns(columns)
        if len(values) != len(columns):
            raise MisMatchException('Values does not match with columns.')

        return {columns[i]: values[i] for i in range(len(columns))}

    def create(self):
        command = f"CREATE TABLE IF NOT EXISTS {self._name} ({', '.join([column.get_sql() for column in self._columns])});"
        self._database.execute(command)

    @property
    def columns(self):
        return self._columns

    @property
    def name(self):
        return self._name

    def get_column(self, target) -> Optional[EasyColumn]:
        if target in self._columns:
            return target
        for column in self._columns:
            if column.name == target:
                return column
        return None

    def select(self, columns: ListOrSingle[Union[EasyColumn, str]] = None, where: Where = None):
        if columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            columns = [self.get_column(column) for column in columns]

        return Select(self._database, columns, self, where).execute()

    def insert(self, values: Union[Collection[Any], Dict[Union[EasyColumn, str], Any]]):
        return Insert(self._database, self.manage_values(values), self).execute()

    def update(self, values: Union[Collection[Any], Dict[Union[EasyColumn, str], Any]], where: Where = None):
        return Update(self._database, self.manage_values(values), self, where).execute()

    def delete(self, where: Where = None):
        return Delete(self._database, self, where).execute()

    def set(self, values: Union[Collection[Any], Dict[Union[EasyColumn, str], Any]], where: Where = None):
        values = self.manage_values(values)
        selection = self.select(values.keys(), where)
        if selection and len(selection) > 0:
            self.update(values, where)
        else:
            self.insert(values)


class Select(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, columns: Collection[EasyColumn] = None, table: EasyTable = None, where: Where = None):
        if table and columns:
            table.assert_columns(columns)

        self._database = database
        self._table = table
        self._columns = columns
        self._where = where

    def _asserts(self):
        if self._table is None:
            raise MissingArgumentException('Table is missing.')
        self._table.assert_columns(self._columns)

    def where(self, where: Where):
        self._where = self._where or where
        return self

    def from_table(self, table: EasyTable):
        self._table = self._table or table
        return self

    def get_value(self) -> str:
        self._asserts()
        columns = ', '.join([column.name for column in self._columns]) if self._columns else '*'
        return f"SELECT {columns} FROM {self._table.name}" + (f' {self._where.get_value()};' if self._where else ";")

    def execute(self):
        self._asserts()
        result = self._database.execute(self.get_value(), auto_commit=False).fetchall()
        columns = self._columns if self._columns else self._table.columns
        new_result = []
        for item in result:
            new_result.append(tuple([columns[i].cast(item[i]) for i in range(len(columns))]))
        return new_result


class Insert(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, values: Dict[EasyColumn, Any], table: EasyTable = None):
        if table:
            table.assert_columns(values.keys())

        self._database = database
        self._values = values
        self._table = table

    def _asserts(self):
        if self._table is None:
            raise MissingArgumentException('Table is missing.')
        self._table.assert_columns(self._values.keys())

    def into_table(self, table: EasyTable):
        self._table = self._table or table
        return self

    def get_value(self) -> str:
        self._asserts()
        columns = ', '.join([column.name for column in self._values.keys()])
        values = ', '.join([column.parse(value) for column, value in self._values.items()])
        return f"INSERT INTO {self._table.name} ({columns}) VALUES ({values});"

    def execute(self):
        self._asserts()
        command = self.get_value()
        return self._database.execute(command).lastrowid


# noinspection SqlWithoutWhere
# The asserts will not allow the missing where
class Update(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, values: Dict[EasyColumn, Any], table: EasyTable = None, where: Where = None):
        if table:
            table.assert_columns(values.keys())

        self._database = database
        self._values = values
        self._table = table
        self._where = where

    def _asserts(self, safety: bool = True):
        if self._table is None:
            raise MissingArgumentException('Table is missing.')
        self._table.assert_columns(self._values.keys())

        if self._database.safe and safety:
            if self._where is None:
                raise DatabaseSafetyException('Safety Warning: Update without any condition is prohibited.')

    def table(self, table: EasyTable):
        self._table = self._table or table
        return self

    def where(self, where: Where):
        self._where = self._where or where
        return self

    def get_value(self) -> str:
        self._asserts(safety=False)
        columns = ', '.join([column.name for column in self._values.keys()])
        set_command = ', '.join([f'{column.name} = {column.parse(value)}' for column, value in self._values.items()])
        return f"UPDATE {self._table.name} SET {set_command}" + f' {self._where.get_value()};' if self._where else ";"

    def execute(self):
        self._asserts()
        command = self.get_value()
        return self._database.execute(command).lastrowid


# noinspection SqlWithoutWhere
# The asserts will not allow the missing where
class Delete(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable = None, where: Where = None):
        self._database = database
        self._table = table
        self._where = where

    def _asserts(self, safety: bool = True):
        if self._table is None:
            raise MissingArgumentException('Table is missing.')

        if self._database.safe:
            if self._where is None and safety:
                raise DatabaseSafetyException('Safety Warning: Delete without any condition is prohibited.')

    def where(self, where: Where):
        self._where = self._where or where
        return self

    def from_table(self, table: EasyTable):
        self._table = self._table or table
        return self

    def get_value(self) -> str:
        self._asserts(safety=False)
        return f"DELETE FROM {self._table.name}" + f' {self._where.get_value()};' if self._where else ";"

    def execute(self):
        self._asserts()
        command = self.get_value()
        return self._database.execute(command).lastrowid
