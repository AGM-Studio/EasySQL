from typing import List, Optional, Union, Any, Dict

import mysql.connector

from .ABC import ListOrSingle, SQLCommandExecutable, Collection, SQLType, CHARSET
from .Exceptions import MissingArgumentException, MisMatchException, DatabaseSafetyException
from .Logging import logger
from .Where import Where

__all__ = ['EasyDatabase', 'EasyTable', 'EasyColumn',
           'Select', 'Insert', 'Update', 'Delete', 'SelectData']


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
    def __init__(self, host="127.0.0.1", port=3306, database=None, user="root", password=None, charset: Optional[CHARSET] = None):
        if database is None:
            raise MissingArgumentException('database argument is required.')
        if password is None:
            raise MissingArgumentException('password is not provided.')
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

        logger.debug(f'SQL command has been requested to be executed:\n\tCommand: "{operation}"\n\tParameters: {params}\n\tCommit: {auto_commit}')
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

    def prepare(self, alter_columns=True):
        command = f'SHOW TABLES FROM {self._database.name} WHERE Tables_in_{self._database.name} = \'{self._name}\';'
        exists = bool(self._database.execute(command, buffered=True).fetchall())
        if not exists:
            if self._columns:
                command = f"CREATE TABLE IF NOT EXISTS {self._name} ({', '.join([column.get_sql() for column in self._columns])});"
                self._database.execute(command)
            else:
                raise MissingArgumentException('Table defnied does not exists, as there is no columns available to create one')
        else:
            columns = self._database.describe_table(self)
            if self._columns and self.columns != columns:
                lc1 = list(self._columns)
                lc2 = list(columns)
                size = max(len(lc1), len(lc2))
                length = len(str(max(lc1, key=lambda val: len(str(val)))))
                for i in range(len(lc1), size):
                    lc1.append("-" * length)
                for i in range(len(lc2), size):
                    lc2.append("-" * length)

                logger.warn(f'Columns specified do not match with existing ones:\n\tSpecified:{" " * (length - 10)}\t\tExisting:\n\t' +
                            '\n\t'.join([f'{lc1[i]}{" " * (length - len(str(lc1[i])))}\t\t{lc2[i]}' for i in range(size)]))
                raise MisMatchException('Existing table does not match with specified columns.')
            else:
                self._columns = columns

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
            if isinstance(columns, str) or isinstance(columns, EasyColumn):
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
        if selection:
            self.update(values, where)
        else:
            self.insert(values)


class SelectData:
    def __init__(self, table: EasyTable, data_array: Union[tuple, list], columns: Union[tuple, list]):
        if len(data_array) != len(columns):
            raise MisMatchException('Data does not match the columns')

        self._table = table
        self._data = {}
        for i in range(len(data_array)):
            col = self._table.get_column(columns[i])

            if col is None:
                raise MisMatchException(f'Unable to find `{columns[i]}` in the table')

            self._data[col] = col.cast(data_array[i])

    def __repr__(self):
        return f'<SelectData "{self._table.name}">'

    def get(self, column):
        col = self._table.get_column(column)

        if col is None or col not in self._data.keys():
            raise ValueError(f'Unable to find `{column}` in data')

        return self._data[col]

    def __iter__(self):
        return iter([self])

    @property
    def data(self):
        return self._data.copy()


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

    def execute(self) -> Union[None, SelectData, List[SelectData]]:
        self._asserts()
        result = self._database.execute(self.get_value(), auto_commit=False).fetchall()
        columns = self._columns if self._columns else self._table.columns
        new_result = []
        for item in result:
            new_result.append(SelectData(self._table, item, columns))

        return None if len(new_result) == 0 else new_result[0] if len(new_result) == 1 else new_result


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
