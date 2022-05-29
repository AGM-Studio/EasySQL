from typing import Union, List, Iterable, Any, Sequence

from .ABC import SQLCommandExecutable
from .Classes import EasyDatabase, EasyTable, EasyColumn
from .Exceptions import DatabaseSafetyException
from .Where import Where


class SelectData:
    def __init__(self, table: EasyTable, data_array: Union[tuple, list], columns: Union[tuple, list]):
        if len(data_array) != len(columns):
            raise ValueError('Data does not match the columns')

        columns = table.assert_columns(columns)

        self._table = table
        self._data = dict(zip(columns, data_array))

    def __repr__(self):
        return f'<SelectData source="{self._table.name}">'

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
    def __init__(self, database: EasyDatabase, table: EasyTable = None, columns: Sequence[EasyColumn] = None, where: Where = None, limit: int = None, offset: int = None, order: Iterable[EasyColumn] = None, descending: bool = False):
        self._database = database
        self._table = table
        self._columns = columns
        self._where = where
        self._limit = limit
        self._offset = offset
        self._order = order
        self._desc = descending

    def get_value(self) -> str:
        sql = f"SELECT {', '.join([column.name for column in self._columns]) if self._columns else '*'} FROM {self._table.name}"
        if isinstance(self._where, Where):
            sql += f" {self._where.get_value()}"
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"
        if self._order is not None:
            sql += f" ORDER BY {','.join([column.name for column in self._order])}"
        return sql + ";"

    def execute(self) -> Union[None, SelectData, List[SelectData]]:
        result = self._database.execute(self.get_value(), auto_commit=False).fetchall()
        columns = self._columns if self._columns else self._table.columns
        new_result = [SelectData(self._table, item, columns) for item in result]

        return None if len(new_result) == 0 else new_result[0] if len(new_result) == 1 else new_result


class Insert(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable, columns: Sequence[EasyColumn], values: Sequence[Any]):
        self._database = database
        self._columns = columns
        self._values = values
        self._table = table

        if len(self._columns) != len(self._values):
            raise ValueError('Values length do not match with the columns')

    def get_value(self) -> str:
        columns = ', '.join([column.name for column in self._columns])
        values = ', '.join([column.parse(value) for column, value in zip(self._columns, self._values)])
        return f"INSERT INTO {self._table.name} ({columns}) VALUES ({values});"

    def execute(self):
        return self._database.execute(self.get_value(), buffered=True).lastrowid


# noinspection SqlWithoutWhere
# The asserts will not allow the missing where
class Update(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable, columns: Sequence[EasyColumn], values: Sequence[Any], where: Where = None):
        self._database = database
        self._columns = columns
        self._values = values
        self._table = table
        self._where = where

        if len(self._columns) != len(self._values):
            raise ValueError('Values length do not match with the columns')

    def get_value(self) -> str:
        set_command = ', '.join([f'{column.name} = {column.parse(value)}' for column, value in zip(self._columns, self._values)])
        return f"UPDATE {self._table.name} SET {set_command}" + f' {self._where.get_value()};' if self._where else ";"

    def execute(self):
        if self._database.safe and self._where is None:
            raise DatabaseSafetyException('Update without any condition is prohibited')

        return self._database.execute(self.get_value(), buffered=True).lastrowid


# noinspection SqlWithoutWhere
# The asserts will not allow the missing where
class Delete(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable = None, where: Where = None):
        self._database = database
        self._table = table
        self._where = where

    def get_value(self) -> str:
        return f"DELETE FROM {self._table.name}" + f' {self._where.get_value()};' if self._where else ";"

    def execute(self):
        if self._database.safe and self._where is None:
            raise DatabaseSafetyException('Update without any condition is prohibited')

        return self._database.execute(self.get_value(), buffered=True).lastrowid
