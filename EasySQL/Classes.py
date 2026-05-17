import inspect
from itertools import zip_longest
from typing import Optional, Union, Any, Sequence, TypeVar, Tuple, List, Type

from .ABC import SQLCommandExecutable
from .Characters import Charset
from .Constraints import NOT_NULL, Unique, UNIQUE, PRIMARY, SQLConstraints
from .Database import EasyDatabase
from .Exceptions import DatabaseSafetyException
from .Logging import logger
from .Types import SQLType
from .Where import WhereAble, Where

__all__ = ['EasyTable', 'EasyColumn', 'EasyForeignColumn', 'SQLData', 'EmptySQLData']


class SQLData:
    def __init__(self, table: "EasyTable", data_array: Union[tuple, list], columns: Union[tuple, list]):
        if len(data_array) != len(columns):
            raise ValueError('Data does not match the columns')

        columns = table.assert_columns(columns)

        self._table = table
        self._data = dict(zip(columns, data_array))

    def __repr__(self):
        return f'<SelectData source="{self._table.name}" values={self._data}>'

    def get(self, column):
        col = self._table.get_column(column)

        if col is None or col not in self._data.keys():
            raise ValueError(f'Unable to find `{column}` in data')

        return col.cast(self._data[col])

    def __iter__(self):
        return iter([self])

    def __len__(self):
        return len(self._data.keys())

    @property
    def data(self):
        return self._data.copy()


class EmptySQLData(SQLData):
    def __init__(self, table: "EasyTable"):
        super().__init__(table, [], [])

    def __iter__(self):
        return iter([])

    def __len__(self):
        return 0

    def __repr__(self):
        return f'<EmptySelectData source="{self._table.name}">'


SD = TypeVar('SD', bound=SQLData)


class EasyColumn(WhereAble):
    def __init__(self, name: str, sql_type: SQLType, *tags: SQLConstraints, default: Any = None, order: int = None):
        self.name = name
        self.sql_type = sql_type
        self.tags = tags
        self.default = default if default else sql_type.default if NOT_NULL in self.tags else None
        self.order = order

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
        return isinstance(other, EasyColumn) and self.name == other.name and self.sql_type == other.sql_type

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
        tags = (NOT_NULL,) if NOT_NULL in tags else ()
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
            self.refer_table = self.refer_column.table

        column = self.refer_table.get_column(self.refer_column)
        if not isinstance(column, EasyColumn):
            raise ValueError(f"Unable to find column \"{self.refer_column}\" in table \"{self.refer_table}\"")

    def __repr__(self):
        return f'<EasyForeignColumn "{self.name}" reference={self.refer_table.name}({self.refer_column.name})>'

    def get_sql(self):
        return EasyColumn.get_sql(self)


T = TypeVar('T')
SOS = Union[T, Sequence[T]]
ECOS = Union[EasyColumn, str]
SOS_ECOS = SOS[ECOS]


class EasyTable:
    _database: EasyDatabase = NotImplemented
    _name: str = NotImplemented
    _columns: Tuple[EasyColumn, ...] = ()
    _data_class: Type[T] = None
    _data_convertor = None

    _charset: Charset = None

    PRIMARY: List[EasyColumn] = None
    UNIQUES: List[Unique] = None

    def __init_subclass__(cls, **kwargs):
        for key in ('database', 'name', 'charset', 'data_class'):
            setattr(cls, f'_{key}', kwargs.pop(key, None) or getattr(cls, f'_{key}'))

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

        cls._columns = tuple(columns)

        if cls._data_class is not None:
            if not issubclass(cls._data_class, SQLData):
                method = getattr(cls._data_class, 'from_sql_data', None)
                if method:
                    if not inspect.ismethod(method):
                        raise TypeError(f'The "from_sql_data" method of data class "{cls._data_class}" must be decorated class method')

                    signature = inspect.signature(method)
                    if len(signature.parameters.values()) != 1:
                        raise TypeError(f'The "from_sql_data" method of data class must take only a SQLData')

                    cls._data_convertor = method
                else:
                    raise TypeError(f'Data class "{cls._data_class}" must be a subclass of SelectData or has "from_sql_data" class method')
            else:
                cls._data_convertor = cls._data_class

    def __init__(self, auto_prepare: bool = True, *, _force=False):
        if self.__class__ == EasyTable and not _force:
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
        exists = bool(self._database.execute_command(command))
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
                self._database.execute_command(command)
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
                    col = self._database.execute_command(command, auto_commit=False).fetchall()[0]
                except Exception:
                    col = None

                if charset.collation != col:
                    command = f'ALTER TABLE {self.name} CONVERT TO CHARACTER SET {charset.name} COLLATE {charset.collation};'
                    self._database.execute_command(command)

                self._charset = charset

            except Exception as e:
                logger.warn(f"Altering the charset of table failed due {e}")

    def count_rows(self):
        return int(self._database.execute_command(f"SELECT COUNT(*) FROM {self.name};").fetchone()[0])

    def get_column(self, target: Union[ECOS], *, force=False) -> Optional[EasyColumn]:
        if target in self._columns:
            return target
        for column in self._columns:
            if column.name == target:
                return column

        if not force:
            return None
        raise ValueError(f'"{target}" is not implemented in the table({self.name}).')

    def select(self, *columns: ECOS):
        assert self.prepared, 'Unable to perform action before preparing the table'
        return Select(self._database, self, *columns).convert_by(self._data_convertor)

    def insert(self, *values: Any):
        assert self.prepared, 'Unable to perform action before preparing the table'
        return Insert(self._database, self, *values)

    def update(self, *columns: ECOS):
        assert self.prepared, 'Unable to perform action before preparing the table'
        return Update(self._database, self, *columns)

    def delete(self, where: Where = None):
        assert self.prepared, 'Unable to perform action before preparing the table'
        return Delete(self._database, self, where)


class Select(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable, *columns: ECOS):
        self._database = database
        self._table = table
        self._columns = table.assert_columns(columns)
        self._where = None
        self._limit = None
        self._offset = None
        self._order = None
        self._desc = False
        self._force_one = False
        self._convertor = None

    def get_value(self) -> str:
        parts = [
            f"SELECT {', '.join([col.name for col in self._columns]) if self._columns else '*'}",
            f"FROM {self._table.name}",
        ]
        if self._where:
            parts.append(self._where.get_value())
        if self._order:
            parts.append(f"ORDER BY {', '.join([col.name for col in self._order])}{' DESC' if self._desc else ''}")
        limit = 1 if self._force_one else self._limit
        if limit is not None:
            parts.append(f"LIMIT {limit}")
        if self._offset is not None:
            parts.append(f"OFFSET {self._offset}")
        return " ".join(parts) + ";"

    def execute(self) -> Union[None, SD, List[SD]]:
        result = self._database.execute(self, auto_commit=False)
        columns = self._columns or self._table.columns
        new_result = [SQLData(self._table, item, columns) for item in result]

        if self._convertor:
            new_result = [self._convertor(item) for item in new_result]

        if self._force_one:
            return new_result[0] if new_result else None
        return (
            EmptySQLData(self._table)
            if not new_result
            else new_result[0]
            if len(new_result) == 1
            else new_result
        )

    def where(self, where: Where) -> "Select": return self._set(where=where)
    def limit(self, limit: int) -> "Select": return self._set(limit=limit)
    def offset(self, offset: int) -> "Select": return self._set(offset=offset)
    def order(self, *order: ECOS) -> "Select": return self._set(order=self._table.assert_columns(order))
    def descending(self) -> "Select": return self._set(desc=True)
    def just_one(self) -> "Select": return self._set(force_one=True)
    def convert_by(self, convertor=None) -> "Select": return self._set(convertor=convertor)


class Insert(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable, *values: Any):
        self._database = database
        self._table = table
        self._columns = table.columns
        self._values = values
        self._update = True

    def get_value(self) -> str:
        if len(self._columns) != len(self._values):
            raise ValueError('Values length do not match with the columns of the table')

        columns = ', '.join(column.name for column in self._columns)
        values = ', '.join(column.parse(value) for column, value in zip(self._columns, self._values))

        extra = (
            f" ON DUPLICATE KEY UPDATE " +
            ', '.join(f"{column.name}={column.parse(value)}" for column, value in zip(self._columns, self._values))
            if self._update else ""
        )

        return f"INSERT INTO {self._table.name} ({columns}) VALUES ({values}){extra};"

    def execute(self):
        return self._database.execute(self)

    def into(self, *columns: ECOS) -> "Insert": return self._set(columns=self._table.assert_columns(columns))

    def do_not_update(self) -> "Insert": return self._set(update=False)


# noinspection SqlWithoutWhere
# The asserts will not allow the missing where
class Update(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable, *columns: ECOS):
        self._database = database
        self._table = table
        self._columns = self._table.assert_columns(columns)
        self._values = []
        self._where = None

    def get_value(self) -> str:
        if len(self._columns) != len(self._values):
            raise ValueError('Values length do not match with the columns')

        set_command = ', '.join([f'{column.name} = {column.parse(value)}' for column, value in zip(self._columns, self._values)])
        return f"UPDATE {self._table.name} SET {set_command}" + f' {self._where.get_value()};' if self._where else ";"

    def execute(self):
        if self._database.safe and self._where is None:
            raise DatabaseSafetyException('Update without any condition is prohibited')

        return self._database.execute(self)

    def where(self, where: Where) -> "Update":
        return self._set(where=where)

    def to(self, *values) -> "Update":
        return self._set(values=values)


# noinspection SqlWithoutWhere
# The asserts will not allow the missing where
class Delete(SQLCommandExecutable):
    def __init__(self, database: EasyDatabase, table: EasyTable = None, where: Where = None):
        self._database = database
        self._table = table
        self._where = where

    def get_value(self) -> str:
        return f"DELETE FROM {self._table.name}" + (f' {self._where.get_value()};' if self._where else ";")

    def execute(self):
        if self._database.safe and self._where is None:
            raise DatabaseSafetyException('Delete without any condition is prohibited')

        return self._database.execute(self)
