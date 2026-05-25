from abc import ABC, abstractmethod
from itertools import zip_longest
from typing import Union, Any, TypeVar, overload, Generic, List, Optional, Tuple, Type, Dict, Iterable

from .constants import *
from .database import AsyncDB, SyncedDB
from .logger import logger
from .sql import (
    Where, WhereIsEqual, WhereIsNotEqual,
    WhereIsGreaterEqual, WhereIsGreater,
    WhereIsLesserEqual, WhereIsLesser, SQLType, Types
)

T = TypeVar("T")
D = TypeVar("D", bound="SQLData")
DB = TypeVar("DB", bound=Union[AsyncDB, SyncedDB])


__all__ = [
    "SQLData", "SyncedSQLData", "AsyncSQLData",
    "SyncedSQLTable", "AsyncSQLTable",
    "SQLColumnExpr", "SQLForeignColumnExpr", "SQLColumn", "SQLForeignColumn"
]


class ABCSQLTable(ABC, Generic[D, DB]):
    """Abstract Base Class for Sync & Async SQL table."""
    def __init__(self, cls: Type[D], name: str = None, database: DB = None, charset: Charset = None):
        if database is None: raise ValueError("database is a required argument")
        self.database = database
        self.name = name or self.__name__
        self.charset = charset or self.database.charset
        self.data_class = cls

        self.uniques = getattr(cls, "uniques", [])
        self.primary_map: Dict[str, SQLColumnExpr] = {}

        self._columns: Dict[str, SQLColumnExpr] = {key: value for key, value in cls.__dict__.items() if
                                                   isinstance(value, SQLColumnExpr)}
        for key, column in self._columns.items():
            if UNIQUE in column.tags: self.uniques.append(Unique(column))
            if PRIMARY in column.tags: self.primary_map[key] = column

            column.tags = tuple([tag for tag in column.tags if tag != UNIQUE and tag != PRIMARY])

        self.__prepared = False
        self.database.add_to_prepare(self._prepare())

    @abstractmethod
    def insert(self, _: D = None, __update=True, **kwargs): ...

    @abstractmethod
    def insert_with_no_update(self, _: D = None, **kwargs): ...

    @abstractmethod
    def select(
            self, where = None, *,
            order: Union[Iterable, "SQLColumnExpr", str] = None, descending: bool = False,
            limit: int = None, get_one: bool = None,
            offset: int = None
    ): ...

    @abstractmethod
    def delete(self, where): ...

    @abstractmethod
    def update(self, _: D = None, **kwargs): ...

    @abstractmethod
    def update_where(self, where, _: D = None, **kwargs): ...

    async def _prepare(self):
        # noinspection PyUnresolvedReferences
        async_db: AsyncDB = self.database if isinstance(self.database, AsyncDB) else self.database.async_db
        exists = await async_db.execute(
            f"SHOW TABLES FROM {self.database.name} WHERE Tables_in_{self.database.name} = \"{self.name}\";")
        if not exists:
            if not self._columns:
                raise ValueError("No columns where specified and table does not exist")

            command = ", ".join([column.get_sql() for column in self.columns])
            if len(self.primary_map) > 0:
                command += f", PRIMARY KEY({", ".join(column.name for column in self.primary_map.values())})"

            for column in self._columns:
                if isinstance(column, SQLForeignColumnExpr):
                    command += f", FOREIGN KEY ({column.name}) REFERENCES {column.refer_table.name}({column.refer_column.name})"
                    if column.cascade: command += " ON DELETE CASCADE"

            for unique in self.uniques:
                command += f", {unique._sql}"

            command = f"CREATE TABLE {self.name} ({command});"
            await async_db.execute(command)
        else:
            result = await async_db.execute(f"DESCRIBE {self.database.name}.{self.name};")
            columns = []
            for column in result:
                sqltype = Types.from_string(column[1])
                if sqltype is None:
                    raise TypeError(f"Unable to recognize name \"{column[1]}\" as a SQLType")

                tags = []
                prim = []
                if column[2] == "NO": tags.append(NOT_NULL)
                if column[3] == "PRI": prim.append(column[0])
                columns.append(SQLColumn(column[0], sqltype, *tags, default=column[4]))

            c1 = set(self.columns)
            c2 = set(columns)

            if c1 != c2:
                lc1 = [column.__repr__() for column in c1 - c2]
                lc2 = [column.__repr__() for column in c2 - c1]
                lc = zip_longest(lc1, lc2, "")
                length = len(max(["Provided: "] + lc1, key=lambda col: len(col)))
                raise ValueError(
                    f"Columns specified do not match with existing ones:\n\tProvided:{" " * (length - 10)}\t\tExisting:\n\t" +
                    "\n\t".join([f"{lci[0]}{" " * (length - len(str(lci[0])))}\t\t{lci[1]}" for lci in lc])
                )

        if self.charset is not None:
            try:
                try:
                    command = f"SELECT TABLE_COLLATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \"{self.name}\""
                    col = await async_db.execute(command, auto_commit=False)
                    col = col[0]
                except Exception:
                    col = None

                if self.charset.collation != col:
                    command = f"ALTER TABLE {self.name} CONVERT TO CHARACTER SET {self.charset.name} COLLATE {self.charset.collation};"
                    await self.database.execute(command)

            except Exception as e:
                logger.warn(f"Altering the charset of table failed due {e}")

        self.__prepared = True

    @property
    def columns(self):
        return self._columns.values()

    @property
    def is_prepared(self):
        return self.__prepared

    def get_column(self, target: Union[str, "SQLColumnExpr"], *, force=False) -> Optional["SQLColumnExpr"]:
        if target in self._columns: return target
        for column in self._columns.values():
            if column.name == target: return column

        if not force: return None
        raise ValueError(f"\"{target}\" is not implemented in the table({self.name}).")

    def _handle_obj(self, _: D = None, **kwargs):
        if _ is not None:
            if kwargs: raise TypeError(f"Object {_} is already defined, can't pass extra data anymore!")
            if not isinstance(_, self.data_class): raise TypeError(f"Object of type {type(_)} is not supported!")

        get = (lambda k: getattr(_, k, None)) if _ is not None else (lambda k: kwargs.get(k, None))
        columns, values = [], []
        for key, column in self._columns.items():
            if (value := get(key)) is None: continue
            columns.append(column.name)
            values.append(column.parse(value))

        return columns, values

    def _get_insert_command(self, _: D = None, __update=True, **kwargs):
        columns, values = self._handle_obj(_, **kwargs)
        command = f"INSERT INTO {self.name} ({", ".join(columns)}) VALUES ({", ".join(values)})"
        if __update:
            command += " ON DUPLICATE KEY UPDATE " + (", ".join(f"{c}={v}" for c, v in zip(columns, values))) + ";"
        else:
            command += ";"

        return command

    def _get_update_command(self, where: Optional[Where], _: D = None, **kwargs):
        columns, values = self._handle_obj(_, **kwargs)
        if where is None:
            if _ is None: raise ValueError("Where cannot be None when you don't specify an object.")
            where = self.get_primary_where_for(_)
        return f"UPDATE {self.name} SET {", ".join(f"{c}={v}" for c, v in zip(columns, values))} {where.sql()};"

    def _get_select_command(
            self, where: Where = None, *,
            order: Union[Iterable, "SQLColumnExpr", str] = None, descending: bool = False,
            limit: int = None, get_one: bool = None,
            offset: int = None
    ):
        parts = [f"SELECT * FROM {self.name}"]
        if where: parts.append(where.sql())
        if isinstance(order, enumerate):
            order = [c.name if isinstance(c, SQLColumnExpr) else c for c in order]
        elif isinstance(order, SQLColumnExpr):
            order = [order.name]
        elif isinstance(order, str):
            order = [order]
        elif order is not None:
            order = None
            logger.warn(f"Unknown ordering type {type(order)}")
        if order is not None: parts.append(f"ORDER BY {', '.join(order)}{' DESC' if descending else ''}")

        limit = 1 if get_one else limit
        if limit is not None: parts.append(f"LIMIT {limit}")
        if offset is not None: parts.append(f"OFFSET {offset}")
        return " ".join(parts) + ";"

    def _get_delete_command(self, where: Where = None):
        return f"DELETE FROM {self.name} {where.sql()};"

    def _convert_select_result(self, result: tuple, get_one: bool):
        objects = []
        for data in result:
            kw = {c[0]: c[1].cast(v) for c, v in zip(self._columns.items(), data)}
            objects.append(self.data_class(**kw))

        return objects if not get_one else objects[0] if len(objects) > 0 else None

    def get_primary_where_for(self, obj: D) -> Where:
        if len(self.primary_map) == 0: raise ValueError("There is no primary in your table to generate the Where clause.")
        where_clause = None
        for key, column in self.primary_map.items():
            if where_clause is None: where_clause = WhereIsEqual(column, getattr(obj, key, None))
            else: where_clause &= WhereIsEqual(column, getattr(obj, key, None))

        return where_clause


class SyncedSQLTable(ABCSQLTable[D, SyncedDB]):
    def insert_with_no_update(self, _: D = None, **kwargs):
        return self.insert(_, __update=False, **kwargs)

    def insert(self, _: D = None, *, __update=True, **kwargs):
        return self.database.execute(self._get_insert_command(_, __update, **kwargs))

    def select(
            self, where = None, *,
            order: Union[Iterable, "SQLColumnExpr", str] = None, descending: bool = False,
            limit: int = None, get_one: bool = None,
            offset: int = None
    ):
        result = self.database.execute(self._get_select_command(
            where, order=order, descending=descending, limit=limit, get_one=get_one, offset=offset
        ), auto_commit=False)
        return self._convert_select_result(result, get_one)

    def delete(self, where):
        return self.database.execute(self._get_delete_command(where))

    def update(self, _: D = None, **kwargs):
        return self.database.execute(self._get_update_command(None, _, **kwargs))

    def update_where(self, where, _: D = None, **kwargs):
        return self.database.execute(self._get_update_command(where, _, **kwargs))


class AsyncSQLTable(ABCSQLTable[D, AsyncDB]):
    async def insert_with_no_update(self, _: D = None, **kwargs):
        return await self.insert(_, __update=False, **kwargs)

    async def insert(self, _: D = None, *, __update=True, **kwargs):
        return await self.database.execute(self._get_insert_command(_, __update, **kwargs))

    async def select(
            self, where = None, *,
            order: Union[Iterable, "SQLColumnExpr", str] = None, descending: bool = False,
            limit: int = None, get_one: bool = None,
            offset: int = None
    ):
        result = await self.database.execute(self._get_select_command(
            where, order=order, descending=descending, limit=limit, get_one=get_one, offset=offset
        ), auto_commit=False)
        return self._convert_select_result(result, get_one)

    async def delete(self, where):
        return await self.database.execute(self._get_delete_command(where))

    async def update(self, _: D = None, **kwargs):
        return await self.database.execute(self._get_update_command(None, _, **kwargs))

    async def update_where(self, where, _: D = None, **kwargs):
        return await self.database.execute(self._get_update_command(where, _, **kwargs))


class SQLData:
    table: ABCSQLTable
    uniques: List[Unique]
    _column_attributes: set = set()
    def __init_subclass__(cls, _abstract: bool = False, name: str = None, database: Union[SyncedDB, AsyncDB] = None, charset: Charset = None, **kwargs):
        if _abstract: return
        for key in dir(cls):
            if isinstance(getattr(cls, key), SQLColumnExpr): cls._column_attributes.add(key)

        if isinstance(database, SyncedDB): cls.table = SyncedSQLTable(cls, name, database, charset)
        elif isinstance(database, AsyncDB): cls.table = AsyncSQLTable(cls, name, database, charset)
        else: raise TypeError(f"Database must be an instance of SyncedDB or AsyncDB and not \"{type(database)}\".")

    def __init__(self, **kwargs):
        for key in dir(self):
            obj = getattr(self, key)
            if not isinstance(obj, SQLColumnExpr): continue
            value = kwargs.get(key, None)
            if value is None: value = obj.default
            setattr(self, key, value)

    def __repr__(self):
        return f"{self.__class__.__name__}[{", ".join(f"{k}={getattr(self, k)}" for k in self._column_attributes)}]"


class SyncedSQLData(SQLData, _abstract=True):
    table: SyncedSQLTable
    def __init_subclass__(cls, name: str = None, database: SyncedDB = None, charset: Charset = None, **kwargs):
        for key in dir(cls):
            if isinstance(getattr(cls, key), SQLColumnExpr): cls._column_attributes.add(key)

        if isinstance(database, SyncedDB): cls.table = SyncedSQLTable(cls, name, database, charset)
        else: raise TypeError("Database must be an instance of SyncedDB.")


class AsyncSQLData(SQLData, _abstract=True):
    table: AsyncSQLTable
    def __init_subclass__(cls, name: str = None, database: AsyncDB = None, charset: Charset = None, **kwargs):
        for key in dir(cls):
            if isinstance(getattr(cls, key), SQLColumnExpr): cls._column_attributes.add(key)

        if isinstance(database, AsyncDB): cls.table = AsyncSQLTable(cls, name, database, charset)
        else: raise TypeError("Database must be an instance of SyncedDB.")


class SQLColumnExpr(Generic[T]):
    def __init__(self, name: str, sql_type: SQLType[T], *tags: SQLConstraints, default: T = None):
        self.name: str = name
        self.sql_type: SQLType[T] = sql_type
        self.tags: Tuple[SQLConstraints, ...] = tuple(tags)
        self.default: T = default if default else sql_type.default if NOT_NULL in self.tags else None
        self._owner: Optional[Type[SQLData]]

    def __set_name__(self, owner: Type[SQLData], name):
        if not issubclass(owner, SQLData): raise TypeError("Can only be defined inside a SQLData")
        self._owner = owner

    def __hash__(self):
        return hash((self.name, self.sql_type))

    def __repr__(self):
        return f"Column[\"{self.name}\", type={self.sql_type.name}]"

    def __str__(self):
        return self.name

    def _create_where(self, where_type: Type[Where], value):
        try:
            return where_type(self, self.sql_type.cast(value))
        except Exception as e:
            raise TypeError(f"Unable to create Where clause by \"{value}\"({type(value)}) due to {e}.")

    def __eq__(self, other):
        if isinstance(other, SQLColumnExpr):
            return self.name == other.name and self.sql_type == other.sql_type

        try:
            value = self.sql_type.cast(other)
            return WhereIsEqual(self, value)
        except Exception:
            raise TypeError(f"Unable to create Where clause by \"{other}\"({type(other)})")

    def __ne__(self, other):
        try:
            value = self.sql_type.cast(other)
            return WhereIsNotEqual(self, value)
        except Exception:
            raise TypeError(f"Unable to create Where clause by \"{other}\"({type(other)})")

    def __le__(self, other):
        try:
            value = self.sql_type.cast(other)
            return WhereIsLesserEqual(self, value)
        except Exception:
            raise TypeError(f"Unable to create Where clause by \"{other}\"({type(other)})")

    def __lt__(self, other):
        try:
            value = self.sql_type.cast(other)
            return WhereIsLesser(self, value)
        except Exception:
            raise TypeError(f"Unable to create Where clause by \"{other}\"({type(other)})")

    def __ge__(self, other):
        try:
            value = self.sql_type.cast(other)
            return WhereIsGreaterEqual(self, value)
        except Exception:
            raise TypeError(f"Unable to create Where clause by \"{other}\"({type(other)})")

    def __gt__(self, other):
        try:
            value = self.sql_type.cast(other)
            return WhereIsGreater(self, value)
        except Exception:
            raise TypeError(f"Unable to create Where clause by \"{other}\"({type(other)})")

    @property
    def table(self):
        return self._owner.table if self._owner else None

    def get_sql(self) -> str:
        value = f"{self.name} {self.sql_type.name}"
        for tag in self.sql_type.tags:
            value += " " + tag
        for tag in self.tags:
            value += " " + tag.value
        if self.default is not None:
            value += f" DEFAULT {self.sql_type.parse(self.default)}"
        return value

    def parse(self, value) -> str:
        return self.sql_type.parse(value)

    def cast(self, value) -> T:
        return self.sql_type.cast(value)
    

class SQLForeignColumnExpr(SQLColumnExpr):
    @staticmethod
    def of(column, name: str = None, *tags: SQLConstraints, default: Any = None):
        if not isinstance(column, SQLColumnExpr): raise TypeError("Can only target a SQLColumn")
        tags = (NOT_NULL,) if NOT_NULL in tags else ()
        name = f"{column.name} of {column.table.name}" if name is None else name
        return SQLForeignColumnExpr(name, column.table, column, *tags, default=default)

    def __init__(self, name: str, table: "ABCSQLTable", reference: SQLColumnExpr, *tags: SQLConstraints, default: Any = None, cascade: bool = True):
        self.refer_table = table
        self.refer_column = reference
        self.cascade = cascade

        tags = (NOT_NULL,) if NOT_NULL in tags else ()
        super().__init__(name, reference.sql_type, *tags, default=default)

    def __set_name__(self, owner: Type[SQLData], name):
        if not issubclass(owner, SQLData): raise TypeError("Can only be defined inside a SQLData")
        super().__set_name__(owner, name)
        if self.refer_table is None:
            self.refer_table = self.refer_column.table

        # By getting the column we make sure it exists
        self.refer_table.get_column(self.refer_column, force=True)

    def __repr__(self):
        return f"ForeignColumn[\"{self.name}\" reference={self.refer_table.name}({self.refer_column.name})]"


# noinspection PyPep8Naming
@overload
def SQLColumn(name: str, sql_type: SQLType[T], *tags: SQLConstraints, default: Any = None) -> T: ...
# noinspection PyPep8Naming
def SQLColumn(name: str, sql_type: SQLType[T], *tags: SQLConstraints, default: Any = None):
    return SQLColumnExpr(name, sql_type, *tags, default=default)
# noinspection PyPep8Naming
@overload
def SQLForeignColumn(name: str, column, *tags: SQLConstraints, default: Any = None) -> T: ...
# noinspection PyPep8Naming
def SQLForeignColumn(name: str, column, *tags: SQLConstraints, default: Any = None):
    return SQLForeignColumnExpr.of(column, name, *tags, default=default)