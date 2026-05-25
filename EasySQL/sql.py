from typing import TypeVar, Generic, Dict, Type, Iterable, Optional, List

__all__ = [
    "SQLType", "BitSQLType", "IntegerSQLType", "UnsignedIntegerSQLType", "SignedIntegerSQLType",
    "StringSQLType", "BoolSQLType", "FloatSQLType", "DoubleSQLType", "Types",
    "Where", "WhereIsEqual", "WhereIsNotEqual",
    "WhereIsGreater", "WhereIsLesser", "WhereIsGreaterEqual", "WhereIsLesserEqual",
    "WhereIsLike", "WhereIsIn", "WhereIsBetween"
]


T = TypeVar("T")


class SQLType(Generic[T]):
    mapping: Dict[str, "SQLType"] = {}
    def __init__(self, cls: Type[T], name, *other_names, args: Iterable[int] = None, default: T = None, tags: Iterable[str] = None):
        self._cls = cls
        self._name = name
        self._args = args or ()
        self._tags = tags or ()
        self._default = default

        SQLType.mapping[name.lower()] = self
        for name in other_names: SQLType.mapping[name.lower()] = self

    def __eq__(self, other):
        return isinstance(other, SQLType) and other.name == self.name and other.args == self.args

    def __hash__(self):
        return hash((self.name, self.args))

    def __repr__(self):
        return f"{self.__class__.__name__}({",".join(str(arg) for arg in self._args)})"

    @property
    def name(self):
        return f"{self._name}({",".join(str(arg) for arg in self._args)})" if self._args else self._name

    @property
    def tags(self) -> tuple:
        return self._tags

    def cast(self, value) -> Optional[T]:
        return None if value is None else self._cls(value)

    def parse(self, value: T) -> str:
        value = self.cast(value)
        return "null" if value is None else str(value)

    @property
    def default(self) -> Optional[T]:
        return self._default

    @property
    def args(self):
        return self._args


class BitSQLType(SQLType[int]):
    def __init__(self, bit_size):
        super().__init__(int, "BIT", args=(bit_size,), default=0)
        self.bit_size = bit_size
        self.minimum = 0
        self.maximum = 2 ** bit_size

    def __call__(self, bit_size):
        return BitSQLType(bit_size)

    def cast(self, value) -> Optional[int]:
        if value is None: return None

        value = int(value)
        if self.minimum <= value <= self.maximum: return value

        raise ValueError(f"Can only accept between {self.minimum} and {self.maximum}, but got {value}")


class IntegerSQLType(SQLType[int]):
    def __init__(self, bit_size: int, name, *other_names, unsigned: bool = False):
        super().__init__(int, name, *other_names, default=0, tags=("UNSIGNED",) if unsigned else None)
        self.bit_size = bit_size
        self.minimum = 0 if unsigned else (-(2 ** (bit_size - 1)))
        self.maximum = 2 ** bit_size if unsigned else (2 ** (bit_size - 1) - 1)

    def cast(self, value) -> Optional[int]:
        if value is None: return None

        value = int(value)
        if self.minimum <= value <= self.maximum: return value

        raise ValueError(f"Can only accept between {self.minimum} and {self.maximum}, but got {value}")


class SignedIntegerSQLType(IntegerSQLType):
    def __init__(self, bit_size: int, name, *other_names):
        super().__init__(bit_size, name, *other_names)
        self._unsigned = UnsignedIntegerSQLType(bit_size, name, *other_names)

    # noinspection PyPep8Naming
    @property
    def UNSIGNED(self) -> "UnsignedIntegerSQLType":
        if self._unsigned: return self._unsigned
        raise TypeError(f"{self.name} does not support unsigned")


class UnsignedIntegerSQLType(IntegerSQLType):
    def __init__(self, bit_size: int, name, *other_names):
        super().__init__(bit_size, name, *other_names, unsigned=True)


class StringSQLType(SQLType[str]):
    def __init__(self, bit_size: int, name, *other_names):
        super().__init__(str, name, *other_names, args=(bit_size,), default="")

    def __call__(self, bit_size: int):
        return StringSQLType(bit_size, self._name)

    def parse(self, value: str) -> str:
        return "null" if value is None else f"\"{value}\""


class BoolSQLType(SQLType[bool]):
    def __init__(self, other_names: List[str] = None):
        super().__init__(bool, "BIT", *other_names, args=(1,), default=False)

    def cast(self, value) -> Optional[bool]:
        return None if value is None else value == b"\x01"

    def parse(self, value: bool) -> str:
        return "1" if value else "0"


class FloatSQLType(SQLType[float]):
    def __init__(self, name, *other_names):
        super().__init__(float, name, *other_names, default=0.0)


class DoubleSQLType(SQLType[float]):
    def __init__(self, a, b, name, *other_names):
        super().__init__(float, name, *other_names, args=(a, b), default=0.0)

    def __call__(self, a, b):
        return DoubleSQLType(a, b, self._name)


class Types:
    BIT = BitSQLType(1)
    INT64 = BIGINT = SignedIntegerSQLType(64, "BIGINT")
    INT32 = INT = INTEGER = SignedIntegerSQLType(32, "INT", "INTEGER")
    INT24 = MEDIUMINT = SignedIntegerSQLType(24, "MEDIUMINT")
    INT16 = SMALLINT = SignedIntegerSQLType(16, "SMALLINT")
    INT8 = TINYINT = IntegerSQLType(8, "TINYINT")

    BOOL = BoolSQLType(["bool", "boolean"])

    FLOAT = FloatSQLType("FLOAT")
    DOUBLE = DoubleSQLType(12, 6, "DOUBLE")
    DEC = DECIMAL = DoubleSQLType(12, 6, "DECIMAL", "DEC")

    STRING = VARCHAR = StringSQLType(255, "VARCHAR")
    CHAR = StringSQLType(255, "CHAR")


    mapping = {
        'bigint': INT64, 'int': INT32, 'integer': INT32,
        'mediumint': INT24, 'smallint': INT16, 'tinyint': INT8,
        'bool': BOOL, 'boolean': BOOL, 'float': FLOAT,
        'double': DOUBLE, 'decimal': DECIMAL, 'dec': DECIMAL,
        'bit': BIT, 'varchar': STRING, 'char': CHAR
    }


    @staticmethod
    def from_string(string: str):
        args = string[string.find("(") + 1:string.rfind(")")] if string.find("(") >= 0 else None
        string = string[:string.find("(")].lower() if string.find("(") >= 0 else string

        key = Types.mapping.get(string, None)
        if key is None: return None
        if key == Types.BIT and string != "bit": return Types.BOOL
        if args is None or not isinstance(key, (DoubleSQLType, BitSQLType, StringSQLType)): return key
        return key(*[int(arg) for arg in args.split(",")])


# noinspection PyPep8Naming
class Where:
    def __init__(self, sql_string):
        self._sql = sql_string

    def sql(self) -> str:
        return f"WHERE {self._sql}"

    def AND(self, other):
        if not isinstance(other, Where): raise TypeError(f"Unsupported type \"{type(other)}\"")
        return Where(f"({self._sql} AND {other._sql})")

    def OR(self, other):
        if not isinstance(other, Where): raise TypeError(f"Unsupported type \"{type(other)}\"")
        return Where(f"({self._sql} OR {other._sql})")

    def NOT(self):
        return Where(f"NOT {self._sql}")

    def __repr__(self):
        return self.sql()

    def __and__(self, other):
        if isinstance(other, Where):
            return self.AND(other)

        raise TypeError(f"unsupported operand type(s) for &: \"{type(self)}\" and \"{type(other)}\"")

    def __or__(self, other):
        if isinstance(other, Where):
            return self.OR(other)

        raise TypeError(f"unsupported operand type(s) for |: \"{type(self)}\" and \"{type(other)}\"")

    def __invert__(self):
        return self.NOT()


class WhereIsEqual(Where):
    def __init__(self, column, value):
        self.value = value
        self.column = column
        super().__init__(f"{column.name} = {column.parse(value)}")

    def __invert__(self):
        return WhereIsNotEqual(self.column, self.value)


class WhereIsNotEqual(Where):
    def __init__(self, column, value):
        self.value = value
        self.column = column
        super().__init__(f"{column.name} <> {column.parse(value)}")

    def __invert__(self):
        return WhereIsEqual(self.column, self.value)


class WhereIsGreater(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} > {column.parse(value)}")


class WhereIsGreaterEqual(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} >= {column.parse(value)}")


class WhereIsLesser(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} < {column.parse(value)}")


class WhereIsLesserEqual(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} <= {column.parse(value)}")


class WhereIsLike(Where):
    def __init__(self, column, value):
        super().__init__(f"{column.name} LIKE {column.parse(value).replace("%", "%%")}")


class WhereIsIn(Where):
    def __init__(self, column, values):
        values = (column.parse(value) for value in values)
        super().__init__(f"{column.name} IN {values}")


class WhereIsBetween(Where):
    def __init__(self, column, a, b):
        values = (column.parse(a), column.parse(b))
        super().__init__(f"{column.name} = {values}")
