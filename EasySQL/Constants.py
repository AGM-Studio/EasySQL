from typing import TYPE_CHECKING, Any, List, Dict, Callable, Iterable


if TYPE_CHECKING:
    from .Classes import EasyColumn

__all__ = [
    "PRIMARY", "NOT_NULL", "AUTO_INCREMENT", "UNIQUE", "Unique",
    "SQLConstraints", "Charsets", "Charset", "Types", "SQLType"
]


# Constraints -------------------------------------------------------------------------------------------------------- #
class SQLConstraints:
    def __init__(self, value):
        self.value = value
        self.column_constraint = True


PRIMARY = SQLConstraints("PRIMARY KEY")
NOT_NULL = SQLConstraints("NOT NULL")
AUTO_INCREMENT = SQLConstraints("AUTO_INCREMENT")
UNIQUE = SQLConstraints("UNIQUE")


class Unique(SQLConstraints):
    """
    Constraint representing unique
    """

    def __init__(self, *columns: "EasyColumn", name: str = None):
        """
        Unique constractor

        :param columns: the column or columns for this constraint
        :param name: the name for this constraint
        """
        sql = f"UNIQUE ({", ".join([column.name for column in columns])})"
        super().__init__(f"CONSTRAINT {name} {sql}" if name is not None else sql)


# Charsets ----------------------------------------------------------------------------------------------------------- #
class Charset:
    def __init__(self, name, collation):
        self._name = name
        self._collation = collation

    def __repr__(self):
        return f"<CHARSET \"{self._name}\">"

    def __str__(self):
        return self._name

    @property
    def name(self):
        return self._name

    @property
    def collation(self):
        return self._collation


class Charsets:
    # CP
    CP850 = Charset("cp850", "cp850_general_ci")
    CP852 = Charset("cp852", "cp852_general_ci")
    CP866 = Charset("cp866", "cp866_general_ci")
    CP932 = Charset("cp932", "cp932_japanese_ci")
    CP1250 = Charset("cp1250", "cp1250_general_ci")
    CP1251 = Charset("cp1251", "cp1251_general_ci")
    CP1256 = Charset("cp1256", "cp1256_general_ci")
    CP1257 = Charset("cp1257", "cp1257_general_ci")
    # LATIN
    LATIN1 = Charset("latin1", "latin1_swedish_ci")
    LATIN2 = Charset("latin2", "latin2_general_ci")
    LATIN5 = Charset("latin5", "latin5_turkish_ci")
    LATIN7 = Charset("latin7", "latin7_general_ci")
    # UTF
    UTF8 = Charset("utf8", "utf8_general_ci")
    UTF8MB4 = Charset("utf8mb4", "utf8mb4_general_ci")
    UTF16 = Charset("utf16", "utf16_general_ci")
    UTF16LE = Charset("utf16le", "utf16le_general_ci")
    UTF32 = Charset("utf32", "utf32_general_ci")
    # Others
    ARMSCII8 = Charset("armscii8", "armscii8_general_ci")
    ASCII = Charset("ascii", "ascii_general_ci")
    BIG5 = Charset("big5", "big5_chinese_ci")
    BINARY = Charset("binary", "binary")
    DEC8 = Charset("dec8", "dec8_swedish_ci")
    EUCJPMS = Charset("eucjpms", "eucjpms_japanese_ci")
    EUCKR = Charset("euckr", "euckr_korean_ci")
    GB2312 = Charset("gb2312", "gb2312_chinese_ci")
    GBK = Charset("gbk", "gbk_chinese_ci")
    GEOSTD8 = Charset("geostd8", "geostd8_general_ci")
    GREEK = Charset("greek", "greek_general_ci")
    HEBREW = Charset("hebrew", "hebrew_general_ci")
    HP8 = Charset("hp8", "hp8_english_ci")
    KEYBCS2 = Charset("keybcs2", "keybcs2_general_ci")
    KOI8R = Charset("koi8r", "koi8r_general_ci")
    KOI8U = Charset("koi8u", "koi8u_general_ci")
    MACCE = Charset("macce", "macce_general_ci")
    MACROMAN = Charset("macroman", "macroman_general_ci")
    SJIS = Charset("sjis", "sjis_japanese_ci")
    SWE7 = Charset("swe7", "swe7_swedish_ci")
    TIS620 = Charset("tis620", "tis620_thai_ci")
    UCS2 = Charset("ucs2", "ucs2_general_ci")
    UJIS = Charset("ujis", "ujis_japanese_ci")


# Types -------------------------------------------------------------------------------------------------------- #
def _get_int_cast_(size, unsigned=False):
    minimum = 0 if unsigned else (-(2 ** (size - 1)))
    maximum = 2 ** size if unsigned else (2 ** (size - 1) - 1)

    def cast(value):
        if value is None:
            return None

        value = int(value)
        if not (minimum <= value <= maximum):
            raise ValueError(f'can only accept between {minimum} and {maximum}, but got {value}')

        return value

    return cast


def _float_cast(value):
    if value is None:
        return None

    return float(value)


def _string_cast(value):
    if value is None:
        return None

    return f"{value}"


def _string_parse(value):
    if value is None:
        return 'null'

    return f"'{value}'"


class SQLType:
    mapping: Dict[str, "SQLType"] = {}
    def __init__(self, name, *args, other_names: List[str] = None, caster: Callable[[Any], Any] = None, get_caster: Callable[["SQLType"], Callable[[Any], Any]] = None, default: Any = None, parser: Callable[[Any], str] = None, modifiable: bool = False, tags: Iterable[str] = None):
        self._name = name
        self._args = args
        self._tags = tags or ()

        self._modify_args = dict(caster=caster, get_caster=get_caster, default=default, parser=parser)

        if caster is None and get_caster is not None:
            caster = get_caster(self)

        if caster is None:
            raise NotImplementedError('cast method is not implemented')

        try:
            caster(default)
        except Exception:
            raise NotImplementedError('cast method is not implemented correctly')

        self._caster = caster
        self._default = default

        self._parser = parser if parser is not None else lambda value: 'null' if value is None else str(value)
        self._modifiable = modifiable

        SQLType.mapping[name.lower()] = self
        for name in other_names or []:
            SQLType.mapping[name.lower()] = self

    def __call__(self, *args):
        if self._modifiable or not args:
            return SQLType(self._name, *args, **self._modify_args, modifiable=self._modifiable)

        from EasySQL import SQLTypeException
        raise SQLTypeException('this sql type is not accepting new arguments')

    def __eq__(self, other):
        try:
            return other.name == self.name and other.args == self.args
        except Exception:
            return False

    def __hash__(self):
        return hash((self.name, self.args))

    def __repr__(self):
        return f'<SQLTYPE "{self.name}">'

    @property
    def name(self):
        return f'{self._name}({",".join([str(arg) for arg in self._args])})' if self._args else self._name

    @property
    def tags(self):
        return self._tags

    def cast(self, value):
        return self._caster(value)

    def parse(self, value):
        return self._parser(self.cast(value))

    @property
    def default(self):
        return self._default

    @property
    def args(self):
        return self._args

    @property
    def modifiable(self):
        return self._modifiable


class IntegerSQLType(SQLType):
    def __init__(self, name, bit_size, default: Any = None, unsigned: bool = False, other_names: List[str] = None):
        super().__init__(name, other_names=other_names, caster=_get_int_cast_(bit_size), default=default)

        self.bit_size = bit_size
        if unsigned:
            self._unsigned = SQLType(name, caster=_get_int_cast_(bit_size, True), default=default, tags=['UNSIGNED'])
        else:
            self._unsigned = None

    # noinspection PyPep8Naming
    @property
    def UNSIGNED(self) -> SQLType:
        if self._unsigned:
            return self._unsigned

        raise TypeError(f'{self.name} can not support unsigned')


class Types:
    INT64 = BIGINT = IntegerSQLType('BIGINT', 64, 0, True)
    INT32 = INT = INTEGER = IntegerSQLType('INT', 32, 0, True, ['integer'])
    INT24 = MEDIUMINT = IntegerSQLType('MEDIUMINT', 24, 0, True)
    INT16 = SMALLINT = IntegerSQLType('SMALLINT', 16, 0, True)
    INT8 = TINYINT = IntegerSQLType('TINYINT', 8, 0, False)

    BIT = SQLType(
        'BIT', 1, default=0, modifiable=True,
        get_caster=lambda self: _get_int_cast_(self.args[0])
    )
    BOOL = SQLType(
        'BIT', 1, default=False, other_names=['bool', 'boolean'],
        caster=lambda value: None if value is None else True if value else False,
        parser=lambda value: '1' if value else '0'
    )

    FLOAT = SQLType('FLOAT', caster=_float_cast, default=0.0)
    DOUBLE = SQLType('DOUBLE', 12, 6, caster=_float_cast, default=0.0, modifiable=True)
    DEC = DECIMAL = SQLType('DECIMAL', 12, 6, caster=_float_cast, default=0.0, modifiable=True, other_names=['dec'])

    STRING = VARCHAR = SQLType('VARCHAR', 255, caster=_string_cast, default='', parser=_string_parse, modifiable=True)
    CHAR = SQLType('CHAR', 255, caster=_string_cast, default='', parser=_string_parse, modifiable=True)


    @staticmethod
    def from_string(string: str):
        args = string[string.find('(') + 1:string.rfind(')')] if string.find('(') >= 0 else None
        string = string[:string.find('(')].lower() if string.find('(') >= 0 else string

        key = SQLType.mapping.get(string)
        if key is None: return None
        if key == Types.BIT and string != 'bit': return Types.BOOL
        if not key.modifiable or args is None: return key
        return key(*[int(arg) for arg in args.split(',')])
