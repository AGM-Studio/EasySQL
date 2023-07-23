from .ABC import SQLType


def _get_int_cast_(size, unsigned=False):
    minimum = (-(2 ** (size - 1))) if unsigned else 0
    maximum = (2 ** (size - 1) - 1) if unsigned else 2 ** size

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


INT64 = BIGINT = SQLType('BIGINT', caster=_get_int_cast_(64), default=0)
INT32 = INT = INTEGER = SQLType('INT', caster=_get_int_cast_(32), default=0)
INT24 = MEDIUMINT = SQLType('MEDIUMINT', caster=_get_int_cast_(24), default=0)
INT16 = SMALLINT = SQLType('SMALLINT', caster=_get_int_cast_(16), default=0)
INT8 = TINYINT = SQLType('TINYINT', caster=_get_int_cast_(8), default=0)

BIGINT.UNSIGNED = SQLType('BIGINT', caster=_get_int_cast_(64, True), default=0)
INTEGER.UNSIGNED = SQLType('BIGINT', caster=_get_int_cast_(32, True), default=0)
MEDIUMINT.UNSIGNED = SQLType('BIGINT', caster=_get_int_cast_(24, True), default=0)
SMALLINT.UNSIGNED = SQLType('BIGINT', caster=_get_int_cast_(16, True), default=0)

BIT = SQLType('BIT', 1, get_caster=lambda self: _get_int_cast_(self.args[0]), default=0, modifiable=True)
BOOL = SQLType('BIT', 1, caster=lambda value: None if value is None else True if value else False, default=False, parser=lambda value: '1' if value else '0')

FLOAT = SQLType('FLOAT', caster=_float_cast, default=0.0)
DOUBLE = SQLType('DOUBLE', 12, 6, caster=_float_cast, default=0.0, modifiable=True)
DEC = DECIMAL = SQLType('DECIMAL', 12, 6, caster=_float_cast, default=0.0, modifiable=True)

STRING = VARCHAR = SQLType('VARCHAR', 255, caster=_string_cast, default='', parser=_string_parse, modifiable=True)
CHAR = SQLType('CHAR', 255, caster=_string_cast, default='', parser=_string_parse, modifiable=True)

type_dict = {
    INT64: ['bigint'],
    INT32: ['int', 'integer'],
    INT24: ['mediumint'],
    INT16: ['smallint'],
    INT8: ['tinyint'],
    BIT: ['bit', 'bool', 'boolean'],
    FLOAT: ['float'],
    DOUBLE: ['double'],
    DEC: ['decimal', 'dec'],
    STRING: ['varchar'],
    CHAR: ['char']
}


def string_to_type(string: str):
    args = string[string.find('(') + 1:string.rfind(')')] if string.find('(') >= 0 else None
    string = string[:string.find('(')].lower() if string.find('(') >= 0 else string

    for key, value in type_dict.items():
        if string in value:
            if key == BIT and string != 'bit':
                return BOOL
            if not key.modifiable or args is None:
                return key
            return key(*[int(arg) for arg in args.split(',')])
    return None
