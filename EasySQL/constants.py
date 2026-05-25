from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from .classes import SQLColumnExpr


T = TypeVar("T")
__all__ = [
    "PRIMARY", "NOT_NULL", "AUTO_INCREMENT", "UNIQUE", "Unique", "SQLConstraints", "Charsets", "Charset"
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

    def __init__(self, *columns: "SQLColumnExpr", name: str = None):
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
