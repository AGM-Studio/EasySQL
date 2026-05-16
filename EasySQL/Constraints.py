from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .Classes import EasyColumn

__all__ = ['PRIMARY', 'NOT_NULL', 'AUTO_INCREMENT', 'UNIQUE', 'Unique', 'SQLConstraints']


class SQLConstraints:
    def __init__(self, value):
        self.value = value
        self.column_constraint = True


PRIMARY = SQLConstraints('PRIMARY KEY')
NOT_NULL = SQLConstraints('NOT NULL')
AUTO_INCREMENT = SQLConstraints('AUTO_INCREMENT')
UNIQUE = SQLConstraints('UNIQUE')


class Unique(SQLConstraints):
    """
    Constraint representing unique
    """

    def __init__(self, *columns: 'EasyColumn', name: str = None):
        """
        Unique constractor

        :param columns: the column or columns for this constraint
        :param name: the name for this constraint
        """
        sql = f'UNIQUE ({", ".join([column.name for column in columns])})'
        super().__init__(f"CONSTRAINT {name} {sql}" if name is not None else sql)
