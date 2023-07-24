from typing import TYPE_CHECKING

from .ABC import SQLConstraints

if TYPE_CHECKING:
    from .Classes import EasyColumn

__all__ = ['PRIMARY', 'NOT_NULL', 'AUTO_INCREMENT', 'UNIQUE', 'Unique']

PRIMARY = SQLConstraints('PRIMARY KEY')
NOT_NULL = SQLConstraints('NOT NULL')
AUTO_INCREMENT = SQLConstraints('AUTO_INCREMENT')
UNIQUE = SQLConstraints('UNIQUE')


class Unique(SQLConstraints):
    """
    Constraint representing unique
    """

    def __init__(self, *columns: EasyColumn, name: str = None):
        """
        Unique constractor

        :param columns: the column or columns for this constraint
        :param name: the name for this constraint
        """
        if name is None:
            super().__init__(f'UNIQUE ({", ".join([column.name for column in columns])})')
        else:
            super().__init__(f'CONSTRAINT {name} UNIQUE ({", ".join([column.name for column in columns])})')
