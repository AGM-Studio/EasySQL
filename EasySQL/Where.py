from .ABC import SQLCommand


class Where(SQLCommand):
    def __init__(self, sql_string):
        self.value = sql_string

    def get_value(self) -> str:
        return f'WHERE {self.value}'

    def AND(self, other: 'Where'):
        return Where(f'({self.value} AND {other.value})')

    def OR(self, other: 'Where'):
        return Where(f'({self.value} OR {other.value})')

    def NOT(self):
        return Where(f'NOT {self.value}')

    def __and__(self, other):
        if isinstance(other, Where):
            return self.AND(other)

        raise TypeError(f"unsupported operand type(s) for &: '{type(self)}' and '{type(other)}'")

    def __or__(self, other):
        if isinstance(other, Where):
            return self.OR(other)

        raise TypeError(f"unsupported operand type(s) for |: '{type(self)}' and '{type(other)}'")

    def __invert__(self):
        return self.NOT()


class WhereIsEqual(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} = {column.parse(value)}')


class WhereIsNotEqual(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} <> {column.parse(value)}')


class WhereIsGreater(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} > {column.parse(value)}')


class WhereIsGreaterEqual(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} => {column.parse(value)}')


class WhereIsLesser(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} < {column.parse(value)}')


class WhereIsLesserEqual(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} =< {column.parse(value)}')


class WhereIsLike(Where):
    def __init__(self, column, value):
        super().__init__(f'{column.name} LIKE {column.parse(value)}')


class WhereIsIn(Where):
    def __init__(self, column, values):
        values = (column.parse(value) for value in values)
        super().__init__(f'{column.name} IN {values}')


class WhereIsBetween(Where):
    def __init__(self, column, a, b):
        values = (column.parse(a), column.parse(b))
        super().__init__(f'{column.name} = {values}')


__all__ = ['Where', 'WhereIsEqual', 'WhereIsNotEqual', 'WhereIsGreater', 'WhereIsLesser',
           'WhereIsGreaterEqual', 'WhereIsLesserEqual', 'WhereIsLike', 'WhereIsIn', 'WhereIsBetween']