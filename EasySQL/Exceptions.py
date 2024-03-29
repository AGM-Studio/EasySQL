class DatabaseConnectionException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<DatabaseSafetyException "{self.message}">'

    def __str__(self):
        return self.message


class DatabaseSafetyException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<DatabaseSafetyException "{self.message}">'

    def __str__(self):
        return self.message


class SQLTypeException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<SQLTypeException "{self.message}">'

    def __str__(self):
        return self.message
