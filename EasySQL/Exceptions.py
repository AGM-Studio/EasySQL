class MissingArgumentException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<MissingArgumentException "{self.message}">'

    def __str__(self):
        return self.message


class MisMatchException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<MisMatchException "{self.message}">'

    def __str__(self):
        return self.message


class DatabaseSafetyException(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<DatabaseSafetyException "{self.message}">'

    def __str__(self):
        return self.message
