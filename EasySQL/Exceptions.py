class DatabaseError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f'<{self.__class__.__name__} "{self.message}">'

    def __str__(self):
        return self.message

class DatabaseConnectionError(DatabaseError):
    """Error raised when the database is accessed while is not connected."""
class DatabaseSafetyException(DatabaseError):
    """Error raised when an insecure action is attempted."""
class SQLTypeException(DatabaseError):
    """Error related to the SQL types."""
