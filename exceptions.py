class MigrationError(Exception):
    """Base exception for all migration-related errors."""
    def __init__(self, message, table_name=None):
        super().__init__(message)
        self.table_name = table_name
        self.message = message

    def __str__(self):
        if self.table_name:
            return f"[{self.table_name}] {self.message}"
        return self.message

class NoChunkingKeyError(MigrationError):
    """Raised when a suitable column for chunking cannot be found."""
    def __init__(self, message, table_name=None):
        super().__init__(message, table_name)

class DatabaseConnectionError(MigrationError):
    """Raised when database connection fails."""
    def __init__(self, message, table_name=None):
        super().__init__(message, table_name)

class MigrationBatchError(MigrationError):
    """
    Raised when a specific batch fails during migration.
    Includes context about the processing key to aid debugging.
    """
    def __init__(self, message, table_name=None, current_key=None):
        super().__init__(message, table_name)
        self.current_key = current_key

    def __str__(self):
        context = f"Key={self.current_key}" if self.current_key else "Unknown Key"
        return f"[{self.table_name}] ({context}) {self.message}"
