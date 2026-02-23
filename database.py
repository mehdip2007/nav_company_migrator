import urllib.parse
from sqlalchemy import create_engine, text
from exceptions import DatabaseConnectionError

class DatabaseConnector:
    def __init__(self, server, port, database, user, password, driver, trust):
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver
        self.trust = trust
        
        self.engine = self._create_engine()

    def _create_engine(self):
        try:
            odbc_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.user};PWD={self.password};"
                f"Encrypt=yes;TrustServerCertificate={self.trust};"
                f"Connection Timeout=30;"
                f"MARS_Connection=Yes;"
            )
            odbc_connect = urllib.parse.quote_plus(odbc_str)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_connect}")
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to {self.database}: {str(e)}")

    def get_engine(self):
        return self.engine
    
    def get_database_name(self):
        return self.database
