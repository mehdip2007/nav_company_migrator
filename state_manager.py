from sqlalchemy import text, MetaData, Table, Column, String, DateTime
from sqlalchemy.sql import select, insert, update
from datetime import datetime

class StateManager:
    def __init__(self, engine):
        self.engine = engine
        self.table_name = "ETL_State"
        self._ensure_state_table()

    def _ensure_state_table(self):
        """Create ETL_State table if it doesn't exist in the Archive DB."""
        with self.engine.connect() as conn:
            conn.execute(text(f"""
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{self.table_name}]') AND type in (N'U'))
                CREATE TABLE [dbo].[{self.table_name}] (
                    [TableName] NVARCHAR(255) PRIMARY KEY,
                    [LastProcessedKey] NVARCHAR(255),
                    [Status] NVARCHAR(50),
                    [UpdatedDate] DATETIME
                )
            """))
            conn.commit()

    def get_checkpoint(self, table_name):
        """Retrieve the last processed key for a table."""
        with self.engine.connect() as conn:
            stmt = text(f"SELECT [LastProcessedKey] FROM [dbo].[{self.table_name}] WHERE [TableName] = :tname")
            result = conn.execute(stmt, {"tname": table_name}).fetchone()
            return result[0] if result else None

    def update_checkpoint(self, table_name, last_key, status="Running"):
        """Upsert checkpoint data."""
        with self.engine.connect() as conn:
            # Check if exists
            check_stmt = text(f"SELECT 1 FROM [dbo].[{self.table_name}] WHERE [TableName] = :tname")
            exists = conn.execute(check_stmt, {"tname": table_name}).fetchone()
            
            if exists:
                stmt = text(f"""
                    UPDATE [dbo].[{self.table_name}] 
                    SET [LastProcessedKey] = :lkey, [Status] = :status, [UpdatedDate] = :udate 
                    WHERE [TableName] = :tname
                """)
            else:
                stmt = text(f"""
                    INSERT INTO [dbo].[{self.table_name}] ([TableName], [LastProcessedKey], [Status], [UpdatedDate])
                    VALUES (:tname, :lkey, :status, :udate)
                """)
            
            conn.execute(stmt, {
                "tname": table_name, 
                "lkey": str(last_key), 
                "status": status, 
                "udate": datetime.now()
            })
            conn.commit()
