from sqlalchemy import inspect, text, MetaData
from exceptions import NoChunkingKeyError

class SchemaManager:
    def __init__(self, live_engine, archive_engine, company_name):
        self.live_engine = live_engine
        self.archive_engine = archive_engine
        self.company_name = company_name
        self.prefix = f"[{company_name}$"

    def get_company_tables(self):
        """Fetch all tables belonging to the company from Live DB."""
        with self.live_engine.connect() as conn:
            # If company name is not provided, fetch the first one
            if not self.company_name:
                stmt = text("SELECT TOP 1 [Name] FROM [dbo].[Company]")
                result = conn.execute(stmt).fetchone()
                if result:
                    self.company_name = result[0]
                    self.prefix = f"[{self.company_name}$"
                else:
                    raise Exception("No companies found in Live DB.")
            
            # Get tables matching prefix
            stmt = text("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME LIKE :prefix + '%' 
                AND TABLE_TYPE = 'BASE TABLE'
            """)
            results = conn.execute(stmt, {"prefix": self.prefix.replace('[', '').replace(']', '')}).fetchall()
            return [row[0] for row in results]

    def sync_table_schema(self, table_name):
        """Create table in Archive if missing, preserving PKs/Constraints."""
        with self.live_engine.connect() as live_conn:
            with self.archive_engine.connect() as archive_conn:
                # Check if exists in archive
                exists = archive_conn.execute(text(f"""
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = '{table_name}'
                """)).fetchone()

                if not exists:
                    # Get Create Script from Live (Simplified approach using SELECT INTO then fix)
                    # Robust approach: Use SQLAlchemy Reflection + Create
                    metadata = MetaData()
                    metadata.reflect(bind=self.live_engine, only=[table_name])
                    table = metadata.tables[table_name]
                    table.create(bind=archive_conn)
                    archive_conn.commit()
                    print(f"Created table: {table_name}")
                else:
                    print(f"Table exists: {table_name}")

    def disable_constraints(self, table_name, engine):
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE [dbo].[{table_name}] NOCHECK CONSTRAINT ALL"))
            conn.commit()

    def enable_constraints(self, table_name, engine):
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE [dbo].[{table_name}] CHECK CONSTRAINT ALL"))
            conn.commit()

    def get_chunking_column(self, table_name):
        """
        Determine the best column for chunking/resume.
        Priority: PK (Int) -> Identity -> Common NAV Columns -> First PK Column.
        """
        with self.live_engine.connect() as conn:
            # 1. Check for Primary Key
            stmt = text("""
                SELECT kcu.COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.COLUMNS c ON kcu.COLUMN_NAME = c.COLUMN_NAME
                WHERE tc.TABLE_NAME = :tname AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION
            """)
            pk_cols = conn.execute(stmt, {"tname": table_name}).fetchall()
            
            if pk_cols:
                # If single PK and numeric/datetime, use it
                if len(pk_cols) == 1:
                    col_name, dtype = pk_cols[0]
                    if dtype in ['int', 'bigint', 'datetime', 'datetime2', 'uniqueidentifier']:
                        return col_name
                # Fallback to first PK column if composite
                return pk_cols[0][0]

            # 2. Check for Identity
            stmt_id = text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = :tname AND COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
            """)
            identity = conn.execute(stmt_id, {"tname": table_name}).fetchone()
            if identity:
                return identity[0]

            # 3. Common NAV Columns
            common_cols = ["Entry No_", "No_", "ID", "$systemID"]
            stmt_col = text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = :tname AND COLUMN_NAME IN :commons
            """)
            common = conn.execute(stmt_col, {"tname": table_name, "commons": tuple(common_cols)}).fetchall()
            if common:
                return common[0][0]

            raise NoChunkingKeyError(f"No suitable chunking key found for table {table_name}")

    def get_insert_columns(self, table_name):
        """Get column names excluding timestamp/rowversion."""
        with self.live_engine.connect() as conn:
            stmt = text("""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = :tname
            """)
            cols = conn.execute(stmt, {"tname": table_name}).fetchall()
            # Exclude timestamp/rowversion
            valid_cols = [c[0] for c in cols if c[1].upper() != 'TIMESTAMP']
            return valid_cols
