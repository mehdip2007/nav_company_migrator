from sqlalchemy import inspect, text, MetaData
from exceptions import NoChunkingKeyError


class SchemaManager:
    def __init__(self, live_engine, archive_engine, company_name):
        self.live_engine = live_engine
        self.archive_engine = archive_engine
        self.company_name = company_name
        self.prefix = f"[{company_name}$" if company_name else None
        # FIX #12: Resolve company name eagerly in __init__ so get_* methods stay pure
        self._resolve_company_name()

    def _resolve_company_name(self):
        """
        FIX #12: Moved company name resolution out of get_company_tables()
        so that getter methods have no hidden side effects.
        """
        if not self.company_name:
            with self.live_engine.connect() as conn:
                stmt = text("SELECT TOP 1 [Name] FROM [dbo].[Company]")
                result = conn.execute(stmt).fetchone()
                if result:
                    self.company_name = result[0]
                    self.prefix = f"[{self.company_name}$"
                else:
                    raise Exception("No companies found in Live DB.")

    def get_company_tables(self):
        """
        Fetch all tables belonging to the company from Live DB.
        FIX #4 + #12: Uses bound parameters. No side effects — company name
        is already resolved in __init__ via _resolve_company_name().
        """
        with self.live_engine.connect() as conn:
            # FIX #4: Use bound parameter instead of f-string interpolation
            prefix_search = f"{self.company_name}$%"
            stmt = text("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME LIKE :prefix
                AND TABLE_TYPE = 'BASE TABLE'
            """)
            results = conn.execute(stmt, {"prefix": prefix_search}).fetchall()
            return [row[0] for row in results]

    def sync_table_schema(self, table_name):
        """
        Create table in Archive if missing, preserving PKs/Constraints.
        FIX #4: Uses bound parameter for the EXISTS check.
        """
        with self.live_engine.connect() as live_conn:
            with self.archive_engine.connect() as archive_conn:
                # FIX #4: Parameterized query — no f-string for user-derived values
                exists = archive_conn.execute(text("""
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = :tname
                """), {"tname": table_name}).fetchone()

                if not exists:
                    metadata = MetaData()
                    metadata.reflect(bind=self.live_engine, only=[table_name])
                    table = metadata.tables[table_name]
                    table.create(bind=archive_conn)
                    archive_conn.commit()
                    print(f"Created table: {table_name}")
                else:
                    print(f"Table exists: {table_name}")

    def disable_constraints(self, table_name, engine):
        # FIX #4: Sanitize table name before interpolation
        safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] NOCHECK CONSTRAINT ALL"))
            conn.commit()

    def enable_constraints(self, table_name, engine):
        # FIX #4: Sanitize table name before interpolation
        safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] CHECK CONSTRAINT ALL"))
            conn.commit()

    def get_chunking_column(self, table_name):
        """
        Determine the best column for chunking/resume.
        Priority: PK (single, Int/datetime) -> Identity -> Common NAV Columns -> Error.

        FIX #2: Composite PKs no longer silently fall back to the first column.
        A composite PK with no usable single-column key now raises NoChunkingKeyError
        to prevent pagination on a non-unique column causing missed or duplicated rows.

        FIX #4: All queries use bound parameters.
        """
        with self.live_engine.connect() as conn:
            # 1. Check for Primary Key
            stmt = text("""
                SELECT kcu.COLUMN_NAME, c.DATA_TYPE 
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                JOIN INFORMATION_SCHEMA.COLUMNS c 
                    ON kcu.COLUMN_NAME = c.COLUMN_NAME
                    AND kcu.TABLE_NAME = c.TABLE_NAME
                WHERE tc.TABLE_NAME = :tname 
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION
            """)
            pk_cols = conn.execute(stmt, {"tname": table_name}).fetchall()

            if pk_cols:
                if len(pk_cols) == 1:
                    col_name, dtype = pk_cols[0]
                    if dtype in ['int', 'bigint', 'datetime', 'datetime2', 'uniqueidentifier']:
                        return col_name
                    # Single PK but not a clean orderable type — still usable cautiously
                    # (e.g. varchar PKs in NAV like "No_")
                    return col_name

                # FIX #2: Composite PK detected — do NOT silently use first column.
                # Instead, check if any single column in the composite key is also
                # an identity column, which would make it safe to use alone.
                #
                # FIX (pyodbc TVP bug): SQL Server via pyodbc cannot handle a Python tuple
                # as a bound parameter for IN clauses — it tries to interpret it as a
                # Table-Valued Parameter and throws error 2715 or "incorrect syntax near @P2".
                # Solution: fetch ALL identity columns for the table without an IN filter,
                # then intersect with pk_col_names in Python. Cleaner and avoids the issue.
                pk_col_names = [row[0] for row in pk_cols]
                stmt_id = text("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = :tname 
                    AND COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
                """)
                all_identity_cols = {
                    row[0] for row in conn.execute(stmt_id, {"tname": table_name}).fetchall()
                }
                # Find the first PK column that is also an identity column
                identity_in_pk = next(
                    (col for col in pk_col_names if col in all_identity_cols), None
                )

                if identity_in_pk:
                    return identity_in_pk

                # Composite PK with no identity column — unsafe to chunk on any single column
                raise NoChunkingKeyError(
                    f"Table '{table_name}' has a composite PK ({', '.join(pk_col_names)}) "
                    f"with no identity column. Cannot determine a safe single-column chunking key. "
                    f"Skipping to prevent missed or duplicated rows.",
                    table_name=table_name
                )

            # 2. Check for Identity column (no PK case)
            stmt_id = text("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = :tname 
                AND COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
            """)
            identity = conn.execute(stmt_id, {"tname": table_name}).fetchone()
            if identity:
                return identity[0]

            # 3. Common NAV/BC Columns as fallback
            # FIX (pyodbc TVP bug): Same issue — tuple in IN clause breaks pyodbc.
            # Fetch ALL column names for the table and intersect in Python instead.
            common_cols = ["Entry No_", "No_", "ID", "$systemId"]
            stmt_col = text("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = :tname
            """)
            all_cols = {
                row[0] for row in conn.execute(stmt_col, {"tname": table_name}).fetchall()
            }
            # Prioritize by order defined in common_cols
            for col in common_cols:
                if col in all_cols:
                    return col

            raise NoChunkingKeyError(
                f"No suitable chunking key found for table '{table_name}'. "
                f"No usable PK, identity, or known NAV column was detected.",
                table_name=table_name
            )

    def get_pk_columns(self, table_name):
        """
        Return all PK column names for a table in ordinal order.
        Used by the offset-based migrator as the ORDER BY clause for composite PK tables.
        Returns an empty list if no PK is defined.
        """
        with self.live_engine.connect() as conn:
            stmt = text("""
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.TABLE_NAME = :tname
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION
            """)
            rows = conn.execute(stmt, {"tname": table_name}).fetchall()
            return [row[0] for row in rows]

    def get_insert_columns(self, table_name):
        """
        Get column names excluding timestamp/rowversion.
        FIX #4: Uses bound parameter.
        """
        with self.live_engine.connect() as conn:
            stmt = text("""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = :tname
            """)
            cols = conn.execute(stmt, {"tname": table_name}).fetchall()
            # Exclude timestamp/rowversion — cannot be inserted explicitly
            valid_cols = [c[0] for c in cols if c[1].upper() != 'TIMESTAMP']
            return valid_cols