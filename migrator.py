from sqlalchemy import text
from tqdm import tqdm
from state_manager import StateManager
from exceptions import MigrationError, MigrationBatchError
from datetime import datetime, date
import time


class MigrationStats:
    """Tracks statistics for a single table migration."""
    def __init__(self, table_name):
        self.table_name = table_name
        self.total_source_rows = 0
        self.rows_migrated = 0
        self.rows_skipped = 0
        self.start_time = 0
        self.end_time = 0
        self.errors = []
        self.mode = "key"  # "key" or "offset" — shown in summary

    @property
    def duration_seconds(self):
        return self.end_time - self.start_time if self.end_time and self.start_time else 0

    @property
    def rows_per_second(self):
        if self.duration_seconds > 0:
            return self.rows_migrated / self.duration_seconds
        return 0


class DataMigrator:
    def __init__(self, live_engine, archive_engine, live_db_name, archive_db_name, batch_size, logger):
        self.live_engine = live_engine
        self.archive_engine = archive_engine
        self.live_db = live_db_name
        self.archive_db = archive_db_name
        self.batch_size = batch_size
        self.state_manager = StateManager(archive_engine)
        self.logger = logger
        self.completed_stats = []

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def get_source_count(self, table_name):
        """Get total rows in source table (with NOLOCK for speed)."""
        try:
            with self.live_engine.connect() as conn:
                safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)")
                ).fetchone()
                return result[0] if result else 0
        except Exception as e:
            self.logger.warning(f"Could not count rows for {table_name}: {e}")
            return 0

    def _has_identity_column(self, table_name):
        """
        FIX: IDENTITY_INSERT error.
        Check whether the ARCHIVE copy of the table has an identity column.
        If yes, we must SET IDENTITY_INSERT ON before inserting explicit values.
        """
        safe_table = table_name.replace("'", "''")
        try:
            with self.archive_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = :tname
                    AND COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
                """), {"tname": safe_table}).fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            self.logger.warning(f"Could not check identity column for {table_name}: {e}")
            return False

    def _resolve_start_key(self, table_name, chunking_key, state_key):
        """
        Determine the correct starting key for key-based migration.
        Priority: StateManager checkpoint -> MAX key in archive -> None (fresh start).
        Self-healing: works even if ETL_State is wiped.
        """
        if state_key is not None:
            return state_key

        try:
            with self.archive_engine.connect() as conn:
                safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
                safe_key = chunking_key.replace("[", "").replace("]", "").replace("'", "''")
                existing = conn.execute(
                    text(f"SELECT MAX([{safe_key}]) FROM [{self.archive_db}].[dbo].[{safe_table}]")
                ).fetchone()[0]
            if existing is not None:
                self.logger.info(
                    f"No checkpoint for {table_name}, archive has data. Resuming from MAX key: {existing}"
                )
                return existing
        except Exception as e:
            self.logger.warning(f"Could not determine archive MAX key for {table_name}: {e}")

        return None

    def _build_where_clause(self, chunking_key, current_key):
        """
        FIX: Datetime/date quoting.
        Build WHERE clause for key-based pagination.
        Strings, datetime, and date objects all need to be single-quoted in SQL.
        Only numeric types (int, float, Decimal) should be unquoted.
        """
        safe_key = chunking_key.replace("[", "").replace("]", "").replace("'", "''")

        if current_key is None:
            return "1=1"

        # FIX: previously only str was quoted — datetime objects came back from pyodbc
        # as Python datetime.datetime, not str, so they fell into the unquoted branch
        # producing: WHERE [Date] > 2027-01-01 00:00:00  (invalid SQL)
        if isinstance(current_key, (str, datetime, date)):
            escaped = str(current_key).replace("'", "''")
            return f"[{safe_key}] > '{escaped}'"
        else:
            # Numeric types (int, float, Decimal) — no quotes needed
            return f"[{safe_key}] > {current_key}"

    def _execute_batch_with_identity(self, sql, safe_table, has_identity):
        """
        FIX: IDENTITY_INSERT error.
        Execute an INSERT statement, wrapping with IDENTITY_INSERT ON/OFF
        when the target table has an identity column.
        SET IDENTITY_INSERT must be in the same connection as the INSERT.
        """
        with self.archive_engine.connect() as conn:
            if has_identity:
                conn.execute(text(f"SET IDENTITY_INSERT [dbo].[{safe_table}] ON"))
            result = conn.execute(text(sql))
            if has_identity:
                conn.execute(text(f"SET IDENTITY_INSERT [dbo].[{safe_table}] OFF"))
            conn.commit()
            return result.rowcount

    # -------------------------------------------------------------------------
    # Key-based migration (single/identity PK tables)
    # -------------------------------------------------------------------------

    def migrate_table(self, table_name, chunking_key, columns):
        """
        Key-based migration with resume capability.
        Used for tables with a single usable PK, identity column, or known NAV key.
        """
        stats = MigrationStats(table_name)
        stats.start_time = time.time()
        stats.mode = "key"

        safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
        safe_key = chunking_key.replace("[", "").replace("]", "").replace("'", "''")
        safe_columns = [c.replace("[", "").replace("]", "").replace("'", "''") for c in columns]

        self.logger.info(f"Starting migration for table: {table_name}")

        # Check IDENTITY_INSERT requirement upfront (FIX)
        has_identity = self._has_identity_column(table_name)
        if has_identity:
            self.logger.info(f"Identity column detected on {table_name} — IDENTITY_INSERT will be enabled per batch.")

        # 1. Get Max Key from Live
        try:
            with self.live_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX([{safe_key}]) FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)")
                ).fetchone()
                max_key = result[0] if result and result[0] is not None else None
        except Exception as e:
            err = MigrationError(f"Failed to get Max Key: {str(e)}", table_name)
            self.logger.error(str(err))
            stats.errors.append(str(err))
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        if max_key is None:
            self.logger.info(f"Table {table_name} is empty. Skipping.")
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        # 2. Get Total Source Count
        stats.total_source_rows = self.get_source_count(table_name)

        # 3. Resolve starting key
        state_key = self.state_manager.get_checkpoint(table_name)
        current_key = self._resolve_start_key(table_name, safe_key, state_key)

        # 4. Disable Constraints
        constraints_disabled = False
        try:
            with self.archive_engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] NOCHECK CONSTRAINT ALL"))
                conn.commit()
                constraints_disabled = True
        except Exception as e:
            err = MigrationError(f"Failed to disable constraints: {str(e)}", table_name)
            self.logger.error(str(err))
            stats.errors.append(str(err))
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        try:
            col_list = ", ".join([f"[{c}]" for c in safe_columns])
            pbar = tqdm(
                total=stats.total_source_rows,
                desc=f"Migrating {table_name}",
                unit="rows",
                initial=0
            )

            while True:
                where_clause = self._build_where_clause(safe_key, current_key)
                order_clause = f"[{safe_key}]"

                sql = f"""
                    INSERT INTO [{self.archive_db}].[dbo].[{safe_table}] ({col_list})
                    SELECT TOP {self.batch_size} {col_list}
                    FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)
                    WHERE {where_clause}
                    ORDER BY {order_clause}
                """

                try:
                    rows_affected = self._execute_batch_with_identity(sql, safe_table, has_identity)

                    if rows_affected == 0:
                        break

                    stats.rows_migrated += rows_affected
                    pbar.update(rows_affected)

                    # Update checkpoint from archive MAX
                    with self.archive_engine.connect() as conn:
                        new_max = conn.execute(
                            text(f"SELECT MAX([{safe_key}]) FROM [{self.archive_db}].[dbo].[{safe_table}]")
                        ).fetchone()[0]
                        current_key = new_max
                        self.state_manager.update_checkpoint(table_name, current_key, "Running")

                except Exception as e:
                    raise MigrationBatchError(
                        message=f"Batch insert failed: {str(e)}",
                        table_name=table_name,
                        current_key=current_key
                    )

            pbar.close()

        except MigrationBatchError as e:
            self.logger.error(str(e))
            stats.errors.append(str(e))

        except Exception as e:
            err = MigrationError(f"Unexpected error in migration loop: {str(e)}", table_name)
            self.logger.critical(str(err))
            stats.errors.append(str(err))

        finally:
            if constraints_disabled:
                try:
                    with self.archive_engine.connect() as conn:
                        conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] CHECK CONSTRAINT ALL"))
                        conn.commit()
                    self.logger.info(f"Constraints re-enabled for {table_name}")
                except Exception as e:
                    self.logger.error(f"Failed to enable constraints for {table_name}: {e}")

            stats.end_time = time.time()

            final_status = "Completed" if len(stats.errors) == 0 else "Failed"
            try:
                self.state_manager.update_checkpoint(table_name, current_key, final_status)
            except Exception as e:
                self.logger.warning(f"Could not write final status for {table_name}: {e}")

            self.completed_stats.append(stats)

            if len(stats.errors) == 0:
                self.logger.info(
                    f"Completed {table_name}. "
                    f"Migrated: {stats.rows_migrated}, "
                    f"Duration: {stats.duration_seconds:.2f}s"
                )
            else:
                self.logger.warning(
                    f"Finished {table_name} with errors. Migrated: {stats.rows_migrated}"
                )

    # -------------------------------------------------------------------------
    # Offset-based migration (composite PK tables)
    # -------------------------------------------------------------------------

    def migrate_table_by_offset(self, table_name, pk_cols, columns):
        """
        Offset-based migration for composite PK tables with no usable single chunking key.

        FIX: Duplicate key violation on resume.
        Uses INSERT ... SELECT ... WHERE NOT EXISTS (built from pk_cols) so that
        rows already in the archive are safely skipped rather than causing a PK violation.
        Offset still advances by batch_size each iteration — NOT EXISTS handles the
        overlap, and termination is driven by current_offset >= total_source_rows.
        """
        stats = MigrationStats(table_name)
        stats.start_time = time.time()
        stats.mode = "offset"

        safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
        safe_pk_cols = [c.replace("[", "").replace("]", "").replace("'", "''") for c in pk_cols]
        safe_columns = [c.replace("[", "").replace("]", "").replace("'", "''") for c in columns]

        self.logger.info(f"[OFFSET MODE] Starting migration for table: {table_name} | PK: {pk_cols}")

        # Check IDENTITY_INSERT requirement (FIX — offset tables can also have identity cols)
        has_identity = self._has_identity_column(table_name)
        if has_identity:
            self.logger.info(f"Identity column detected on {table_name} — IDENTITY_INSERT will be enabled per batch.")

        # 1. Get total source count (used for termination + tqdm)
        stats.total_source_rows = self.get_source_count(table_name)

        if stats.total_source_rows == 0:
            self.logger.info(f"Table {table_name} is empty. Skipping.")
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        # 2. Resolve starting offset
        checkpoint = self.state_manager.get_checkpoint(table_name)
        if checkpoint is not None:
            try:
                current_offset = int(checkpoint)
                self.logger.info(f"Resuming {table_name} from offset {current_offset}")
            except ValueError:
                # Checkpoint exists but is not an integer — previously ran in key mode
                current_offset = 0
        else:
            # No checkpoint — fast-forward using archive row count
            try:
                with self.archive_engine.connect() as conn:
                    archive_count = conn.execute(
                        text(f"SELECT COUNT(*) FROM [{self.archive_db}].[dbo].[{safe_table}]")
                    ).fetchone()[0]
                if archive_count > 0:
                    current_offset = archive_count
                    self.logger.info(
                        f"No checkpoint for {table_name}, archive has {archive_count} rows. "
                        f"Fast-forwarding offset to {current_offset}."
                    )
                else:
                    current_offset = 0
            except Exception as e:
                self.logger.warning(f"Could not read archive count for {table_name}: {e}. Starting from 0.")
                current_offset = 0

        # 3. Disable Constraints
        constraints_disabled = False
        try:
            with self.archive_engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] NOCHECK CONSTRAINT ALL"))
                conn.commit()
                constraints_disabled = True
        except Exception as e:
            err = MigrationError(f"Failed to disable constraints: {str(e)}", table_name)
            self.logger.error(str(err))
            stats.errors.append(str(err))
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        try:
            col_list = ", ".join([f"[{c}]" for c in safe_columns])
            order_clause = ", ".join([f"[{c}]" for c in safe_pk_cols])

            # FIX: Build NOT EXISTS clause using all PK columns to prevent duplicate inserts
            # This is safe to do in offset mode because we're already doing a full table scan —
            # the NOT EXISTS cross-db check adds minimal overhead relative to the OFFSET scan cost.
            pk_match_clause = " AND ".join(
                [f"t.[{c}] = batch.[{c}]" for c in safe_pk_cols]
            )

            pbar = tqdm(
                total=stats.total_source_rows,
                desc=f"[OFFSET] {table_name}",
                unit="rows",
                initial=current_offset
            )

            while True:
                # FIX: Wrap source batch in subquery alias "batch", apply NOT EXISTS
                # against archive using all PK columns. This prevents PK violations
                # even if offset and archive row count drift out of alignment on resume.
                sql = f"""
                    INSERT INTO [{self.archive_db}].[dbo].[{safe_table}] ({col_list})
                    SELECT {col_list} FROM (
                        SELECT {col_list}
                        FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)
                        ORDER BY {order_clause}
                        OFFSET {current_offset} ROWS FETCH NEXT {self.batch_size} ROWS ONLY
                    ) AS batch
                    WHERE NOT EXISTS (
                        SELECT 1 FROM [{self.archive_db}].[dbo].[{safe_table}] t
                        WHERE {pk_match_clause}
                    )
                """

                try:
                    rows_affected = self._execute_batch_with_identity(sql, safe_table, has_identity)

                    # Always advance offset by full batch_size — NOT EXISTS handles dedup,
                    # so offset tracks position in SOURCE, not in archive.
                    current_offset += self.batch_size
                    stats.rows_migrated += rows_affected
                    pbar.update(rows_affected)

                    self.state_manager.update_checkpoint(table_name, str(current_offset), "Running")

                    # Terminate once offset has moved past total source rows
                    if current_offset >= stats.total_source_rows:
                        break

                except Exception as e:
                    raise MigrationBatchError(
                        message=f"Offset batch insert failed: {str(e)}",
                        table_name=table_name,
                        current_key=current_offset
                    )

            pbar.close()

        except MigrationBatchError as e:
            self.logger.error(str(e))
            stats.errors.append(str(e))

        except Exception as e:
            err = MigrationError(f"Unexpected error in offset migration loop: {str(e)}", table_name)
            self.logger.critical(str(err))
            stats.errors.append(str(err))

        finally:
            if constraints_disabled:
                try:
                    with self.archive_engine.connect() as conn:
                        conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] CHECK CONSTRAINT ALL"))
                        conn.commit()
                    self.logger.info(f"Constraints re-enabled for {table_name}")
                except Exception as e:
                    self.logger.error(f"Failed to enable constraints for {table_name}: {e}")

            stats.end_time = time.time()

            final_status = "Completed" if len(stats.errors) == 0 else "Failed"
            try:
                self.state_manager.update_checkpoint(table_name, str(current_offset), final_status)
            except Exception as e:
                self.logger.warning(f"Could not write final status for {table_name}: {e}")

            self.completed_stats.append(stats)

            if len(stats.errors) == 0:
                self.logger.info(
                    f"[OFFSET MODE] Completed {table_name}. "
                    f"Migrated: {stats.rows_migrated}, "
                    f"Duration: {stats.duration_seconds:.2f}s"
                )
            else:
                self.logger.warning(
                    f"[OFFSET MODE] Finished {table_name} with errors. "
                    f"Migrated: {stats.rows_migrated}"
                )

    def get_summary(self):
        return self.completed_stats
