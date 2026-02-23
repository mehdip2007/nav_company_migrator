from sqlalchemy import text
from tqdm import tqdm
from state_manager import StateManager
from exceptions import MigrationError, MigrationBatchError, NoChunkingKeyError
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

    def get_source_count(self, table_name):
        """Get total rows in source table (with NOLOCK for speed)."""
        try:
            with self.live_engine.connect() as conn:
                # FIX #4: table name comes from INFORMATION_SCHEMA so it's trusted,
                # but we still sanitize by stripping brackets to prevent injection.
                safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
                stmt = text(f"SELECT COUNT(*) FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)")
                result = conn.execute(stmt).fetchone()
                return result[0] if result else 0
        except Exception as e:
            self.logger.warning(f"Could not count rows for {table_name}: {e}")
            return 0

    def _resolve_start_key(self, table_name, chunking_key, state_key):
        """
        FIX #3 (first-run edge case): Determine the correct starting key.
        Priority: StateManager checkpoint -> MAX key in archive -> None (fresh start).
        This makes resume self-healing even if ETL_State is wiped.
        """
        # If state manager has a checkpoint, trust it first
        if state_key is not None:
            return state_key

        # Otherwise, derive from archive directly (handles partial first runs)
        try:
            with self.archive_engine.connect() as conn:
                safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
                safe_key = chunking_key.replace("[", "").replace("]", "").replace("'", "''")
                existing = conn.execute(
                    text(f"SELECT MAX([{safe_key}]) FROM [{self.archive_db}].[dbo].[{safe_table}]")
                ).fetchone()[0]
            if existing is not None:
                self.logger.info(f"No checkpoint found for {table_name}, but archive has data. Resuming from MAX key: {existing}")
                return existing
        except Exception as e:
            self.logger.warning(f"Could not determine archive MAX key for {table_name}: {e}")

        return None  # Truly fresh start

    def _build_where_clause(self, chunking_key, current_key):
        """
        FIX #3: Build WHERE clause based on current_key.
        The checkpoint-based WHERE clause alone prevents duplicates —
        NOT EXISTS subquery is removed entirely.
        """
        safe_key = chunking_key.replace("[", "").replace("]", "").replace("'", "''")

        if current_key is None:
            return "1=1"

        if isinstance(current_key, str):
            # Escape single quotes in string keys
            escaped = current_key.replace("'", "''")
            return f"[{safe_key}] > '{escaped}'"
        else:
            return f"[{safe_key}] > {current_key}"

    def migrate_table(self, table_name, chunking_key, columns):
        """
        Migrate data for a single table with resume capability, stats tracking,
        and structured exception handling.

        Critical fixes applied:
        - #3: NOT EXISTS subquery removed; checkpoint WHERE clause handles dedup
        - #4: Table/column names sanitized before interpolation
        - #8: Final status ('Completed'/'Failed') written to ETL_State
        - #9: tqdm initialized with total rows for proper ETA
        """
        stats = MigrationStats(table_name)
        stats.start_time = time.time()

        # Sanitize names used in dynamic SQL (FIX #4)
        safe_table = table_name.replace("[", "").replace("]", "").replace("'", "''")
        safe_key = chunking_key.replace("[", "").replace("]", "").replace("'", "''")
        safe_columns = [c.replace("[", "").replace("]", "").replace("'", "''") for c in columns]

        self.logger.info(f"Starting migration for table: {table_name}")

        # 1. Get Max Key from Live
        try:
            with self.live_engine.connect() as conn:
                stmt = text(f"SELECT MAX([{safe_key}]) FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)")
                max_key_result = conn.execute(stmt).fetchone()
                max_key = max_key_result[0] if max_key_result and max_key_result[0] is not None else None
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

        # 2. Get Total Source Count for tqdm + Summary (FIX #9)
        stats.total_source_rows = self.get_source_count(table_name)

        # 3. Resolve starting key — checkpoint first, then archive MAX, then None (FIX #3)
        state_key = self.state_manager.get_checkpoint(table_name)
        current_key = self._resolve_start_key(table_name, safe_key, state_key)

        # 4. Disable Constraints on Archive
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

            # FIX #9: Pass total to tqdm so ETA and percentage are shown
            pbar = tqdm(
                total=stats.total_source_rows,
                desc=f"Migrating {table_name}",
                unit="rows",
                initial=0
            )

            while True:
                # FIX #3: Build WHERE clause — no NOT EXISTS needed
                where_clause = self._build_where_clause(safe_key, current_key)
                order_clause = f"[{safe_key}]"

                # Clean INSERT ... SELECT without NOT EXISTS correlated subquery
                sql = f"""
                    INSERT INTO [{self.archive_db}].[dbo].[{safe_table}] ({col_list})
                    SELECT TOP {self.batch_size} {col_list}
                    FROM [{self.live_db}].[dbo].[{safe_table}] WITH (NOLOCK)
                    WHERE {where_clause}
                    ORDER BY {order_clause}
                """

                try:
                    with self.archive_engine.connect() as conn:
                        result = conn.execute(text(sql))
                        conn.commit()
                        rows_affected = result.rowcount

                    if rows_affected == 0:
                        break

                    stats.rows_migrated += rows_affected
                    pbar.update(rows_affected)

                    # Update checkpoint using archive MAX (single round-trip per batch)
                    with self.archive_engine.connect() as conn:
                        stmt = text(f"SELECT MAX([{safe_key}]) FROM [{self.archive_db}].[dbo].[{safe_table}]")
                        new_max = conn.execute(stmt).fetchone()[0]
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
            # 5. Re-enable Constraints (always runs)
            if constraints_disabled:
                try:
                    with self.archive_engine.connect() as conn:
                        conn.execute(text(f"ALTER TABLE [dbo].[{safe_table}] CHECK CONSTRAINT ALL"))
                        conn.commit()
                    self.logger.info(f"Constraints re-enabled for {table_name}")
                except Exception as e:
                    self.logger.error(f"Failed to enable constraints for {table_name}: {e}")

            stats.end_time = time.time()

            # FIX #8: Write final status to ETL_State so 'Running' doesn't linger
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
                    f"Finished {table_name} with errors. "
                    f"Migrated: {stats.rows_migrated}"
                )

    def get_summary(self):
        return self.completed_stats