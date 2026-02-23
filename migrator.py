from sqlalchemy import text
from tqdm import tqdm
from state_manager import StateManager
from exceptions import MigrationError, MigrationBatchError, NoChunkingKeyError
from logger import LoggerSetup
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
                stmt = text(f"SELECT COUNT(*) FROM [{self.live_db}].[dbo].[{table_name}] WITH (NOLOCK)")
                result = conn.execute(stmt).fetchone()
                return result[0] if result else 0
        except Exception as e:
            self.logger.warning(f"Could not count rows for {table_name}: {e}")
            return 0

    def migrate_table(self, table_name, chunking_key, columns):
        """
        Migrate data for a single table with resume capability, stats tracking, 
        and structured exception handling.
        """
        stats = MigrationStats(table_name)
        stats.start_time = time.time()
        
        self.logger.info(f"Starting migration for table: {table_name}")

        # 1. Get Max Key from Live
        try:
            with self.live_engine.connect() as conn:
                stmt = text(f"SELECT MAX([{chunking_key}]) FROM [{self.live_db}].[dbo].[{table_name}] WITH (NOLOCK)")
                max_key_result = conn.execute(stmt).fetchone()
                max_key = max_key_result[0] if max_key_result[0] is not None else 0
        except Exception as e:
            err = MigrationError(f"Failed to get Max Key: {str(e)}", table_name)
            self.logger.error(str(err))
            stats.errors.append(str(err))
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        if max_key is None:
            self.logger.info(f"Table {table_name} is empty.")
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            return

        # 2. Get Total Source Count for Summary
        stats.total_source_rows = self.get_source_count(table_name)

        # 3. Get Last Checkpoint
        last_key = self.state_manager.get_checkpoint(table_name)
        current_key = last_key

        # 4. Disable Constraints on Archive
        constraints_disabled = False
        try:
            with self.archive_engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE [dbo].[{table_name}] NOCHECK CONSTRAINT ALL"))
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
            col_list = ", ".join([f"[{c}]" for c in columns])
            pbar = tqdm(desc=f"Migrating {table_name}", unit="rows", initial=0)

            while True:
                # Build Dynamic SQL for Batch
                if current_key is None:
                    where_clause = "1=1"
                    order_clause = f"[{chunking_key}]"
                else:
                    if isinstance(current_key, str):
                        where_clause = f"[{chunking_key}] > '{current_key}'"
                    else:
                        where_clause = f"[{chunking_key}] > {current_key}"
                    order_clause = f"[{chunking_key}]"

                sql = f"""
                    INSERT INTO [{self.archive_db}].[dbo].[{table_name}] ({col_list})
                    SELECT TOP {self.batch_size} {col_list}
                    FROM [{self.live_db}].[dbo].[{table_name}] WITH (NOLOCK)
                    WHERE {where_clause}
                    AND NOT EXISTS (
                        SELECT 1 FROM [{self.archive_db}].[dbo].[{table_name}] T 
                        WHERE T.[{chunking_key}] = [{self.live_db}].[dbo].[{table_name}].[{chunking_key}]
                    )
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

                    # Update Checkpoint
                    with self.archive_engine.connect() as conn:
                        stmt = text(f"SELECT MAX([{chunking_key}]) FROM [{self.archive_db}].[dbo].[{table_name}]")
                        new_max = conn.execute(stmt).fetchone()[0]
                        current_key = new_max
                        self.state_manager.update_checkpoint(table_name, current_key, "Running")

                except Exception as e:
                    # Raise custom exception with context (Processing ID)
                    raise MigrationBatchError(
                        message=f"Batch insert failed: {str(e)}", 
                        table_name=table_name, 
                        current_key=current_key
                    )

            pbar.close()

        except MigrationBatchError as e:
            # Catch custom batch error to log context and break loop gracefully
            self.logger.error(str(e))
            stats.errors.append(str(e))
        
        except Exception as e:
            # Catch any other unexpected errors
            err = MigrationError(f"Unexpected error in migration loop: {str(e)}", table_name)
            self.logger.critical(str(err))
            stats.errors.append(str(err))
        
        finally:
            # 5. Re-enable Constraints (Always run this)
            if constraints_disabled:
                try:
                    with self.archive_engine.connect() as conn:
                        conn.execute(text(f"ALTER TABLE [dbo].[{table_name}] CHECK CONSTRAINT ALL"))
                        conn.commit()
                    self.logger.info(f"Constraints re-enabled for {table_name}")
                except Exception as e:
                    self.logger.error(f"Failed to enable constraints for {table_name}: {e}")
            
            stats.end_time = time.time()
            self.completed_stats.append(stats)
            
            if len(stats.errors) == 0:
                self.logger.info(f"Completed {table_name}. Migrated: {stats.rows_migrated}, Duration: {stats.duration_seconds:.2f}s")
            else:
                self.logger.warning(f"Finished {table_name} with errors. Migrated: {stats.rows_migrated}")

    def get_summary(self):
        return self.completed_stats
