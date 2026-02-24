from config_manager import ConfigManager
from database import DatabaseConnector
from schema_manager import SchemaManager
from migrator import DataMigrator
from exceptions import NoChunkingKeyError, MigrationError
from logger import LoggerSetup
from tabulate import tabulate


def print_summary(stats_list, log_file, logger):
    """
    FIX: Summary now written to both console and log file.
    Includes per-table breakdown plus totals.
    """
    lines = []
    lines.append("")
    lines.append("=" * 80)
    lines.append("MIGRATION SUMMARY")
    lines.append("=" * 80)

    table_data = []
    total_rows = 0
    total_duration = 0

    for stats in stats_list:
        remaining = stats.total_source_rows - stats.rows_migrated
        speed = f"{stats.rows_per_second:.2f} rows/sec"
        status = "SUCCESS" if len(stats.errors) == 0 else "FAILED"
        mode = getattr(stats, "mode", "key").upper()

        table_data.append([
            stats.table_name,
            status,
            mode,
            stats.total_source_rows,
            stats.rows_migrated,
            remaining,
            f"{stats.duration_seconds:.2f}s",
            speed
        ])

        total_rows += stats.rows_migrated
        total_duration += stats.duration_seconds

    headers = ["Table", "Status", "Mode", "Source Count", "Migrated", "Remaining", "Duration", "Throughput"]
    table_str = tabulate(table_data, headers=headers, tablefmt="grid")

    lines.append(table_str)
    lines.append("")
    lines.append("=" * 80)
    lines.append(f"TOTAL ROWS MIGRATED: {total_rows}")
    lines.append(f"TOTAL DURATION: {total_duration:.2f} seconds")
    if total_duration > 0:
        lines.append(f"OVERALL THROUGHPUT: {total_rows / total_duration:.2f} rows/sec")
    lines.append(f"LOG FILE: {log_file}")
    lines.append("=" * 80)
    lines.append("")

    # Print to console
    for line in lines:
        print(line)

    # FIX: Also write every summary line to the log file
    for line in lines:
        logger.info(line)


def main():
    # 1. Initialize Logger
    logger_setup = LoggerSetup()
    logger = logger_setup.get_logger()
    logger.info("Starting NAV/BC Migration Process...")

    try:
        # 2. Load Config
        config = ConfigManager()

        # 3. Initialize Connections
        live_params = config.get_live_connection_params()
        live_conn = DatabaseConnector(**live_params)

        archive_params = live_params.copy()
        archive_params["database"] = config.get_archive_database_name()
        archive_conn = DatabaseConnector(**archive_params)

        logger.info(f"Connected to Live: {live_conn.get_database_name()}")
        logger.info(f"Connected to Archive: {archive_conn.get_database_name()}")

        # 4. Initialize Managers
        company_name = config.get_company_name()
        schema_mgr = SchemaManager(live_conn.get_engine(), archive_conn.get_engine(), company_name)

        # 5. Get Tables
        logger.info("Discovering tables...")
        tables = schema_mgr.get_company_tables()
        logger.info(f"Found {len(tables)} tables for company '{schema_mgr.company_name}'.")

        # 6. Migration Loop
        migrator = DataMigrator(
            live_conn.get_engine(),
            archive_conn.get_engine(),
            live_conn.get_database_name(),
            archive_conn.get_database_name(),
            config.get_batch_size(),
            logger
        )

        for table in tables:
            try:
                # Ensure Schema exists in Archive
                schema_mgr.sync_table_schema(table)

                # Get Columns (excluding timestamp)
                columns = schema_mgr.get_insert_columns(table)

                # --- Strategy Selection ---
                # Key-based first (fastest). Falls back to offset for composite PKs.
                try:
                    chunk_key = schema_mgr.get_chunking_column(table)
                    migrator.migrate_table(table, chunk_key, columns)

                except NoChunkingKeyError:
                    pk_cols = schema_mgr.get_pk_columns(table)

                    if not pk_cols:
                        logger.warning(
                            f"Skipping '{table}': no PK, no identity, and no known NAV columns. "
                            f"Cannot determine a safe migration strategy."
                        )
                        continue

                    logger.info(
                        f"Composite PK detected on '{table}' {pk_cols}. "
                        f"Switching to OFFSET-based migration."
                    )
                    migrator.migrate_table_by_offset(table, pk_cols, columns)

            except Exception as e:
                logger.critical(f"CRITICAL ERROR on {table}: {e}")
                continue

        # 7. Print + Log Summary (FIX: logger passed so summary goes to log file too)
        print_summary(migrator.get_summary(), logger_setup.log_file_path, logger)
        logger.info("Migration Process Finished.")

    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
        raise e


if __name__ == "__main__":
    main()
