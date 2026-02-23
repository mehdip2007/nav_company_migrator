from config_manager import ConfigManager
from database import DatabaseConnector
from schema_manager import SchemaManager
from migrator import DataMigrator
from exceptions import NoChunkingKeyError, MigrationError
from logger import LoggerSetup
from tabulate import tabulate

def print_summary(stats_list, log_file):
    print("\n" + "="*80)
    print("MIGRATION SUMMARY")
    print("="*80)
    
    table_data = []
    total_rows = 0
    total_duration = 0
    
    for stats in stats_list:
        remaining = stats.total_source_rows - stats.rows_migrated
        # Note: Remaining is approximate if archive had pre-existing data
        speed = f"{stats.rows_per_second:.2f} rows/sec"
        
        status = "SUCCESS" if len(stats.errors) == 0 else "FAILED"
        
        table_data.append([
            stats.table_name,
            status,
            stats.total_source_rows,
            stats.rows_migrated,
            remaining,
            f"{stats.duration_seconds:.2f}s",
            speed
        ])
        
        total_rows += stats.rows_migrated
        total_duration += stats.duration_seconds

    headers = ["Table", "Status", "Source Count", "Migrated", "Remaining", "Duration", "Throughput"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    print("\n" + "="*80)
    print(f"TOTAL ROWS MIGRATED: {total_rows}")
    print(f"TOTAL DURATION: {total_duration:.2f} seconds")
    if total_duration > 0:
        print(f"OVERALL THROUGHPUT: {total_rows/total_duration:.2f} rows/sec")
    print(f"LOG FILE: {log_file}")
    print("="*80 + "\n")

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
                # Ensure Schema
                schema_mgr.sync_table_schema(table)
                
                # Get Chunking Key
                try:
                    chunk_key = schema_mgr.get_chunking_column(table)
                except NoChunkingKeyError as e:
                    logger.warning(f"Skipping {table}. Reason: {e}")
                    continue
                
                # Get Columns (excluding timestamp)
                columns = schema_mgr.get_insert_columns(table)
                
                # Migrate Data
                migrator.migrate_table(table, chunk_key, columns)
                
            except Exception as e:
                logger.critical(f"CRITICAL ERROR on {table}: {e}")
                # Continue to next table instead of stopping entire process
                continue 

        # 7. Print Summary
        print_summary(migrator.get_summary(), logger_setup.log_file_path)
        logger.info("Migration Process Finished.")

    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
        raise e

if __name__ == "__main__":
    main()
