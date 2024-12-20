import pyodbc
import os
from datetime import datetime
import logging
from config.settings import (
    SQL_SERVER,
    ORIGINAL_DB,
    BACKUP_PATH,
    BACKUP_DB,
    USER,
    PASSWORD,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("backup.log"), logging.StreamHandler()],
)


def get_connection(database=None):
    """Establish a connection to the SQL Server."""
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};"
        if database:
            conn_str += f"DATABASE={database};"
        if USER and PASSWORD:
            conn_str += f"UID={USER};PWD={PASSWORD}"
        else:
            conn_str += "Trusted_Connection=yes;"

        logging.debug(f"Connection string: {conn_str}")  # Optional: For debugging
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True
        return conn
    except pyodbc.Error as e:
        logging.error(f"Database connection failed: {e}")
        raise


def backup_database():
    """Create a backup of the database."""
    try:
        if not os.path.exists(BACKUP_PATH):
            os.makedirs(BACKUP_PATH)

        backup_file = os.path.join(
            BACKUP_PATH,
            f"{ORIGINAL_DB}_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.bak",
        )
        with get_connection() as conn:
            cursor = conn.cursor()
            backup_query = f"""
            BACKUP DATABASE {ORIGINAL_DB}
            TO DISK = '{backup_file}'
            WITH FORMAT, INIT;
            """
            cursor.execute(backup_query)
            logging.info(f"Backup created successfully: {backup_file}")
        return backup_file
    except Exception as e:
        logging.error(f"Backup failed: {e}")
        raise


def restore_backup(backup_file):
    """Restore the database from a backup file."""
    if not os.path.exists(backup_file):
        logging.error(f"Backup file not found: {backup_file}")
        raise FileNotFoundError(f"Backup file not found: {backup_file}")

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            restore_query = f"""
            RESTORE DATABASE {BACKUP_DB}
            FROM DISK = '{backup_file}'
            WITH REPLACE;
            """
            cursor.execute(restore_query)
            logging.info(f"Backup restored successfully to {BACKUP_DB}")
    except Exception as e:
        logging.error(f"Restore failed: {e}")
        raise


def count_tables_and_rows(database):
    """Count the number of tables and rows in a database."""
    try:
        with get_connection(database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
            )
            table_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT SUM(row_count)
                FROM (
                    SELECT p.[Rows] AS row_count
                    FROM sys.objects AS o
                    INNER JOIN sys.partitions AS p ON o.object_id = p.object_id
                    WHERE o.type = 'U' AND p.index_id IN (0, 1)
                ) AS total_rows;
                """
            )
            row_count = cursor.fetchone()[0] or 0  # Handle None case for empty tables
        return table_count, row_count
    except Exception as e:
        logging.error(f"Error counting tables and rows: {e}")
        raise


def validate_backup():
    """Validate the backup by comparing tables and rows between original and backup databases."""
    try:
        original_counts = count_tables_and_rows(ORIGINAL_DB)
        backup_counts = count_tables_and_rows(BACKUP_DB)

        logging.info("\nValidation Results:")
        logging.info(
            f"Original Database ({ORIGINAL_DB}) - Tables: {original_counts[0]}, Rows: {original_counts[1]}"
        )
        logging.info(
            f"Backup Database ({BACKUP_DB}) - Tables: {backup_counts[0]}, Rows: {backup_counts[1]}"
        )

        if original_counts == backup_counts:
            logging.info(
                "✅ Validation successful: The backup matches the original database."
            )
        else:
            logging.warning(
                "❌ Validation failed: The backup does not match the original database."
            )
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        raise


def main(action, backup_file=None):
    """Main function to perform backup, restore, or validation."""
    try:
        if action == "backup":
            backup_file = backup_database()
        elif action == "restore" and backup_file:
            restore_backup(backup_file)
        elif action == "validate":
            validate_backup()
        else:
            logging.error("Invalid action. Use 'backup', 'restore', or 'validate'.")
    except Exception as e:
        logging.error(f"Operation failed: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Database Backup and Validation Tool")
    parser.add_argument(
        "action", choices=["backup", "restore", "validate"], help="Action to perform"
    )
    parser.add_argument(
        "--file", help="Backup file path for restore action", default=None
    )
    args = parser.parse_args()

    main(args.action, args.file)
