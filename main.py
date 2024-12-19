import pyodbc
import os
from datetime import datetime
from config.settings import SQL_SERVER, ORIGINAL_DB, BACKUP_PATH, BACKUP_DB, USER, PASSWORD

def get_connection(database=None):
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};"
    if database:
        conn_str += f"DATABASE={database};"
    if USER and PASSWORD:
        conn_str += f"UID={USER};PWD={PASSWORD}"
    else:
        conn_str += "Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True  # Enable auto-commit mode
    return conn


def backup_database():
    backup_file = os.path.join(BACKUP_PATH, f"{ORIGINAL_DB}_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.bak")
    with get_connection() as conn:
        cursor = conn.cursor()
        backup_query = f"""
        BACKUP DATABASE {ORIGINAL_DB}
        TO DISK = '{backup_file}'
        WITH FORMAT, INIT;
        """
        cursor.execute(backup_query)
        cursor.commit()
    print(f"Backup created: {backup_file}")
    return backup_file



def count_tables_and_rows(database):
    with get_connection(database) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
        table_count = cursor.fetchone()[0]

        cursor.execute("""
        SELECT SUM(row_count)
        FROM (
            SELECT p.[Rows] AS row_count
            FROM sys.objects AS o
            INNER JOIN sys.partitions AS p ON o.object_id = p.object_id
            WHERE o.type = 'U' AND p.index_id IN (0, 1)
        ) AS total_rows;
        """)
        row_count = cursor.fetchone()[0]
    
    return table_count, row_count


def main():

    if not os.path.exists(BACKUP_PATH):
        os.makedirs(BACKUP_PATH)


    backup_database()
    # restore_backup(backup_file)

    original_counts = count_tables_and_rows(ORIGINAL_DB)
    backup_counts = count_tables_and_rows(BACKUP_DB)

    print("\nValidation Results:")
    print(f"Original Database - Tables: {original_counts[0]}, Rows: {original_counts[1]}")
    print(f"Backup Database - Tables: {backup_counts[0]}, Rows: {backup_counts[1]}")

    if original_counts == backup_counts:
        print("Validation successful: The backup matches the original database.")
    else:
        print("Validation failed: The backup does not match the original database.")

if __name__ == "__main__":
    main()
