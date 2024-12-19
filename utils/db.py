import pyodbc
from config.settings import SQL_SERVER, USER, PASSWORD

def get_connection(database=None):
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};"
    if database:
        conn_str += f"DATABASE={database};"
    if USER and PASSWORD:
        conn_str += f"UID={USER};PWD={PASSWORD}"
    else:
        conn_str += "Trusted_Connection=yes;"
    return pyodbc.connect(conn_str)
