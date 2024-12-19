import os
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
ORIGINAL_DB = os.getenv("ORIGINAL_DB")
BACKUP_DB = os.getenv("BACKUP_DB")
BACKUP_PATH = os.getenv("BACKUP_PATH")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
