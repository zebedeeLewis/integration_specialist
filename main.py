import pyodbc
import os

SERVER = os.getenv("DB_HOST")
DATABASE = os.getenv("DB_CONTEXT")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

connectionString = (
  f'DRIVER={{ODBC Driver 18 for SQL Server}};' +
  f'TrustServerCertificate=no;' +
  f'SERVER={SERVER},1433;' +
  f'DATABASE={DATABASE};' +
  f'encrypt=no;' +
  f'UID={USERNAME};' +
  f'PWD={PASSWORD}')

SQL_QUERY = """
CREATE TABLE vehicle_inventory (
  col1 varchar(128),
  col2 varchar(128)
);
"""

conn = pyodbc.connect(connectionString)
cursor = conn.cursor()
cursor.execute(SQL_QUERY)
conn.commit()
