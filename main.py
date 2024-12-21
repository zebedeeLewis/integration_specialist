#!/usr/bin/env python

import pyodbc
import os

class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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

SQL_QUERY = f"""
CREATE TABLE vehicle_inventory (
  col1 varchar(128),
  col2 varchar(128)
);
"""

def main():
  print(f'{colors.OKBLUE}[[INFO]]: attempting database connection using connection string:\n{colors.ENDC}'+
        f'\t{colors.OKBLUE}{connectionString}...{colors.ENDC}')
  try:
    conn = pyodbc.connect(connectionString)
  except Exception as e:
    print(f'{colors.FAIL}[[ERROR]]: failed to connect to database.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    exit(1)
  
  cursor = conn.cursor()
  cursor.execute(SQL_QUERY)
  conn.commit()
  conn.close()

if __name__ == "__main__":
  main()
