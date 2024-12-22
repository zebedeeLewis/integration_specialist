#!/usr/bin/env python

import pyodbc
import os

FIELDS = {
  "vehicle_identification_number": "VARCHAR(17) UNIQUE NOT NULL",
  "make":                          "VARCHAR(32) NOT NULL",
  "model":                         "VARCHAR(32) NOT NULL",
  "year":                          "SMALLINT NOT NULL",
  "mileage":                       "INT NOT NULL",
  "price":                         "MONEY NOT NULL",
  "condition":                     "VARCHAR(32) NOT NULL",
  "color":                         "VARCHAR(32) NOT NULL",
  "interior_color":                "VARCHAR(32) NOT NULL",
  "engine":                        "VARCHAR(32) NOT NULL",
  "transmission":                  "VARCHAR(32) NOT NULL",
  "drive_train":                   "VARCHAR(32) NOT NULL",
  "fuel_type":                     "VARCHAR(32) NOT NULL",
  "body_style":                    "VARCHAR(32) NOT NULL",
  "number_of_seats":               "TINYINT NOT NULL",
  "doors":                         "TINYINT NOT NULL",
  "stock_number":                  "INT NOT NULL PRIMARY KEY IDENTITY(1, 1)",
  "notes":                         "VARCHAR(256)",
}

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

INVENTORY_TABLE = "vehicle_inventory"
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


def create_table(cursor, table_name, fields):
  print(f'{colors.OKBLUE}[[INFO]]: attempting to create table "{table_name}"{colors.ENDC}')
  query = (
    f"CREATE TABLE {table_name} ("+
    ', '.join([' '.join(x) for x in fields.items()]) +
    ')' )

  try:
    cursor.execute(query).commit()
  except Exception as e:
    print(f'{colors.FAIL}[[ERROR]]: while attempting to create table {table_name}.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None

  print(f'{colors.OKBLUE}[[INFO]]: successfuly created table "{table_name}"{colors.ENDC}')
  return cursor


def check_if_table_exists(cursor, table_name):
  print(f'{colors.OKBLUE}[[INFO]]: checking if table "{table_name}" exists.{colors.ENDC}')

  try:
    res = cursor.execute(f"SELECT name FROM sys.tables WHERE name = '{table_name}'")
  except Exception as e: 
    print(f'{colors.FAIL}[[ERROR]]: while checking existence of table.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None
 
  table_names = res.fetchone()

  if table_names is None:
    print(f'{colors.OKBLUE}[[INFO]]: table "{table_name}" does not exist.{colors.ENDC}')
    return []
  else:
    print(f'{colors.OKBLUE}[[INFO]]: table already "{table_name}" exists.{colors.ENDC}')
    return table_names
    

def open_connection():
  print(f'{colors.OKBLUE}[[INFO]]: attempting database connection using connection string:\n{colors.ENDC}'+
        f'\t{colors.OKBLUE}{connectionString}...{colors.ENDC}')
  try:
    conn = pyodbc.connect(connectionString)
  except Exception as e:
    print(f'{colors.FAIL}[[ERROR]]: failed to connect to database.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None

  print(f'{colors.OKBLUE}[[INFO]]: successfuly connected to database.{colors.ENDC}')
  return conn
  

def main():
  conn = open_connection()
  if conn == None:
    exit(1)
  
  cursor = conn.cursor()
  table_names = check_if_table_exists(cursor, INVENTORY_TABLE)
  if table_names == None:
    exit(2)

  cursor = create_table(cursor, INVENTORY_TABLE, FIELDS) if len(table_names) == 0 else cursor
  if cursor == None:
    exit(3)

  conn.close()


if __name__ == "__main__":
  main()
