#!/usr/bin/env python

import pdb

import re
import os
import os.path
from functools import reduce

import pyodbc
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, sessionmaker, mapped_column, MappedAsDataclass, Mapped
from sqlalchemy import Table, select, Column, Integer, String, Identity
from sqlalchemy.dialects.mssql import TINYINT, VARCHAR, SMALLINT, MONEY
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

INVENTORY_TABLE = "vehicle_inventory"


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class VehicleInventory(Base):
  __tablename__                  = INVENTORY_TABLE

  vehicle_identification_number : Mapped[str] = mapped_column(VARCHAR(17), unique=True, nullable=False)
  make                          : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  model                         : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  year                          : Mapped[int] = mapped_column(SMALLINT, nullable=False)
  mileage                       : Mapped[int] = mapped_column(Integer, nullable=False)
  price                         : Mapped[str] = mapped_column(MONEY, nullable=False)
  condition                     : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  color                         : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  interior_color                : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  engine                        : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  transmission                  : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  drive_train                   : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  fuel_type                     : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  body_style                    : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  number_of_seats               : Mapped[int] = mapped_column(TINYINT, nullable=False)
  doors                         : Mapped[int] = mapped_column(TINYINT, nullable=False)
  stock_number                  : Mapped[int] = mapped_column(Integer, Identity(), init=False, nullable=False,
                                                              primary_key=True)
  notes                         : Mapped[str] = mapped_column(VARCHAR(256))


TRANSFORMS = {
  4: [
    lambda s: s.strip().replace(',', ''),
  ],
  10: [
    lambda s: s.replace('CVT', 'Automatic'),
  ],
  11: [
    lambda s: s.replace('Four-Wheel Drive', '4WD'),
    lambda s: s.replace('All-Wheel Drive', 'AWD'),
    lambda s: s.replace('Rear-Wheel Drive', 'RWD'),
    lambda s: s.replace('Front-Wheel Drive', 'FWD'),
  ],
  12: [
    lambda s: s.replace('Plug-in Hybrid', 'Hybrid'),
  ],
  13: [
    lambda s: s.replace('Pickup Truck', 'Pickup'),
  ],
  16: [
    lambda s: r''.format(s),
  ],
  -1: [
  ]
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


SERVER = os.getenv("DB_HOST")
DB = os.getenv("DB_CONTEXT")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = './credentials.json'

SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
RANGE = os.getenv("GOOGLE_SHEET_RANGE")


def create_table(session, table):
  print(f'{colors.OKBLUE}[[INFO]]: attempting to create table "{table.name}"{colors.ENDC}')
  try:
    table.create(session.connection().engine)
  except Exception as e:
    print(f'{colors.FAIL}[[ERROR]]: while attempting to create table {table.name}.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None

  print(f'{colors.OKBLUE}[[INFO]]: successfuly created table "{table.name}"{colors.ENDC}')
  return session


def find_duplicates(cursor, table_name, field, value):
  query = (
    f'SELECT * FROM {table_name} '
    f'WHERE {field} = "{value}"' )

  rows = cursor.execute(query).fetchall()
  return rows


def insert_rows_into_table(session, table_name, rows):
  print(f'{colors.OKBLUE}[[INFO]]: attempting to insert data into table "{table_name}"{colors.ENDC}')

  try:
    for row in rows:
      vin = row.vehicle_identification_number
      duplicate = session.scalars(
          select(VehicleInventory).filter_by(vehicle_identification_number=vin) ).all()

      if not duplicate:
        session.add(row)
        session.commit()

  except Exception as e:
    print(f'{colors.FAIL}[[ERROR]]: while attempting to insert data into table {table_name}.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None

  print(f'{colors.OKBLUE}[[INFO]]: successfuly inserted data into table "{table_name}"{colors.ENDC}')
  return session


def check_if_table_exists(conn, table_name):
  print(f'{colors.OKBLUE}[[INFO]]: checking if table "{table_name}" exists.{colors.ENDC}')

  query = f"SELECT name FROM sys.tables WHERE name = '{table_name}'"
  try:
    res = [x for x in conn.execute(sa.text(query)).mappings()]
  except Exception as e: 
    print(f'{colors.FAIL}[[ERROR]]: while checking existence of table.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None

  if not res:
    print(f'{colors.OKBLUE}[[INFO]]: table "{table_name}" does not exist.{colors.ENDC}')
    return []
  else:
    print(f'{colors.OKBLUE}[[INFO]]: table already "{table_name}" exists.{colors.ENDC}')
    return res


def start_session(server, db, user, pwd):
  print(f'{colors.OKBLUE}[[INFO]]: attempting database connection using connection string:\n{colors.ENDC}')

  driver = pyodbc.drivers()[0].replace(' ', '+')
  connectionString = (
    f'mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}?driver={driver}&TrustServerCertificate=no&encrypt=no')
  print(connectionString)

  try:
    engine = sa.create_engine(connectionString)
    Session = sessionmaker(engine)
    session = Session()
  except Exception as e:
    print(f'{colors.FAIL}[[ERROR]]: failed to connect to database.{colors.ENDC}')
    print(f'\t{colors.FAIL}{e}{colors.ENDC}')
    return None

  print(f'{colors.OKBLUE}[[INFO]]: successfuly connected to database.{colors.ENDC}')
  return session
  

def get_data():
  creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

  values = []
  try:
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    values = (
      sheet.values()
           .get(spreadsheetId=SPREADSHEET_ID, range=RANGE)
           .execute()
           .get("values", []) )

  except HttpError as err:
    print(err)
    return None

  return values


def list_get(l, i):
  try:
    return l[i]
  except:
    return ''


def apply_transforms(transforms, row):
  return [ 
    reduce((lambda acc, fn: fn(acc)) ,transforms.get(i, [lambda _:_]) + transforms[-1], c)
    for i, c in enumerate([list_get(row, i) for i in range(17)]) # make sure all rows are same length
  ]


def main():
  session = start_session(SERVER, DB, USER, PWD)
  if session == None:
    exit(1)

  table_names = check_if_table_exists(session, INVENTORY_TABLE)
  if table_names == None:
    exit(2)

  session = create_table(session, VehicleInventory.__table__) if len(table_names) == 0 else session
  if session == None:
    exit(3)

  rows = get_data()
  if rows == None:
    exit(4)

  transformed_rows = [VehicleInventory(*apply_transforms(TRANSFORMS, r)) for r in rows]
  insert_rows_into_table(session, INVENTORY_TABLE, transformed_rows[:])

  session.close()


if __name__ == "__main__":
  main()
