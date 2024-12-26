#!/usr/bin/env python
from __future__ import annotations

import pdb
import re
import os
import os.path
from typing import Callable
from functools import reduce

from expression import result, effect, Result, Ok, Error, curry, pipe as ꓸ, compose as λ 
from pydantic import BaseModel
from dotenv import load_dotenv

import pyodbc
import sqlalchemy as sa
from sqlalchemy.orm import (
  DeclarativeBase, sessionmaker, Session, mapped_column, MappedAsDataclass, Mapped)
from sqlalchemy import select, Integer, String, Identity, Table
from sqlalchemy.dialects.mssql import TINYINT, VARCHAR, SMALLINT, MONEY
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


load_dotenv()

rbind = result.bind
rmap = result.map
map_error = result.map_error
rswap = result.swap

E_CREATE_TABLE = 1

class Base(MappedAsDataclass, DeclarativeBase): pass

class VehicleInventory(Base):
  __tablename__                 = "vehicle_inventory"

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


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = './credentials.json'

SERVER = os.getenv("DB_ADDRESS")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")

SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
RANGE = os.getenv("GOOGLE_SHEET_RANGE")


class Log(BaseModel):
  color: str = ''
  prefix: str = ''
  message: str = ''

  @curry(1)
  @staticmethod
  def apply_level_prefix(log_level: str, log: Log) -> Log:
    match(log_level):
      case "error":
        prefix = "[[ERROR]]: "
      case "warning":
        prefix = "[[WARNING]]: "
      case _:
        prefix = "[[INFO]]: "

    return Log(**{**log.model_dump(), 'prefix':prefix})


  @staticmethod
  def from_str(msg: str) -> Log:
    return Log(message=msg)


  @staticmethod
  def to_str(log: Log) -> str:
    return f'{log.color}{log.prefix}{log.message}\033[0m'


  @curry(1)
  @staticmethod
  def apply_level_color(log_level: str, log: Log) -> Log:
    match(log_level):
      case "error":
        color = '\033[91m'
      case "warning":
        color = '\033[93m'
      case _:
        color = '\033[94m'

    return Log(**{**log.model_dump(), 'color':color})


  # format the given message
  # LogLevel -> Log -> Log
  @staticmethod
  def format(log_level: str) -> Callable[[Log], Log]:
    return λ( Log.apply_level_prefix(log_level)
            , Log.apply_level_color(log_level)
            , )


  @curry(1)
  @staticmethod
  def set_message(msg: str, log: Log) -> Log:
    return Log(**{**log.model_dump(), 'message':msg})


  @effect.result[list, int]()
  @staticmethod
  def write(log: Log) -> Result[Log, int]:
    ꓸ(log,
      Log.to_str,
      print )

    yield log

  @curry(1)
  @staticmethod
  def xinfo(s: str, result: Result) -> Result:
    return ꓸ(
      result,
      rbind(lambda r:ꓸ(
        Log.from_str(s),
        Log.format("info"),
        Log.write,
        rmap(lambda _: r) )))


  @curry(1)
  @staticmethod
  def xerror(s: str, res: Result) -> Result:
    return ꓸ(
      res,
      map_error(lambda r:ꓸ(
        Log.from_str(s),
        Log.format("error"),
        Log.write,
        rbind(lambda _: Error(r)) )))


  @curry(1)
  @staticmethod
  def xwarning(s: str, result: Result) -> Result:
    return ꓸ(
      result,
      rbind(lambda r:ꓸ(
        Log.from_str(s),
        Log.format("warning"),
        Log.write,
        rmap(lambda _: r) )))


# str -> Result[A,E] -> Result[A,E]
Log.info = λ(Log.from_str, Log.format("info"), Log.write)
Log.error = λ(Log.from_str, Log.format("error"), Log.write)
Log.warning = λ(Log.from_str, Log.format("error"), Log.write)


@curry(1)
def sqlalchemy_create_table(table: Table, session: Session) -> Result[Session, int]:
  try:
    table.create(session.connection().engine)
    return Result.Ok(session)
  except Exception as e:
    return Result.Error(e)


def create_table(table: Table) -> Callable[[Session], Result[Session, int]]:
  return λ(
    Ok,
    Log.xinfo(f'attempting to create table "{table.name}"'),
    rbind(sqlalchemy_create_table(table)),
    Log.xerror(f'while attempting to create table {table.name}.\n\t'),
    map_error(λ(
      str,
      Log.from_str,
      Log.format("error"),
      Log.write,
      rbind(lambda _: Error(E_CREATE_TABLE)) )),
    Log.xinfo(f'successfuly created table "{table.name}".') )


def find_duplicates(cursor, table_name, field, value):
  query = (
    f'SELECT * FROM {table_name} '
    f'WHERE {field} = "{value}"' )

  rows = cursor.execute(query).fetchall()
  return rows


def insert_rows_into_table(session, table_name, rows):
  Log.info(f'attempting to insert data into table "{table_name}".')

  try:
    for row in rows:
      vin = row.vehicle_identification_number
      duplicate = session.scalars(
          select(VehicleInventory).filter_by(vehicle_identification_number=vin)
      ).all()

      if not duplicate:
        session.add(row)
        session.commit()

  except Exception as e:
    Log.error(
      f'while attempting to insert data into table {table_name}.\n\t{e}')

    return None

  Log.info(f'successfuly inserted data into table "{table_name}".')
  return session


def check_if_table_exists(conn, table_name):
  Log.info(f'checking if table "{table_name}" exists.')

  query = f"SELECT name FROM sys.tables WHERE name = '{table_name}'"
  try:
    res = [x for x in conn.execute(sa.text(query)).mappings()]
  except Exception as e: 
    Log.error(f'while checking existence of table.\n\t{e}')
    return None

  msg = f'table already "{table_name}" exists.'
  if not res:
    msg = f'table "{table_name}" does not exist.'
    res = []

  Log.info(msg)
  return res


def start_session(server, db, user, pwd):
  Log.info('Starting new database session.')

  connectionString = (
    f'mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}'
    f'?driver={pyodbc.drivers()[0].replace(" ", "+")}'
     '&TrustServerCertificate=no&encrypt=no')

  Log.info(connectionString)

  try:
    engine = sa.create_engine(connectionString)
    Session = sessionmaker(engine)
    session = Session()
  except Exception as e:
    Log.error(f'Failed to start database session.\n\t{e}')
    return None

  Log.info(f'successfuly connected to database.')
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
    Log.error(err)
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

  table_names = check_if_table_exists(session, VehicleInventory.__tablename__)
  if table_names == None:
    exit(2)

  session = create_table(VehicleInventory.__table__)(session) if len(table_names) == 0 else session
  if session == None:
    exit(3)

  rows = get_data()
  if rows == None:
    exit(4)

  transformed_rows = [VehicleInventory(*apply_transforms(TRANSFORMS, r)) for r in rows]
  
  insert_rows_into_table(session, VehicleInventory.__tablename__, transformed_rows[:])

  session.close()


if __name__ == "__main__":
  main()
