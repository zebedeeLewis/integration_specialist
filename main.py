#!/usr/bin/env python
from __future__ import annotations

import pdb
import re
import os
import os.path
from typing import Callable
from functools import reduce

from expression import result, flip, effect, Result, Ok, Error, curry, compose
from expression.collections import TypedArray, seq
from pydantic import BaseModel
from dotenv import load_dotenv

import pyodbc
import sqlalchemy as sa
from sqlalchemy.orm import (
  DeclarativeBase, sessionmaker, Session, mapped_column, MappedAsDataclass, Mapped)
from sqlalchemy import select, text as as_text, Engine, Integer, String, Identity, Table
from sqlalchemy.dialects.mssql import TINYINT, VARCHAR, SMALLINT, MONEY
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


load_dotenv()

thenr = result.bind
rmap = result.map
smap = seq.map
filter = compose(seq.filter, seq.of_iterable)
map_error = result.map_error
rswap = result.swap


E_PERSIST_TABLE = 1
E_INIT_ENGINE = 2
E_INIT_SESSION = 2

E_ODBC_TABLE_EXISTS = 'f405'


class Infix:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __or__(self, other):
        return self.function(other)
    def __call__(self, value1):
      return lambda value2: self.function(value1, value2)

class Unary:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __or__(self, other):
        return self.function(other)
    def __call__(self, value1):
      return lambda value2: self.function(value1, value2)


def add_doc(doc, fn):
  fn.__doc__ = doc
  return fn


Pipe_To = Infix(lambda x, fn: fn(x))
And = Infix(lambda fn1, fn2: compose(fn1, fn2))
Then = Infix(lambda monad, fn: monad.bind(fn))
Catch = Infix(lambda monad, fn: monad.map_error(fn))
Map = Infix(lambda functor, fn: functor.map(fn))
Filter = Infix(lambda sequence, fn: filter(fn)(sequence))
λ = Infix(add_doc)


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


VehicleInventoryTable = VehicleInventory.__table__


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

CONNECTION_STRING = (
  f'mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}'
  f'?driver={pyodbc.drivers()[0].replace(" ", "+")}'
   '&TrustServerCertificate=no&encrypt=no')

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
    return (
      Log.apply_level_prefix(log_level)
      |And| Log.apply_level_color(log_level))


  @curry(1)
  @staticmethod
  def set_message(msg: str, log: Log) -> Log:
    return Log(**{**log.model_dump(), 'message':msg})


  @effect.result[list, int]()
  @staticmethod
  def write(log: Log) -> Result[Log, int]:
    log |Pipe_To| Log.to_str |Pipe_To| print
    yield log

    
  @staticmethod
  def info_message(s: str) -> Callable[[Result], Result]:
    return\
      thenr(lambda r: s\
        |Pipe_To| Log.from_str
        |Pipe_To| Log.format("info")
        |Pipe_To| Log.write
        |Pipe_To| rmap(lambda _: r))


  @staticmethod
  def error_message(s: str) -> Callable[[Result], Result]:
    return\
      map_error(lambda r: s\
        |Pipe_To| Log.from_str
        |Pipe_To| Log.format("error")
        |Pipe_To| Log.write
        |Pipe_To| thenr(lambda _: r |Pipe_To| Error) )


  def warning_message(s: str) -> Callable[[Result], Result]:
    return\
      thenr(lambda r: s\
        |Pipe_To| Log.from_str
        |Pipe_To| Log.format("warning")
        |Pipe_To| Log.write
        |Pipe_To| rmap(lambda _: r) )


# Result[A, E] ->  Result[A, E]
Log.exception = \
  map_error(
   str
   |And| Log.from_str
   |And| Log.format("error")
   |And| Log.write
   |And| thenr(lambda _: Error(None)) )


# str -> Result[A,E] -> Result[A,E]
Log.info =(
  Log.from_str
  |And| Log.format("info")
  |And| Log.write)


# str -> Result[A,E] -> Result[A,E]
Log.error =(
  Log.from_str
  |And| Log.format("error")
  |And| Log.write)


# str -> Result[A,E] -> Result[A,E]
Log.warning =(
  Log.from_str
  |And| Log.format("warning")
  |And| Log.write)


# Callable[[], A] -> Result[A,Exception]
#
# execute the given unary function, wrapping the result in Reslut.Ok.
# Catches and wraps any exciption in a Result.Error.
def try_catch(fn: Callable[[A], B]) -> Callable[[A], Result[B,Exception]]:
  def _fn(a):
    try:
      return Ok(fn(a))
    except Exception as e:
      return Error(e)

  return _fn


@curry(1)
def sqlalchemy_persist_table(table: Table, session: Session) -> Result[Session, Exception]:
  try:
    table.create(session.connection().engine)
    return Result.Ok(session)
  except Exception as e:
    return Result.Error(e)


_persist_table=\
'''
Table -> Session -> Result[Session, int]

Create a new table using the given session.
'''|λ|(lambda table:
        Ok
    |And| Log.info_message(f'Attempting to create table "{table.name}"')
    |And| thenr(sqlalchemy_persist_table(table))
    |And| Log.error_message(f'While attempting to create table {table.name}.')
    |And| Log.exception
    |And| map_error(lambda _:E_PERSIST_TABLE)
    |And| Log.info_message(f'Successfuly created table "{table.name}".'))


def find_duplicates(cursor, table_name, field, value):
  query = (
    f'SELECT * FROM {table_name} '
    f'WHERE {field} = "{value}"' )

  rows = cursor.execute(query).fetchall()
  return rows


def _insert_rows_into_table(session, table_name, rows):
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


sqlalchemy_execute_statement = (
  Session.execute
  |Pipe_To| flip
  |Pipe_To| curry(1) )


check_if_table_exists =\
'''
string -> Result[Session, Error]

Produce true if a table exists in the database.
'''|λ| (lambda name:
      Ok
  |And| Log.info_message(f'checking if table "{name}" exists.')
  |And| thenr(try_catch(
       f"SELECT name FROM sys.tables WHERE name = '{name}'"
       |Pipe_To| as_text
       |Pipe_To| sqlalchemy_execute_statement ))
  |And| rmap(lambda r:\
       r.mappings()
       |Pipe_To| TypedArray.of_seq
       |Pipe_To| TypedArray.is_empty ))


init_db_session = lambda engine:\
  sessionmaker(engine)()


_new_session =\
'''
string -> Result[Session, int]

Produce a new database session.

Side Effects:
Prints progress logs as it attempts and either
succeed/fails to initialize a databse engine,
and subsequently initialize the new session.
''' |λ|(  Ok
     |And| Log.info_message('Initializing database engine.')
     |And| thenr(sa.create_engine |Pipe_To| try_catch)
     |And| Log.info_message('Successfuly initialized database engine.')
     |And| Log.error_message('While initializing database engine.')
     |And| Log.exception
     |And| map_error(lambda _:E_INIT_ENGINE)

     |And| thenr(
         Ok
         |And| Log.info_message('Initializing database session.')
         |And| thenr(try_catch(init_db_session))
         |And| Log.info_message('Successfuly initialized database session.')
         |And| Log.error_message('While initializing database session.')
         |And| Log.exception
         |And| map_error(lambda _:E_INIT_SESSION) ))


def _fetch_data_from_google_sheet():
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


def debug(x):
  pdb.set_trace()
  return x


complete_row =\
''' TODO
'''|λ| (lambda r: True)


correct_row_errors =\
''' TODO
'''|λ| (lambda r: r)


fetch_data_from_google_sheet =\
''' TODO
'''|λ| (lambda: Ok(seq.of(*[])))


@curry(1)
def sqlalchemy_persist_table(table: Table, session: Session) -> Result[Session, Exception]:
  try:
    table.create(session.connection().engine)
    return Result.Ok(session)
  except Exception as e:
    return Result.Error(e)


persist_table =\
'''
Table -> Session -> Result[Session, int]

Persist the given table to the database represented
by `session`.
'''|λ|(lambda table:
  |And| sqlalchemy_persist_table(table)
  |And| map_error(lambda _:E_PERSIST_TABLE) )


new_session =\
'''
string -> Result[Session, int]

Produce a new database session.

Side Effects:
Prints progress logs as it attempts and either
succeed/fails to initialize a databse engine,
and subsequently initialize the new session.
''' |λ|(
  try_catch(sa.create_engine)
  |And| map_error(lambda _:E_INIT_ENGINE)
  |And| thenr(try_catch(init_db_session)
             |And| map_error(lambda _:E_INIT_SESSION) ))


insert_rows_into_table =\
''' TODO
'''|λ|(lambda table: lambda rows: lambda session: Ok(8))


close_session =\
''' TODO
'''|λ|(lambda _: Ok(None))

def main():
  return(
    CONNECTION_STRING
    |Pipe_To| new_session
    |Pipe_To| persist_table(VehicleInventoryTable)
    |Then   | (fetch_data_from_google_sheet()
              |Then| (smap(correct_row_errors)
                     |And| filter(complete_row)
                     |And| insert_rows_into_table(VehicleInventoryTable) ))
    |Catch  | (lambda e: ...)
    |Then   | close_session
    |Then   | (lambda _: Ok(1)) )


if __name__ == "__main__":
  main()
