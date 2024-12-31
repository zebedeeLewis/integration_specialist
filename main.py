#!/usr/bin/env python
from __future__ import annotations

import pdb
import re
import os
import os.path
import textwrap
from typing import Callable
from functools import reduce

from expression import result, flip, effect, Result, Ok, Error, curry, compose, identity, Some, Nothing
from expression.collections import Seq, Block, block
from pydantic import BaseModel
from dotenv import load_dotenv

import pyodbc
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import (
  DeclarativeBase, sessionmaker, Session, mapped_column, MappedAsDataclass, Mapped)
from sqlalchemy import select, text as as_text, Engine, Integer, String, Identity, Table
from sqlalchemy.dialects.mssql import TINYINT, VARCHAR, SMALLINT, MONEY
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


load_dotenv()


index = block.item
choose = block.choose
empty = block.empty
shorten = lambda count: lambda s: textwrap.shorten(s, break_long_words=False, width=count)
then = lambda fn: lambda xs: xs.bind(fn)
map = lambda fn: lambda xs: xs.map(fn)
filter = lambda fn: lambda xs: xs.filter(fn)
map_error = lambda fn: lambda xs: xs.map_error(fn)
rswap = result.swap


E_PERSIST_TABLE = 1
E_INIT_ENGINE = 2
E_INIT_SESSION = 3
E_GOOGLE_CREDENTIALS_FILE = 4
E_BUILD_GOOGLE_SERVICE = 5
E_FETCH_GOOGLE_DATA = 6

E_ODBC_TABLE_EXISTS = 'f405'


def lazy(val):
    return lambda _ = None: val

class Infix:
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
Spread = Infix(lambda sequence, fn:
  fn(**sequence) if isinstance(sequence, dict) else fn(*sequence))
λ = Infix(add_doc)
Apply = Infix(lambda monad1, monad_fn: monad_fn.bind(lambda fn: monad1.map(fn)))


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
      then(lambda r: s\
        |Pipe_To| Log.from_str
        |Pipe_To| Log.format("info")
        |Pipe_To| Log.write
        |Pipe_To| map(lambda _: r))


  @staticmethod
  def error_message(s: str) -> Callable[[Result], Result]:
    return\
      map_error(lambda r: s\
        |Pipe_To| Log.from_str
        |Pipe_To| Log.format("error")
        |Pipe_To| Log.write
        |Pipe_To| then(lambda _: r |Pipe_To| Error) )


  def warning_message(s: str) -> Callable[[Result], Result]:
    return\
      then(lambda r: s\
        |Pipe_To| Log.from_str
        |Pipe_To| Log.format("warning")
        |Pipe_To| Log.write
        |Pipe_To| map(lambda _: r) )


# Result[A, E] ->  Result[A, E]
Log.exception = \
  map_error(
   str
   |And| Log.from_str
   |And| Log.format("error")
   |And| Log.write
   |And| then(lambda _: Error(None)) )


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
def sqlalchemy_persist_item(item, session):
  logs = empty
  try:
    session.add(item)
    session.commit()
  except IntegrityError as e:
    session.rollback()
    session.commit()

    logs = logs.cons(
      'Possible duplicate found while writing item to database'
      |Pipe_To| Log.from_str
      |Pipe_To| Log.format("warning")
    ).cons(
      str(item)
      |Pipe_To| shorten(74)
      |Pipe_To| Log.from_str
      |Pipe_To| Log.format("warning")
    ).cons(
      str(e)
      |Pipe_To| shorten(74)
      |Pipe_To| Log.from_str
      |Pipe_To| Log.format("warning")
    )

    ret = [logs, item, e] |Pipe_To| Block |Pipe_To| Ok
  except Exception as e:
    logs = logs.cons(
      'While writing item to database'
      |Pipe_To| Log.from_str
      |Pipe_To| Log.format("error")
    )
    ret = [logs, item, e] |Pipe_To| Block |Pipe_To| Ok
  finally:
    logs = logs.cons(
      'Successfuly wrote item to database'
      |Pipe_To| Log.from_str
      |Pipe_To| Log.format("info")
    )
    ret = [logs, item, session] |Pipe_To| Block |Pipe_To| Ok
    
  return ret


init_db_session = lambda engine:\
  sessionmaker(engine)()


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


pad_row =\
''' Iterable[string] -> Iterable[string]

pad the end of the array with empty strings from the last element
to the 17th element.
'''|λ| (lambda r: [
    list_get(r, i) for i in range(17)])


apply_transformations =\
''' Iterable[string] -> Iterable[string]
'''|λ| (lambda transforms: lambda r: [
    reduce((lambda acc, fn: fn(acc)) ,transforms.get(i, [lambda _:_]) + transforms[-1], c)
    for i, c in enumerate(r)])


load_google_credentials_from_file =\
''' string -> Result[GoogleCredentials, Exception]

Load credentials used to access google api from the given file.
'''|λ| try_catch(lambda filename:
  service_account.Credentials.from_service_account_file(filename, scopes=SCOPES) )


build_google_service_using_credentials =\
''' GoogleCredentials -> Result[ServiceAccount, Exception]
'''|λ| try_catch(lambda creds: build("sheets", "v4", credentials=creds))


fetch_data_from_google_sheet =\
''' string -> string -> GoogleService -> Result[List[Tuple]], Error]

Fetch the set of cells selected by `range_str` from the google sheet
with the given `sheet_id`, returning the result as a list of tuples
where each tuple contains the data for a single row.
'''|λ| curry(1)(lambda sheet_id, range_str:
  try_catch(lambda service:
    service.spreadsheets()
           .values()
           .get(spreadsheetId=sheet_id, range=range_str)
           .execute()
           .get("values", []) ))


fetch_data_from_source =\
''' () -> Result[Block, Exception]

Authenticate and setup the google api, then fetch and return the
inventory data found in the google sheets document.
'''|λ| (lambda: 
  SERVICE_ACCOUNT_FILE
  |Pipe_To| load_google_credentials_from_file
  |Catch  | lazy(Error(E_BUILD_GOOGLE_SERVICE))
  |Then   | build_google_service_using_credentials
  |Catch  | lazy(Error(E_GOOGLE_CREDENTIALS_FILE))
  |Then   | (RANGE |Pipe_To| fetch_data_from_google_sheet(SPREADSHEET_ID))
  |Catch  | lazy(Error(E_FETCH_GOOGLE_DATA))
  |Map    | Block )


@curry(1)
def sqlalchemy_persist_table(table: Table, session: Session) -> Result[Session, Exception]:
  try:
    table.create(session.connection().engine)
    return Result.Ok(session)
  except Exception as e:
    return Result.Error(e)


persist_table =\
''' Table -> Session -> Result[Session, int]

Persist the given table to the database represented
by `session`.
'''|λ|(lambda table:
  sqlalchemy_persist_table(table)
  |And| map_error(lazy(E_PERSIST_TABLE) ))


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
  |And| then(try_catch(init_db_session)
             |And| map_error(lambda _:E_INIT_SESSION) ))


apply_actions_to_session =\
'''Block[ Callable[[Sesssion]
        , Result[ (Block[Log], VehicleInventory, Session)
                , (Block[Log], VehicleInventory, Exception)]]]
-> Block[(Block[Log], VehicleInventory, Session), (Block[Log], VehicleInventory, Exception)]
'''|λ|(lambda xs: lambda s: xs |Map| (lambda x: x(s).merge()))


persist_inventory_item =\
'''VehicleInventory
-> Session
-> Result[(Block[Log], VehicleInventory, Session), (Block[Log], VehicleInventory, Exception)]
'''|λ|(lambda r: sqlalchemy_persist_item(r))


new_inventory_item =\
''' 
'''|λ|(lambda r: VehicleInventory(*r))


close_session =\
''' TODO
'''|λ|(lambda _: Ok(None))


copy_data_from_source_to_database =\
'''Session
-> Result[ Block[ (Block[Log], VehicleInventory, Session)
                , (Block[Log], VehicleInventory, Exception)]
         , int]
moves the data from the source to database
'''|λ|(fetch_data_from_source()
       |Map| map(pad_row
                |And| apply_transformations(TRANSFORMS)
                |And| new_inventory_item
                |And| persist_inventory_item )
       |Map| apply_actions_to_session)


def main():
  return(
    CONNECTION_STRING
    |Pipe_To| new_session
    |Then   | persist_table(VehicleInventoryTable)
    |Apply  | copy_data_from_source_to_database
    |Map    | map(lambda t:
                   t |Pipe_To| index(0)
                     |Pipe_To| map(Log.write)
                     |Pipe_To| lazy(index(2)(t)) )
    |Map    | choose(lambda x: Some(x) if isinstance(x, Session) else Nothing)
    |Map    | (lambda x: set(x.dict()).pop())
    |Catch  | (lambda e: e |Pipe_To| Ok)
    |Then   | close_session
    |Then   | (lambda e: e |Pipe_To| Ok) )


if __name__ == "__main__":
  main()
