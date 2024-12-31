#!/usr/bin/env python
from __future__ import annotations

import pdb
import os
from pathlib import Path
from typing import Callable, Any, NamedTuple
from functools import reduce

from expression import (
  result, option, effect, Option, Result, Ok, Error, curry,
  compose, identity, Some, Nothing)
from expression.collections import Block, block
from pydantic import BaseModel
from dotenv import load_dotenv

import pyodbc
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm import (
  DeclarativeBase, sessionmaker as _sessionmaker, Session,
  mapped_column, MappedAsDataclass, Mapped)
from sqlalchemy import Integer, String, Identity
from sqlalchemy.dialects.mssql import TINYINT, VARCHAR, SMALLINT, MONEY
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build, Resource

"""
########################################################################
#
# Utility DEFINITIONS
#
########################################################################
"""

flip = lambda fn: lambda *args: lambda *argsb: fn(*argsb)(*args)
Table = sa.FromClause
Cons = Block
cons = curry(1)(block.cons)
then = lambda fn: lambda xs: xs.bind(fn)
map = lambda fn: lambda xs: xs.map(fn)
filter = lambda fn: lambda xs: xs.filter(fn)
catch_ = lambda fn: lambda xs: xs.map_error(fn)
ap = lambda fa: lambda fab: fa.bind(lambda a: fab.map(lambda ab: ab(a)))
apply = lambda a: lambda ab: ab(a)
option_to_result = lambda e: lambda oa: result.of_option(oa, e)
ꓸꓸꓸ = lambda fn: lambda collection:(
  fn(**collection) if isinstance(collection, dict) else fn(*collection))
fold = lambda fn, initial: lambda foldable: foldable.fold(fn, initial)
sessionmaker = flip(_sessionmaker)


class Infix:
  def __init__(self, function):
    self.operator_fn = function

  def __ror__(self, other):
    return Infix(self.operator_fn(other))

  def __or__(self, other):
    return self.operator_fn(other)

  def __rlshift__(self, fn1):
    return Infix(lambda fn2: compose(fn1, flip(self.operator_fn)(fn2)))

  def __rshift__(self, fn2):
    return self.operator_fn(fn2)

  def __call__(self, value1):
    return lambda value2: self.function(value1, value2)


def add_doc(doc, fn):
  fn.__doc__ = doc
  return fn


O = Infix(lambda x: lambda fn: fn(x))
T = Infix(lambda monad: lambda fn: monad.bind(fn))
C = Infix(lambda monad: lambda fn: monad.map_error(fn))
M = Infix(lambda functor: lambda fn: functor.map(fn))


def debug(x):
  pdb.set_trace()
  return x


def ꓸ[T](val: T) -> Callable[[Any], T]:
  """Produce a function that returns `val` when called."""
  def _ret(_: Any = None) -> T:
    return val

  return _ret


def try_[A, B](fn: Callable[[A], B]) -> Callable[[A], Result[B,Exception]]:
  """Execute the given unary function, wrapping the result in Reslut.Ok.
  Ces and wraps any exciption in a Result.Error.
  Callable[[], A] -> Result[A,Exception]

  @param fn: a unary function to execute.
  """
  def _fn(a):
    try:
      return fn(a)|O|Ok
    except Exception as e:
      return e|O|Error

  return _fn


def list_get(l, i):
  try:
    return l[i]
  except:
    return ''


"""
########################################################################
#
# CONSTANT DEFINITIONS
#
########################################################################
"""

E_PERSIST_TABLE = 1
E_INIT_ENGINE = 2
E_INIT_SESSION = 3
E_GOOGLE_CREDENTIALS_FILEPATH = 4
E_INIT_GOOGLE_SERVICE = 5
E_FETCH_GOOGLE_DATA = 6
E_TABLE_MISSING = 7
E_SESSION_MISSING = 8
E_SHEET_ID_MISSING = 9
E_SHEET_CELLRANGE_MISSING = 10
E_API_MISSING = 11
E_RECORDS_MISSING = 12 
E_PERSIST_ITEM = 13


"""
########################################################################
#
# DATA DEFINITIONS
#
########################################################################
"""

class Base(MappedAsDataclass, DeclarativeBase): pass


class VehicleInventory(Base):
  __tablename__    = "vehicle_inventory"

  vin              : Mapped[str] = mapped_column(VARCHAR(17), unique=True,
                                                 nullable=False)
  make             : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  model            : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  year             : Mapped[int] = mapped_column(SMALLINT, nullable=False)
  mileage          : Mapped[int] = mapped_column(Integer, nullable=False)
  price            : Mapped[str] = mapped_column(MONEY, nullable=False)
  condition        : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  color            : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  interior_color   : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  engine           : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  transmission     : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  drive_train      : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  fuel_type        : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  body_style       : Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
  number_of_seats  : Mapped[int] = mapped_column(TINYINT, nullable=False)
  doors            : Mapped[int] = mapped_column(TINYINT, nullable=False)
  stock_number     : Mapped[int] = mapped_column(Integer, Identity(), init=False,
                                                 nullable=False, primary_key=True)
  notes            : Mapped[str] = mapped_column(VARCHAR(256))


def set_vehicle_inventory_col(
  colname: str, val: str, r: VehicleInventory
  ) -> VehicleInventory:
  r.__setattr__(colname, val)
  return r

VehicleInventoryTable = VehicleInventory.__table__


TRANSFORMATIONS = {
  VehicleInventory.mileage: [
    lambda s: s.strip().replace(',', ''),
  ],
  VehicleInventory.transmission: [
    lambda s: s.replace('CVT', 'Automatic'),
  ],
  VehicleInventory.drive_train: [
    lambda s: s.replace('Four-Wheel Drive', '4WD'),
    lambda s: s.replace('All-Wheel Drive', 'AWD'),
    lambda s: s.replace('Rear-Wheel Drive', 'RWD'),
    lambda s: s.replace('Front-Wheel Drive', 'FWD'),
  ],
  VehicleInventory.fuel_type: [
    lambda s: s.replace('Plug-in Hybrid', 'Hybrid'),
  ],
  VehicleInventory.body_style: [
    lambda s: s.replace('Pickup Truck', 'Pickup'),
  ],
  VehicleInventory.stock_number: [
    lambda _: None,
  ],
}


class Log(BaseModel):
  color: str = ''
  prefix: str = ''
  message: str = ''


type HttpGetVehicleInventory = Callable[[str], Result[list[tuple[str]], Exception]]
type DBCreateVehicleInventoryItem = Callable[
  [VehicleInventory], Result[Option[VehicleInventory], int]]
type DBCreateTable = Callable[
  [Table], Result[Table, int]]


class DBDriver(NamedTuple):
  create_item: DBCreateVehicleInventoryItem
  create_table: DBCreateTable


class UninitializedProgram[T](NamedTuple):
  logs: Cons[Log]
  db_driver: Option[DBDriver]
  spreadsheet_api_get: Option[HttpGetVehicleInventory]
  maybe_result: Option[T]

null_program = UninitializedProgram(
  Cons.empty(), Nothing, Nothing, Nothing)


class Program[T](NamedTuple):
  """Describes the program state at each step of it's execution
  
  @prop logs: All logs generated up to the current state of the program.
  @prop db_driver: Database driver used to interact with the database.
  @prop spreadsheet_api_get: function that takes a cell range and returns a
    list of tuples where each tuple is a row from the spreadsheet.
  @prop maybe_result: Result of the last step of the program execution.
  
  examples:
    >>> p1: Program[str] = Program(
    ...   Cons.empty(), Some(db_driver), Nothing, Some(api), Some('current state'))
    >>> p2: Program[int] = Program(
    ...   Cons.empty(), Some(db_driver), Some(VehiclTable), Nothing, Some(6))
    >>> p3: Program[int] = Program(
    ...   Cons.empty(), Nothing, Nothing, Some(api), Nothing)
  """
  logs: Cons[Log]
  db_driver: DBDriver
  spreadsheet_api_get: HttpGetVehicleInventory
  maybe_result: Option[T]


set_program_db_driver = lambda v: lambda p: p.__class__(
  p.logs, v, p.spreadsheet_api_get, p.maybe_result)
get_program_db_driver = lambda p: p.db_driver
set_program_api = lambda v: lambda p: p.__class__(
  p.logs, p.db_driver, v, p.maybe_result)
get_program_api = lambda p: p.spreadsheet_api_get
set_program_result = lambda v: lambda p: p.__class__(
  p.logs, p.db_driver, p.spreadsheet_api_get, v)
get_program_result = lambda p: p.maybe_result


class ApiConfig(NamedTuple):
  """interp. Connection configuration for a google sheets api

  @prop access_scope:
    A collection of U{google Oauth 2.0 api scopes<https://bit.ly/4gIEBXg>}.

  @prop credentials_filepath:
    Path to the google credentials file on the filesystem.

  @prop sheet_id:
    The unique identifier for the google sheet containing the data
    (U{see here<https://bit.ly/4jdgW2I>}).

  examples:
    >>> from pathlib import Path
    >>>
    >>> gc1: ApiConfig = ApiConfig(
    ... ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    ... Path('./credentials.json'),
    ... 1iCXpLbphvexUFHDuYs3VSKQW8gqIXAWCEaNMv_1rxcI,
    ... )
  """
  access_scope: list[str]
  credentials_filepath: Path
  sheet_id: str

set_api_config_access_scope = lambda v: lambda c: ApiConfig(
  v, c.credentials_filepath, c.sheet_id)
api_config_access_scope = lambda c: c.access_scope


class DBConfig(NamedTuple):
  """interp. Configuration needed to compose a database connection string.

  @prop address: IP address of the database server.
  @prop name: The name of the database where data will be stored.
  @prop username: The username to use for database commands.
  @prop password: the password associated with the given `username`.

  examples:
    >>> dbc1: DBConfig = DBConfig(
    ...   '192.168.0.45', 'inventory_table', 'bob', 'secretPassword')
  """
  address: str
  name: str
  username: str
  password: str 


"""
########################################################################
#
# FUNCTION DEFINITIONS
#
########################################################################
"""

def db_url(db_config: DBConfig) -> str:
  """Produce connection string from the given db_config."""
  return (
    f'mssql+pyodbc://{db_config[2]}:{db_config[3]}@{db_config[0]}/{db_config[1]}'
    f'?driver={pyodbc.drivers()[0].replace(" ", "+")}'
     '&TrustServerCertificate=no&encrypt=no')


def data_source_api(
    api_config: ApiConfig
  ) -> Result[Option[HttpGetVehicleInventory], Option[int]]:
  """Produce the api object used to interact with the data source."""
  load_credentials = lambda _:Credentials.from_service_account_file(
    api_config.credentials_filepath, scopes=api_config.access_scope)

  build_sheets_v4_api = lambda creds: build(
    "sheets", "v4", credentials=creds).spreadsheets().values()

  get_cellrange = lambda r: try_(lambda cellrange: r.get(
    spreadsheetId=api_config.sheet_id, range=cellrange
  ).execute().get("values",[]))

  return (
    None
    |O| try_(load_credentials)
        <<C>>ꓸ(E_GOOGLE_CREDENTIALS_FILEPATH|O|Some)
    |T| try_(build_sheets_v4_api)
        <<C>>ꓸ(E_INIT_GOOGLE_SERVICE|O|Some)
    |M| get_cellrange<<O>>Some )


def create_table(
  p: Program[Resource]
  ) -> Result[Program[Any], Program[int]]:
  """Store the given table to the database represented by `session`. """
  return(
    p.db_driver.create_table(VehicleInventoryTable)
    |M| set_program_result<<O>>apply(p)
    |C| set_program_result<<O>>apply(p) )


@curry(1)
def fetch_data_from_source(
    cellrange: str, p: Program[Any]
  ) -> Result[Program[Cons[tuple]], Program[int]]:
  """Fetch the vehicle inventory records from the google sheet"""

  return (
    p.spreadsheet_api_get(cellrange)
    |M| Cons.of_seq
        <<O>>Some<<O>>set_program_result<<O>> apply(p)
    |C|ꓸ(E_FETCH_GOOGLE_DATA)
        <<O>>Some<<O>>set_program_result<<O>>apply(p) )


@curry(1)
def db_driver(config: DBConfig) -> Result[DBDriver, int]:
  return(
    config
    |O| db_url
    |O| try_(sa.create_engine)
        <<C>>ꓸ(E_INIT_ENGINE)
    |T| try_(sessionmaker())
        <<C>>ꓸ(E_INIT_SESSION)
    |M| (lambda session: DBDriver(
          sqlalchemy_create_item(session),
          sqlalchemy_create_table(session) )))


@curry(1)
def sqlalchemy_create_table(
    session: Session, table: Table
  ) -> Result[Table, int]:
  return (
    None
    |O| try_(lambda _: table.create(session.connection().engine))
    |M|ꓸ(table )
    |C|ꓸ(E_PERSIST_TABLE) )


@curry(1)
def sqlalchemy_create_item(
    session: Session, item: VehicleInventory
  ) -> Result[Option[VehicleInventory], int]:
  try:
    session.add(item)
    session.commit()
  except IntegrityError as e:
    session.rollback()
    session.commit()
    ret = item|O|Some|O|Ok
  except Exception as e:
    ret = E_PERSIST_ITEM|O|Error
  else:
    ret = Nothing|O|Ok
    
  return ret


@curry(1)
def transform_col(
    record: VehicleInventory, col: InstrumentedAttribute
  ) -> VehicleInventory:
  return set_vehicle_inventory_col(col.name, reduce(
    (lambda val, fn: fn(val)),
    TRANSFORMATIONS[col],
    record.__getattribute__(col.name) ), record)


def apply_transformations(
    record: VehicleInventory
  ) -> VehicleInventory:
  return reduce(
    (lambda r, col: transform_col(r)(col)),
    TRANSFORMATIONS.keys(),
    record )


def write_data_to_database(
    p: Program[list[tuple]]
  ) -> Result[Program[Cons[VehicleInventory]], Program[int]]:
  """Write the data fetched from the google sheet to the database."""
  pad_row = lambda r: [list_get(r, i) for i in range(17)]
  write_item_to_database = lambda failed_writes_or_error, record_in: (
    failed_writes_or_error
      |T|ꓸ(record_in)
          <<O>> pad_row
          <<O>> (VehicleInventory|O|ꓸꓸꓸ)
          <<O>> apply_transformations
          <<O>> p.db_driver.create_item
          <<T>> (option_to_result(None)
                 <<T>> (cons<<O>>Ok<<O>>ap(failed_writes_or_error))
                 <<O>> result.or_else(failed_writes_or_error) ))

  return (
    p.maybe_result
    |O| option_to_result(Cons.empty())
        <<C>>ꓸ(E_RECORDS_MISSING)
    |T| fold(write_item_to_database, Cons.empty()|O|Ok)
    |M| Some<<O>>set_program_result<<O>>apply(p)
    |C| Some<<O>>set_program_result<<O>>apply(p) )


def write_logs(
    p: Program[Any]
  ) -> Result[Program[Any], Program[int]]:
  """TODO!!!
  """
  return p|O|Ok


def init_program(
    api_config: ApiConfig, db_config: DBConfig
) -> Result[Program, int]:
  return (
    db_driver(db_config)()
    |M| Some
        <<O>>set_program_db_driver
        <<O>>apply(null_program)
    |M| flip(set_program_api)
    |O| ap(data_source_api(api_config))
    |M| (lambda p: Program(
          p.logs, p.db_driver.value, p.spreadsheet_api_get.value,
          p.maybe_result) ))


def close_session(
    p: Program[Any]
  ) -> Program[Any]:
  """TODO!!!
  """
  return p


def main(
    api_config: ApiConfig, db_config: DBConfig, cellrange: str
  ) -> int:
  return(
    init_program(api_config, db_config)
    |T| create_table
    |T| fetch_data_from_source(cellrange)
    |T| write_data_to_database
    |T| write_logs
    |O| result.merge
    |O| close_session
    |O| get_program_result )


if __name__ == "__main__":
  load_dotenv()

  exit(main(
    ApiConfig(
      ['https://www.googleapis.com/auth/spreadsheets.readonly'],
      Path('./credentials.json'),
      os.getenv("GOOGLE_SHEET_ID", ""),
    ),
    DBConfig(
      os.getenv("DB_ADDRESS", ""),
      os.getenv("DB_NAME", ""),
      os.getenv("DB_USER", ""),
      os.getenv("DB_PASSWORD", "")
    ),
    os.getenv("GOOGLE_SHEET_RANGE", "")
  ))
