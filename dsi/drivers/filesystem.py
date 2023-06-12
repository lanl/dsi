import os
import sqlite3
import csv
from abc import (
  ABC,
  abstractmethod,
)

class fsstore(ABC):
  # Declare named types for sql
  DOUBLE = "DOUBLE"
  STRING = "VARCHAR"
  FLOAT = "FLOAT"
  INT = "INT"

  #Declare store types
  GUFI_STORE = "gufi"
  SQLITE_STORE = "sqlite"

  # Declare comparison types for sql
  GT = ">"
  LT = "<"
  EQ = "="

  def __init__(self, storetype):
    if storetype not in [self.GUFI_STORE, self.SQLITE_STORE]:
      storetype = SQLITE_STORE

    self.storetype = storetype

  @abstractmethod
  # Query file name
  def query_fname(self, name ):
    pass


  # Query file size
  def query_fsize(self, operator, size ):
    pass

  # Query file creation time
  def query_fctime(self, operator, size ):
    pass
