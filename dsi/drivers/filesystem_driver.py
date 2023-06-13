"""Abstract class for generic driver interfaces."""

from abc import ABC, abstractmethod
import csv
import os
import sqlite3

class FsStore(ABC):
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
  
    @abstractmethod
    def __init__(self, filename) -> None:
        pass
  
    @abstractmethod
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass
  
    @abstractmethod
    def get_artifacts(self, query):
        pass
