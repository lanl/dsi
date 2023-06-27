from abc import ABCMeta, abstractmethod
import csv
import os
import sqlite3

class Driver(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, filename) -> None:
        pass

    @property
    @abstractmethod
    def git_commit_sha(self):
        pass
 
    @abstractmethod
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass
  
    @abstractmethod
    def get_artifacts(self, query):
        pass
  
    @abstractmethod
    def inspect_artifacts(self):
        pass

class Filesystem(Driver):
    git_commit_sha='46881994cbab234a66ff47411a1d27e4bc442007'
    # Declare named types
    DOUBLE = "DOUBLE"
    STRING = "VARCHAR"
    FLOAT = "FLOAT"
    INT = "INT"
  
    #Declare store types
    GUFI_STORE = "gufi"
    SQLITE_STORE = "sqlite"
    PARQUET_STORE = "parquet"
  
    # Declare comparison types
    GT = ">"
    LT = "<"
    EQ = "="
  
    def __init__(self, filename) -> None:
        pass
  
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass
  
    def get_artifacts(self, query):
        pass
  
    def inspect_artifacts(self):
        pass
