"""Abstract class for generic driver interfaces."""

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

class Filesystem(Driver):
    git_commit_sha='7f7386d90ba8ebf58cb9443105c96a4c0a1dfb42'
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
  
    def __init__(self, filename) -> None:
        pass
  
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass
  
    def get_artifacts(self, query):
        pass
