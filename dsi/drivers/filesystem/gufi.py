import os
import sqlite3
import csv
import subprocess
from dsi.drivers.filesystem_driver import fsstore

from abc import (
  ABC,
  abstractmethod,
)

# Holds table name and data properties 
class data_type:
    name = "DEFAULT"
    properties = {}
    units = {}

class store(fsstore):
    #GUFI Entries table
    #CREATE TABLE entries(name TEXT, type TEXT, inode INT64, mode INT64, nlink INT64, uid INT64, gid INT64, size INT64, blksize INT64, blocks INT64, atime INT64, mtime INT64, ctime INT64, linkname TEXT, xattr_names BLOB, crtime INT64, ossint1 INT64, ossint2 INT64, ossint3 INT64, ossint4 INT64, osstext1 TEXT, osstext2 TEXT);

    #Prefix to GUFI commands
    prefix = ""
    #Index we are querying
    index = ""
    isVerbose = 0

    def __init__(self, prefix, index):
        super().__init__(fsstore.GUFI_STORE)
        self.prefix = prefix
        self.index = index

    # Query file name
    def query_fname(self, name ):
        str_query = "SELECT * FROM entries WHERE name LIKE '%" + str(name) +"%'"
        if self.isVerbose:
            print(str_query)

        resout = self._run_gufi_query(str_query)
        if self.isVerbose:
            print(resout)

        return resout

    # Query file size
    def query_fsize(self, operator, size ):
        str_query = "SELECT * FROM entries where size " + str(operator) + " " + str(size)
        print(str_query)
        if self.isVerbose:
            print(str_query)

        resout = self._run_gufi_query(str_query)
        if self.isVerbose:
            print(resout)

        return resout

    # Query file creation time
    def query_fctime(self, operator, size ):
        str_query = "SELECT * FROM entries where ctime " + str(operator) + " " + str(size)
        if self.isVerbose:
            print(str_query)

        resout = self._run_gufi_query(str_query)
        if self.isVerbose:
            print(resout)

        return resout

    def _run_gufi_query(self, sqlstring):
        result = subprocess.run([self.prefix + "gufi_query", "-d", "|", "-E", sqlstring, self.index], capture_output=True, text=True)
        return result.stdout
