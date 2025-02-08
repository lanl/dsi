from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json
from math import isnan
from pandas import DataFrame, read_csv, concat

from dsi.plugins.metadata import StructuredMetadata


class CollectionReader(StructuredMetadata):
    """
    FileReader Plugins keep information about the file that
    they are ingesting, namely absolute path and hash.
    """

    def __init__(self, collections, filenames, **kwargs):
        super().__init__(**kwargs)
        if type(collections) == dict:
                 self.collections = [collections]
        elif type(filenames) == list:
                 self.collections = collections
        else:
                 raise TypeError

class Dict(CollectionReader):
    """
    A Plugin to capture data from a dicitonary

    The dictionary's data keys are used as columns and values are rows
   
    """

    def __init__(self, collections, **kwargs) -> None:
        super().__init__(collections, **kwargs)
        self.key_data = []
        self.base_dict = OrderedDict()
        
    def pack_header(self) -> None:
        """Set schema with POSIX and JSON data."""
        self.set_schema(self.key_data)

    def add_rows(self) -> None:
        """Reads the dictionary data and adds a list containing 1 or more rows."""

        objs = []
        for idx, collection in enumerate(self.collections):
            objs.append(collection)
            for key, val in collection.items():
                # Check if column already exists
                if key not in self.key_data:
                    self.key_data.append(key)
        
        if not self.schema_is_set():
            self.pack_header()
            for key in self.key_data:
                self.base_dict[key] = None

        for o in objs:
            new_row = self.base_dict.copy()
            for key, val in o.items():
                new_row[key] = val
            print(new_row.values())
            self.add_to_output(list(new_row.values()))

