from collections import OrderedDict
from dsi.plugins.metadata import StructuredMetadata

class CollectionReader(StructuredMetadata):
    """
    FileReader Plugins keep information about the file that
    they are ingesting, namely absolute path and hash.
    """
    def __init__(self, collections, **kwargs):
        super().__init__(**kwargs)
        if type(collections) == OrderedDict:
            self.input_dict = collections
        else:
            return (TypeError, "Input must be an Ordered Dictionary")

class Dict(CollectionReader):
    """
    A Plugin to capture data from an Ordered Dictionary

    The input Ordered Dictionary can either represent a single table or multiple tables. 
    If it is multiple tables, each table must be represented as a nested Ordered Dictionary.
    Regardless of the nesting, a table's Ordered Dictionary must contain column names as keys with the respective row data as a list.   
    """
    def __init__(self, collection, table_name = None, **kwargs) -> None:
        super().__init__(collection, **kwargs)
        self.table_name = table_name

    def add_rows(self) -> None:
        """Reads the input Ordered Dictionary and stores it as either one table or multiple tables in the DSI Abstraction"""

        if not all(isinstance(key, str) for key in self.input_dict.keys()):
            return (TypeError, "Keys in the input Ordered Dict must all be strings. Either table or column names.")

        if all(isinstance(val, list) for val in self.input_dict.values()):
            if self.table_name is None:
                return (ValueError, "table_name argument must be specified to name this single table of data.")
            self.set_schema_2(OrderedDict([(self.table_name, self.input_dict)]))

        elif all(isinstance(val, OrderedDict) for val in self.input_dict.values()):
            for nested_dict in self.input_dict.values():
                if not all(isinstance(v1, list) for v1 in nested_dict.values()):
                    return (ValueError, "Each value in a nested Ordered Dict must be a list.")
            self.set_schema_2(self.input_dict)
        else:
            return (ValueError, "Input Ordered Dict must either represent one table of data or multiple tables (use nested Ordered Dicts).")