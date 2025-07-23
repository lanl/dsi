from collections import OrderedDict
from dsi.plugins.metadata import StructuredMetadata

class CollectionReader(StructuredMetadata):
    """
    CollectionReader parent class. Keeps information about the data that
    is being ingested, namely its type and structure.
    """
    def __init__(self, collections, **kwargs):
        super().__init__(**kwargs)
        if isinstance(collections, dict):
            self.input_dict = collections
        else:
            return (TypeError, "Input must be a Python dictionary or an Ordered Dictionary")

class Dict(CollectionReader):
    """
    A Plugin to capture data from a Dictionary/ Ordered Dictionary

    The input Dictionary/Ordered Dictionary can either represent a single table or multiple tables. 
    If it is multiple tables, each table must be represented as a nested Dictionary/Ordered Dictionary.
    Regardless of the nesting, a table's Dictionary/Ordered Dictionary must contain column names as keys with the respective row data as a list.   
    """
    def __init__(self, collection, table_name = None, **kwargs) -> None:
        super().__init__(collection, **kwargs)
        self.table_name = table_name

    def add_rows(self) -> None:
        """Reads the input Dictionary/Ordered Dictionary and stores it as either one table or multiple tables in the DSI Abstraction"""

        if not all(isinstance(key, str) for key in self.input_dict.keys()):
            return (TypeError, "Keys in the input dictionary must all be strings. Either table or column names.")

        if all(isinstance(val, list) for val in self.input_dict.values()):
            if self.table_name is None:
                return (ValueError, "table_name argument must be specified to name this single table of data.")
            if not isinstance(self.input_dict, OrderedDict) and isinstance(self.input_dict, dict):
                self.input_dict = OrderedDict(self.input_dict)
            self.set_schema_2(OrderedDict([(self.table_name, self.input_dict)]))

        elif all(isinstance(val, dict) for val in self.input_dict.values()):
            for nested_dict in self.input_dict.values():
                if not all(isinstance(v1, list) for v1 in nested_dict.values()):
                    return (ValueError, "Each value in a nested dictionary / Ordered Dict must be a list.")
            if not isinstance(self.input_dict, OrderedDict) and isinstance(self.input_dict, dict):
                new_input = OrderedDict()
                for k, v in self.input_dict.items():
                    if not isinstance(v, OrderedDict) and isinstance(v, dict):
                        new_input[k] = OrderedDict(v)
                    else:
                        new_input[k] = v
                self.input_dict = new_input
            self.set_schema_2(self.input_dict)
        else:
            return (ValueError, "Input dictionary must either represent one table of data or multiple tables (use nested Ordered Dicts).")