from collections import OrderedDict
from pandas import DataFrame
from dsi.plugins.metadata import StructuredMetadata

class CollectionReader(StructuredMetadata):
    """
    CollectionReader parent class. Keeps information about the data that
    is being ingested, namely its type and structure.
    """
    def __init__(self, collections, **kwargs):
        super().__init__(**kwargs)
        self.input_dict = collections

class Dictionary(CollectionReader):
    """
    A Plugin to capture data from a Dictionary/ Ordered Dictionary

    The input Dictionary/Ordered Dictionary can either represent a single table or multiple tables. 
    If it is multiple tables, each table must be represented as a nested Dictionary/Ordered Dictionary.
    Regardless of the nesting, a table's Dictionary/Ordered Dictionary must contain column names as keys with the respective row data as a list.   
    """
    def __init__(self, collection, table_name = None, **kwargs) -> None:
        super().__init__(collection, **kwargs)
        print(type(self.input_dict))
        if not isinstance(self.input_dict, dict):
            raise TypeError("Input must be a Python dictionary or an Ordered Dictionary")
        self.table_name = table_name

    def add_rows(self) -> None:
        """Reads the input Dictionary/Ordered Dictionary and stores it as either one table or multiple tables in the DSI Abstraction"""

        if not all(isinstance(key, str) for key in self.input_dict.keys()):
            raise TypeError("Keys in the input dictionary must all be strings. Either table or column names.")

        if all(isinstance(val, list) for val in self.input_dict.values()):
            if self.table_name is None:
                raise ValueError("table_name argument must be specified for the single table of data.")
            if not isinstance(self.input_dict, OrderedDict) and isinstance(self.input_dict, dict):
                self.input_dict = OrderedDict(self.input_dict)
            self.set_schema_2(OrderedDict([(self.table_name, self.input_dict)]))

        elif not any(isinstance(val, dict) for val in self.input_dict.values()): # checking if single dict with single values (no nested dicts)
            if self.table_name is None:
                raise ValueError("table_name argument must be specified for the single table of data.")
            temp_dict = OrderedDict()
            for k, v in self.input_dict.items():
                if k not in temp_dict.keys():
                    temp_dict[k] = [v]
                else:
                    temp_dict[k].append(v)
            self.set_schema_2(OrderedDict([(self.table_name, temp_dict)]))

        elif all(isinstance(val, dict) for val in self.input_dict.values()):
            result = OrderedDict()
            for table_name, nested_dict in self.input_dict.items():
                if all(isinstance(v1, list) for v1 in nested_dict.values()):
                    if not isinstance(self.input_dict[table_name], OrderedDict) and isinstance(self.input_dict[table_name], dict):
                        result[table_name] = OrderedDict(self.input_dict[table_name])
                    else:
                        result[table_name] = self.input_dict[table_name]
                else: # values are a mix of scalars, lists, dicts --> treat all values as just one row in the table
                    temp_dict = OrderedDict()
                    for k, v in nested_dict.items():
                        temp_dict[k] = [v]
                    result[table_name] = temp_dict
            self.set_schema_2(result)
        else:
            raise ValueError("Input dictionary must either represent one table of data or multiple tables (use nested Ordered Dicts).")

class Dataframe(CollectionReader):
    """A Plugin to capture data from a Pandas DataFrame that must represent a single table."""
    def __init__(self, collection, table_name, **kwargs) -> None:
        super().__init__(collection, **kwargs)
        if not isinstance(self.input_dict, DataFrame):
            raise TypeError("Input must be a pandas DataFrame")
        self.table_name = table_name

    def add_rows(self) -> None:
        """Reads the input Pandas DataFrame and stores it as one table."""
        try:
            ordered_df = OrderedDict((col, self.input_dict[col].tolist()) for col in self.input_dict.columns)
            self.set_schema_2(OrderedDict([(self.table_name, ordered_df)]))
        except Exception:
            raise ValueError("Error reading in the pandas DataFrame into DSI.")
