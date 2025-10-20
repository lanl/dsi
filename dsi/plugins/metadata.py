from collections import OrderedDict
from dsi.plugins.plugin import Plugin
import inspect

class StructuredMetadata(Plugin):
    """ plugin superclass that provides handy methods for structured data """
    git_commit_sha: str = '5d79e08d4a6c1570ceb47cdd61d2259505c05de9'

    def __init__(self, **kwargs):
        """
        Initializes StructuredMetadata class with an output collector (Ordered Dictionary)
        """
        self.output_collector = OrderedDict()
        self.table_cnt = None # schema not set until pack_header
        self.validation_model = None  # optional pydantic Model
        # Check for strict_mode option
        if 'strict_mode' in kwargs:
            if type(kwargs['strict_mode']) == bool:
                self.strict_mode = kwargs['strict_mode']
            else:
                print('strict_mode must be bool type.')
                raise TypeError
        else:
            self.strict_mode = False
        # Lock to enforce strict mode
        self.strict_mode_lock = False

    def set_schema(self, table_data: list, validation_model=None) -> None:
        """
        **Old function to be deprecated soon. Do not use.**

        Initializes column structure in the output_collector and table_cnt.

        This method is typically used within a plugin `pack_header()` method to define
        the expected schema before rows are added.

        `table_data` : list
            Defines the table structure to be used.

            - For multple-table data:
                A list of tuples, each structured as (table_name, list_of_column_names)

            - For single-table data:
                A simple list of column names.
        """
        # Strict mode | SMLock | relation
        # --------------------------------
        # 0 | 0 | Proceed, no lock
        # 0 | 1 | Raise error. Nonsense.
        # 1 | 0 | Proceed, then lock
        # 1 | 1 | Raise error. Previously locked.
        if self.strict_mode and self.strict_mode_lock:
            print('Previously locked schema. Refusing to proceed.')
            raise RuntimeError
        if not self.strict_mode and self.strict_mode_lock:
            print('Strict mode disabled but strict more lock active.')
            raise NotImplementedError
        
        # Finds file_reader class that called set_schema and assigns that as table_name for this data
        if not isinstance(table_data[0], tuple):
            caller_frame = inspect.stack()[1]
            tableName = caller_frame.frame.f_locals.get('self', None).__class__.__name__
            table_data = [(tableName, table_data)]

        for name in table_data:
            eachTableDict = OrderedDict((key, []) for key in name[1])
            self.output_collector[name[0]] = eachTableDict
        self.table_cnt = len(table_data)
        self.validation_model = validation_model

        if not self.strict_mode_lock:
            self.strict_mode_lock = True

    def set_schema_2(self, collection, validation_model=None) -> None:
        """
        **Use this if data in a Reader's ``add_rows()`` is structured as an OrderedDict()**

        Faster update of the DSI abstraction when input data is structured as an OrderedDict.

        This method is optimized for plugins where `add_rows()` passes data as an OrderedDict, and avoids
        the incremental row ingest via `set_schema()` and ``add_to_output()``.

        `collection` : OrderedDict
            The plugin's data structure (with data) passed from a plugin.

            - If collection only contains one table, the data will be wrapped in another OrderedDict,
              where the plugin's class name is the table name key.
        """
        # Finds file_reader class that called set_schema and assigns that as table_name for this data
        if not isinstance(collection[next(iter(collection))], OrderedDict):
            caller_frame = inspect.stack()[1]
            tableName = caller_frame.frame.f_locals.get('self', None).__class__.__name__
            self.output_collector[tableName] = collection
        else:
            self.output_collector = collection
        self.table_cnt = len(collection.keys())
        self.validation_model = validation_model

    def add_to_output(self, row: list, tableName = None) -> None:
        """
        **Old function to be deprecated soon. Do not use.**

        Adds a row of data to the output_collector with enforced structure.
        
        This method is typically used within a plugin's `add_rows()` method to incrementally
        build table output in a consistent format.

        `row` : list
            A single row of data to be added. Must match the expected structure for the target table.

        `tableName` : str, optional
            Name of the table to which the row should be added. 
            If None, the function identifies which plugin called it and assigns tableName for that data
        """        
        # Finds file_reader class that called add_to_output and assigns that as table_name for this data
        if tableName == None:
            caller_frame = inspect.stack()[1]
            tableName = caller_frame.frame.f_locals.get('self', None).__class__.__name__

        if not self.schema_is_set():
            raise RuntimeError("pack_header must be done before add_row")
        if self.validation_model is not None:
            row_dict = {k: v for k, v in zip(self.output_collector[tableName].keys(), row)}
            self.validation_model.model_validate(row_dict)

        elif len(row) != len(self.output_collector[tableName].keys()):
            raise RuntimeError(f"For {tableName}, incorrect number of values was given")
        
        for key, row_elem in zip(self.output_collector[tableName].keys(), row):
            if "dsi_units" != tableName:
                self.output_collector[tableName][key].append(row_elem)
            else:
                self.output_collector[tableName][key] = row_elem

    def schema_is_set(self) -> bool:
        """
        **Old function to be deprecated soon. Do not use.**

        Helper method to see if the schema has been set 
        """
        return self.table_cnt is not None
