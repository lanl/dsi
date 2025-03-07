from collections import OrderedDict
from dsi.plugins.plugin import Plugin
import inspect

class StructuredMetadata(Plugin):
    """ plugin superclass that provides handy methods for structured data """
    git_commit_sha: str = '5d79e08d4a6c1570ceb47cdd61d2259505c05de9'

    def __init__(self, **kwargs):
        """
        Initializes a StructuredDataPlugin with an output collector
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
        Initializes columns in the output_collector and table_cnt.
        Useful in a plugin's pack_header method.

        DO NOT USE THIS WITH SET_SCHEMA_2()

        `table_data`: 

            - for ingested data with multiple tables, table_data is list of tuples where each tuple is structured as (table name, column name list)
            - for data without multiple tables, table_data is just a list of column names
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
        Faster version (time and space) of updating output_collector by directly setting 'collection' to it, if collection is an Ordered Dict

        DO NOT USE THIS WITH SET_SCHEMA(), ADD_TO_OUTPUT(), OR SCHEMA_IS_SET()

        `collection`: data passed in from a plugin as an Ordered Dict. 

            - If only one table of data in there, it is nested in another Ordered Dict with table name as the plugin class name
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
        Adds a row of data to the output_collector and guarantees good structure.
        Useful in a plugin's add_rows method.

        DO NOT USE THIS WITH SET_SCHEMA_2()

        `row`: list of row of data

        `tableName`: default None. Specified name of table to ingest row into.
        """
        #POTENTIALLY REFACTOR AND AVOID FOR LOOP OF INGESTING DATA ROW BY ROW - MAYBE INGEST WHOLE DATA
        
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
        Helper method to see if the schema has been set 

        DO NOT USE THIS WITH SET_SCHEMA_2()
        """
        return self.table_cnt is not None
