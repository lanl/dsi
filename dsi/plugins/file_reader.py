from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json
from math import isnan
from pandas import DataFrame, read_csv, concat
import re
import yaml
import toml

from dsi.plugins.metadata import StructuredMetadata


class FileReader(StructuredMetadata):
    """
    FileReader Plugins keep information about the file that
    they are ingesting, namely absolute path and hash.
    """

    def __init__(self, filenames, **kwargs):
        super().__init__(**kwargs)
        if type(filenames) == str:
            self.filenames = [filenames]
        elif type(filenames) == list:
            self.filenames = filenames
        else:
            raise TypeError
        self.file_info = {}
        for filename in self.filenames:
            sha = sha1(open(filename, 'rb').read())
            self.file_info[abspath(filename)] = sha.hexdigest()


class Csv(FileReader):
    """
    A Plugin to ingest CSV data
    """

    # This turns on strict_mode when reading in multiple csv files that need matching schemas.
    # Default value is False.
    strict_mode = False

    def __init__(self, filenames, **kwargs):
        super().__init__(filenames, **kwargs)
        self.csv_data = {}

    def pack_header(self) -> None:
        """ Set schema based on the CSV columns """

        column_names = list(self.file_info.keys()) + list(self.csv_data.keys())
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """ Adds a list containing one or more rows of the CSV along with file_info to output. """

        if not self.schema_is_set():
            # use Pandas to append all CSVs together as a
            # dataframe, then convert to dict
            if self.strict_mode:
                total_df = DataFrame()
                dfs = []
                for filename in self.filenames:
                    # Initial case. Empty df collection.
                    if total_df.empty:
                        total_df = read_csv(filename)
                        dfs.append(total_df)
                    else:  # One or more dfs in collection
                        temp_df = read_csv(filename)
                        # raise exception if schemas do not match
                        if any([set(temp_df.columns) != set(df.columns) for df in dfs]):
                            print('Error: Strict schema mode is on. Schemas do not match.')
                            raise TypeError
                        dfs.append(temp_df)
                        total_df = concat([total_df, temp_df])

            # Reminder: Schema is not set in this block.
            else:  # self.strict_mode == False
                total_df = DataFrame()
                for filename in self.filenames:
                    temp_df = read_csv(filename)
                    total_df = concat([total_df, temp_df])

            # Columns are present in the middleware already (schema_is_set==True).
            # TODO: Can this go under the else block at line #79?
            self.csv_data = total_df.to_dict('list')
            for col, coldata in self.csv_data.items():  # replace NaNs with None
                self.csv_data[col] = [None if type(item) == float and isnan(item) else item
                                      for item in coldata]
            self.pack_header()

        total_length = len(self.csv_data[list(self.csv_data.keys())[0]])
        for row_idx in range(total_length):
            row = [self.csv_data[k][row_idx] for k in self.csv_data.keys()]
            row_w_fileinfo = list(self.file_info.values()) + row
            self.add_to_output(row_w_fileinfo)


class Bueno(FileReader):
    """
    A Plugin to capture performance data from Bueno (github.com/lanl/bueno)

    Bueno outputs performance data in keyvalue pairs in a file. Keys and values
    are delimited by ``:``. Keyval pairs are delimited by ``\\n``.
    """

    def __init__(self, filenames, **kwargs) -> None:
        super().__init__(filenames, **kwargs)
        self.bueno_data = OrderedDict()

    def pack_header(self) -> None:
        """Set schema with POSIX and Bueno data."""
        column_names = list(self.bueno_data.keys())
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """Parses Bueno data and adds a list containing 1 or more rows."""
        if not self.schema_is_set():
            for idx, filename in enumerate(self.filenames):
                with open(filename, 'r') as fh:
                    file_content = json.load(fh)
                for key, val in file_content.items():
                    # Check if column already exists
                    if key not in self.bueno_data:
                        # Initialize empty column if first time seeing it
                        self.bueno_data[key] = [None] \
                            * len(self.filenames)
                    # Set the appropriate row index value for this keyval_pair
                    self.bueno_data[key][idx] = val
            self.pack_header()

        rows = list(self.bueno_data.values())
        self.add_to_output(rows)
        # Flatten multiple samples of the same file.
        try:
            for col, rows in self.output_collector["Bueno"].items():
                self.output_collector["Bueno"][col] = rows[0] + rows[1]
        except IndexError:
            # First pass. Nothing to do.
            pass

class JSON(FileReader):
    """
    A Plugin to capture JSON data

    The JSON data's keys are used as columns and values are rows
   
    """
    def __init__(self, filenames, **kwargs) -> None:
        super().__init__(filenames, **kwargs)
        self.key_data = []
        self.base_dict = OrderedDict()
        
    def pack_header(self) -> None:
        """Set schema with POSIX and JSON data."""
        self.set_schema(self.key_data)

    def add_rows(self) -> None:
        """Parses JSON data and adds a list containing 1 or more rows."""

        objs = []
        for idx, filename in enumerate(self.filenames):
            with open(filename, 'r') as fh:
                    file_content = json.load(fh)
                    objs.append(file_content)
                    for key, val in file_content.items():
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

class YAML(FileReader):
    '''
    Plugin to read in an individual or a set of YAML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    '''
    def __init__(self, filenames, target_table_prefix = None, yamlSpace = '  ', **kwargs):
        '''
        `filenames`: one yaml file or a list of yaml files to be ingested
        `target_table_prefix`: prefix to be added to every table created to differentiate between other yaml sources
        `yamlSpace`: indent used in ingested yaml files - default 2 spaces but can change to the indentation used in input
        '''
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.yaml_files = [filenames]
        else:
            self.yaml_files = filenames
        self.yamlSpace = yamlSpace
        self.yaml_data = OrderedDict()
        self.target_table_prefix = target_table_prefix

    def pack_header(self) -> None:
        """Set schema with YAML data."""
        table_info = []
        for table_name in list(self.yaml_data.keys()):
            table_info.append((self.target_table_prefix + "_" + table_name, list(self.yaml_data[table_name].keys())))
        self.set_schema(table_info)

    def check_type(self, text):
        """
        Tests input text and returns a predicted compatible SQL Type
        `text`: text string
        `return`: string returned as int, float or still a string
        """
        try:
            _ = int(text)
            return int(text)
        except ValueError:
            try:
                _ = float(text)
                return float(text)
            except ValueError:
                return text
            
    def add_rows(self) -> None:
        """
        Parses YAML data and creates an ordered dict which stores an ordered dict for each table.
        """
        for filename in self.yaml_files:
            with open(filename, 'r') as yaml_file:
                editedString = yaml_file.read()
                editedString = re.sub('specification', f'columns:\n{self.yamlSpace}specification', editedString)
                editedString = re.sub(r'(!.+)\n', r"'\1'\n", editedString)
                yaml_load_data = list(yaml.safe_load_all(editedString))
                
                if not self.schema_is_set():
                    for table in yaml_load_data:
                        unit_list = [col + "_units" for col in table["columns"].keys()]
                        total_col_list = list(sum(zip(table["columns"].keys(), unit_list), ()))
                        self.yaml_data[table["segment"]] = OrderedDict((key, []) for key in total_col_list)
                    self.pack_header()

                for table in yaml_load_data:
                    row = []
                    for col_name, data in table["columns"].items():
                        unit_data = "NULL"
                        if isinstance(data, str) and not isinstance(self.check_type(data[:data.find(" ")]), str):
                            unit_data = data[data.find(' ')+1:]
                            data = self.check_type(data[:data.find(" ")])
                        self.yaml_data[table["segment"]][col_name].append(data)
                        self.yaml_data[table["segment"]][col_name + "_units"].append(unit_data)
                        row.extend([data, unit_data])
                    self.add_to_output(row, self.target_table_prefix + "_" + table["segment"])

class TOML(FileReader):
    '''
    Plugin to read in an individual or a set of TOML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    '''
    def __init__(self, filenames, target_table_prefix = None, **kwargs):
        '''
        `filenames`: one toml file or a list of toml files to be ingested
        `target_table_prefix`: prefix to be added to every table created to differentiate between other toml sources
        '''
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.toml_files = [filenames]
        else:
            self.toml_files = filenames
        self.toml_data = OrderedDict()
        self.target_table_prefix = target_table_prefix

    def pack_header(self) -> None:
        """Set schema with TOML data."""
        table_info = []
        for table_name in list(self.toml_data.keys()):
            table_info.append((self.target_table_prefix + "_" + table_name, list(self.toml_data[table_name].keys())))
        self.set_schema(table_info)

    def check_type(self, text):
        """
        Tests input text and returns a predicted compatible SQL Type
        `text`: text string
        `return`: string returned as int, float or still a string
        """
        try:
            _ = int(text)
            return int(text)
        except ValueError:
            try:
                _ = float(text)
                return float(text)
            except ValueError:
                return text

    def add_rows(self) -> None:
        """
        Parses TOML data and creates an ordered dict whose keys are table names and values are an ordered dict for each table.
        """
        for filename in self.toml_files:
            with open(filename, 'r') as toml_file:
                toml_load_data = toml.load(toml_file)

                if not self.schema_is_set():
                    for tableName, tableData in toml_load_data.items():
                        unit_list = [col + "_units" for col in tableData.keys()]
                        total_col_list = list(sum(zip(tableData.keys(), unit_list), ()))
                        self.toml_data[tableName] = OrderedDict((key, []) for key in total_col_list)
                    self.pack_header()

                for tableName, tableData in toml_load_data.items():
                    row = []
                    for col_name, data in tableData.items():
                        unit_data = "NULL"
                        if isinstance(data, list):
                            unit_data = data[1]
                            data = self.check_type(data[0])
                        self.toml_data[tableName][col_name].append(data)
                        self.toml_data[tableName][col_name + "_units"].append(unit_data)
                        row.extend([data, unit_data])
                    self.add_to_output(row, self.target_table_prefix + "_" + tableName)