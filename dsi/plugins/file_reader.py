from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json
from math import isnan
from pandas import DataFrame, read_csv, concat
import re
import yaml
try: import tomllib
except ModuleNotFoundError: import pip._vendor.tomli as tomllib
# import ast

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

    def __init__(self, filenames, table_name = None, **kwargs):
        super().__init__(filenames, **kwargs)
        self.csv_data = OrderedDict()
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.table_name = table_name

    # def pack_header(self) -> None:
    #     """ Set schema based on the CSV columns """

    #     column_names = list(self.file_info.keys()) + list(self.csv_data.keys())
    #     self.set_schema(column_names)

    def add_rows(self) -> None:
        """ Adds a list containing one or more rows of the CSV along with file_info to output. """

        total_df = DataFrame()
        for filename in self.filenames:
            temp_df = read_csv(filename)
            try:
                total_df = concat([total_df, temp_df])
            except:
                raise ValueError(f"Error in adding {filename} to the existing csv data. Please recheck column names and data structure")

        #convert total_df to ordered dict
        table_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in table_data.items():  # replace NaNs with None
            table_data[col] = [None if type(item) == float and isnan(item) else item for item in coldata]
        
        if self.table_name is not None:
            self.csv_data[self.table_name] = table_data
        else:
            self.csv_data = table_data
        
        self.set_schema_2(self.csv_data)

        # if not self.schema_is_set():
        #     # use Pandas to append all CSVs together as a
        #     # dataframe, then convert to dict
        #     if self.strict_mode:
        #         total_df = DataFrame()
        #         dfs = []
        #         for filename in self.filenames:
        #             # Initial case. Empty df collection.
        #             if total_df.empty:
        #                 total_df = read_csv(filename)
        #                 dfs.append(total_df)
        #             else:  # One or more dfs in collection
        #                 temp_df = read_csv(filename)
        #                 # raise exception if schemas do not match
        #                 if any([set(temp_df.columns) != set(df.columns) for df in dfs]):
        #                     print('Error: Strict schema mode is on. Schemas do not match.')
        #                     raise TypeError
        #                 dfs.append(temp_df)
        #                 total_df = concat([total_df, temp_df])

        #     # Reminder: Schema is not set in this block.
        #     else:  # self.strict_mode == False
        #         total_df = DataFrame()
        #         for filename in self.filenames:
        #             temp_df = read_csv(filename)
        #             total_df = concat([total_df, temp_df])

        #     # Columns are present in the middleware already (schema_is_set==True).
        #     # TODO: Can this go under the else block at line #79?
        #     self.csv_data = total_df.to_dict('list')
        #     for col, coldata in self.csv_data.items():  # replace NaNs with None
        #         self.csv_data[col] = [None if type(item) == float and isnan(item) else item
        #                               for item in coldata]
        #     self.pack_header()

        # total_length = len(self.csv_data[list(self.csv_data.keys())[0]])
        # for row_idx in range(total_length):
        #     row = [self.csv_data[k][row_idx] for k in self.csv_data.keys()]
        #     row_w_fileinfo = list(self.file_info.values()) + row
        #     self.add_to_output(row_w_fileinfo)


class Bueno(FileReader):
    """
    A Plugin to capture performance data from Bueno (github.com/lanl/bueno)

    Bueno outputs performance data in keyvalue pairs in a file. Keys and values
    are delimited by ``:``. Keyval pairs are delimited by ``\\n``.
    """

    def __init__(self, filenames, **kwargs) -> None:
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.bueno_data = OrderedDict()

    # def pack_header(self) -> None:
    #     """Set schema with POSIX and Bueno data."""
    #     column_names = list(self.bueno_data.keys())
    #     self.set_schema(column_names)

    def add_rows(self) -> None:
        """Parses Bueno data and adds a list containing 1 or more rows."""
        for filename in self.filenames:
            with open(filename, 'r') as fh:
                file_content = json.load(fh)
            for key, val in file_content.items():
                if key not in self.bueno_data:
                    self.bueno_data[key] = []
                self.bueno_data[key].append(val)

        max_length = max(len(lst) for lst in self.bueno_data.values())

        # Fill the shorter lists with None (or another value)
        for key, value in self.bueno_data.items():
            if len(value) < max_length:
                # Pad the list with None (or any other value)
                self.bueno_data[key] = value + [None] * (max_length - len(value))
        
        self.set_schema_2(self.bueno_data)

        # for idx, filename in enumerate(self.filenames):
        #     with open(filename, 'r') as fh:
        #         file_content = json.load(fh)
        #     for key, val in file_content.items():
        #         # Check if column already exists
        #         if key not in self.bueno_data:
        #             # Initialize empty column if first time seeing it
        #             self.bueno_data[key] = [None] \
        #                 * len(self.filenames)
        #         # Set the appropriate row index value for this keyval_pair
        #         self.bueno_data[key][idx] = val
        # self.pack_header()

        # rows = list(self.bueno_data.values())
        # self.add_to_output(rows)
        # # Flatten multiple samples of the same file.
        # try:
        #     for col, rows in self.output_collector["Bueno"].items():
        #         self.output_collector["Bueno"][col] = rows[0] + rows[1]
        # except IndexError:
        #     # First pass. Nothing to do.
        #     pass

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

class Schema(FileReader):
    '''
    Plugin to parse schema of a data source that is about to be ingested.
    Schema file input should be a JSON file that stores primary and foreign keys for all tables in the data source.
    Store all relations in global dsi_relations table used for creating backends/writers
    '''
    def __init__(self, filename, target_table_prefix = None, **kwargs):
        super().__init__(filename, **kwargs)
        self.schema_file = filename
        self.target_table_prefix = target_table_prefix
        self.schema_data = OrderedDict()

    def pack_header(self) -> None:
        """Set schema with YAML data."""
        table_info = []
        for table_name in list(self.schema_data.keys()):
            table_info.append((table_name, list(self.schema_data[table_name].keys())))
        self.set_schema(table_info)

    def add_rows(self) -> None:    
        self.schema_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
        with open(self.schema_file, 'r') as fh:
            schema_content = json.load(fh)

            for tableName, tableData in schema_content.items():
                if self.target_table_prefix is not None:
                    tableName = self.target_table_prefix + "__" + tableName
                
                pkList = []
                for colName, colData in tableData["foreign_key"].items():
                    if self.target_table_prefix is not None:
                        colData[0] = self.target_table_prefix + "__" + colData[0]
                    self.schema_data["dsi_relations"]["primary_key"].append((colData[0], colData[1]))
                    self.schema_data["dsi_relations"]["foreign_key"].append((tableName, colName))

                if "primary_key" in tableData.keys():
                    pkList.append((tableName, tableData["primary_key"]))
            
            for pk in pkList:
                if pk not in self.schema_data["dsi_relations"]["primary_key"]:
                    self.schema_data["dsi_relations"]["primary_key"].append(pk)
                    self.schema_data["dsi_relations"]["foreign_key"].append(("NULL", "NULL"))
            self.set_schema_2(self.schema_data)

        # if not self.schema_is_set():
        #     self.schema_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
        #     self.pack_header()

        # with open(self.schema_file, 'r') as fh:
        #     schema_content = json.load(fh)

        #     for tableName, tableData in schema_content.items():
        #         if self.target_table_prefix is not None:
        #             tableName = self.target_table_prefix + "__" + tableName
        #         if tableData["primary_key"] != "NULL":
        #             self.schema_data["dsi_relations"]["primary_key"].append((tableName, tableData["primary_key"]))
        #             self.schema_data["dsi_relations"]["foreign_key"].append(("NULL", "NULL"))
        #             self.add_to_output([(tableName, tableData["primary_key"]), ("NULL", "NULL")], "dsi_relations")

        #         for colName, colData in tableData["foreign_key"].items():
        #             if self.target_table_prefix is not None:
        #                 colData[0] = self.target_table_prefix + "__" + colData[0]
        #             self.schema_data["dsi_relations"]["primary_key"].append((colData[0], colData[1]))
        #             self.schema_data["dsi_relations"]["foreign_key"].append((tableName, colName))
        #             self.add_to_output([(colData[0], colData[1]), (tableName, colName)], "dsi_relations")

class YAML1(FileReader):
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

    # def pack_header(self) -> None:
    #     """Set schema with YAML data."""
    #     table_info = []
    #     for table_name in list(self.yaml_data.keys()):
    #         table_info.append((table_name, list(self.yaml_data[table_name].keys())))
    #     self.set_schema(table_info)

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

                if "dsi_units" not in self.yaml_data.keys():
                    self.yaml_data["dsi_units"] = OrderedDict()
                for table in yaml_load_data:
                    tableName = table["segment"]
                    if self.target_table_prefix is not None:
                        tableName = self.target_table_prefix + "__" + table["segment"]
                    if tableName not in self.yaml_data.keys():
                        self.yaml_data[tableName] = OrderedDict()
                    unitsList = []
                    for col_name, data in table["columns"].items():
                        unit_data = "NULL"
                        if isinstance(data, str) and not isinstance(self.check_type(data[:data.find(" ")]), str):
                            unit_data = data[data.find(' ')+1:]
                            data = self.check_type(data[:data.find(" ")])
                        if col_name not in self.yaml_data[tableName].keys():
                            self.yaml_data[tableName][col_name] = []
                        self.yaml_data[tableName][col_name].append(data)
                        if unit_data != "NULL" and (col_name, unit_data) not in unitsList:
                            unitsList.append((col_name, unit_data))
                    if len(unitsList) > 0 and tableName not in self.yaml_data["dsi_units"].keys():
                        self.yaml_data["dsi_units"][tableName] = unitsList

        self.set_schema_2(self.yaml_data)

                # if not self.schema_is_set():
                #     self.yaml_data["dsi_units"] = OrderedDict()
                #     for table in yaml_load_data:
                #         tableName = table["segment"]
                #         if self.target_table_prefix is not None:
                #             tableName = self.target_table_prefix + "__" + table["segment"]
                #         self.yaml_data[tableName] = OrderedDict((key, []) for key in table["columns"].keys())
                #         self.yaml_data["dsi_units"][tableName] = []
                #     # self.yaml_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
                #     self.pack_header()

                # unit_row = []
                # for table in yaml_load_data:
                #     row = []
                #     table_unit_row = []
                #     tableName = table["segment"]
                #     if self.target_table_prefix is not None:
                #         tableName = self.target_table_prefix + "__" + table["segment"]
                #     for col_name, data in table["columns"].items():
                #         unit_data = "NULL"
                #         if isinstance(data, str) and not isinstance(self.check_type(data[:data.find(" ")]), str):
                #             unit_data = data[data.find(' ')+1:]
                #             data = self.check_type(data[:data.find(" ")])
                #         self.yaml_data[tableName][col_name].append(data)
                #         if (col_name, unit_data) not in self.yaml_data["dsi_units"][tableName]:
                #             table_unit_row.append((col_name, unit_data))
                #             self.yaml_data["dsi_units"][tableName].append((col_name, unit_data))
                #         row.append(data)
                #     self.add_to_output(row, tableName)
                #     unit_row.append(table_unit_row)
                # if len(next(iter(self.output_collector["dsi_units"].values()))) < 1:
                #     self.add_to_output(unit_row, "dsi_units")

class TOML1(FileReader):
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

    # def pack_header(self) -> None:
    #     """Set schema with TOML data."""
    #     table_info = []
    #     for table_name in list(self.toml_data.keys()):
    #         table_info.append((table_name, list(self.toml_data[table_name].keys())))
    #     self.set_schema(table_info)

    def add_rows(self) -> None:
        """
        Parses TOML data and creates an ordered dict whose keys are table names and values are an ordered dict for each table.
        """
        for filename in self.toml_files:
            # with open(filename, 'r+') as temp_file:
            #     editedString = temp_file.read()
            #     if '"{' not in editedString:
            #         editedString = re.sub('{', '"{', editedString)
            #         editedString = re.sub('}', '}"', editedString)
            #         temp_file.seek(0)
            #         temp_file.write(editedString)
            
            toml_load_data = None
            with open(filename, 'rb') as toml_file:
                toml_load_data = tomllib.load(toml_file)

            if "dsi_units" not in self.toml_data.keys():
                    self.toml_data["dsi_units"] = OrderedDict()
            for tableName, tableData in toml_load_data.items():
                if self.target_table_prefix is not None:
                    tableName = self.target_table_prefix + "__" + tableName
                if tableName not in self.toml_data.keys():
                    self.toml_data[tableName] = OrderedDict()
                unitsList = []
                for col_name, data in tableData.items():
                    unit_data = "NULL"
                    if isinstance(data, dict):
                        unit_data = data["units"]
                        data = data["value"]
                    # IF statement for manual data parsing for python 3.10 and below
                    # if isinstance(data, str) and data[0] == "{" and data[-1] == "}":
                    #     data = ast.literal_eval(data)
                    #     unit_data = data["units"]
                    #     data = data["value"]
                    if col_name not in self.toml_data[tableName].keys():
                        self.toml_data[tableName][col_name] = []
                    self.toml_data[tableName][col_name].append(data)
                    if unit_data != "NULL" and (col_name, unit_data) not in unitsList:
                        unitsList.append((col_name, unit_data))
                if len(unitsList) > 0 and tableName not in self.toml_data["dsi_units"].keys():
                    self.toml_data["dsi_units"][tableName] = unitsList

        self.set_schema_2(self.toml_data)

            # if not self.schema_is_set():
            #     for tableName, tableData in toml_load_data.items():
            #         if self.target_table_prefix is not None:
            #             tableName = self.target_table_prefix + "__" + tableName
            #         self.toml_data[tableName] = OrderedDict((key, []) for key in tableData.keys())
            #         self.toml_data["dsi_units"] = OrderedDict([(tableName,[])])
            #     # self.toml_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
            #     self.pack_header()

            # unit_row = []
            # for tableName, tableData in toml_load_data.items():
            #     row = []
            #     table_unit_row = []
            #     if self.target_table_prefix is not None:
            #         tableName = self.target_table_prefix + "__" + tableName
            #     for col_name, data in tableData.items():
            #         unit_data = "NULL"
            #         if isinstance(data, dict):
            #             unit_data = data["units"]
            #             data = data["value"]
            #         # IF statement for manual data parsing for python 3.10 and below
            #         # if isinstance(data, str) and data[0] == "{" and data[-1] == "}":
            #         #     data = ast.literal_eval(data)
            #         #     unit_data = data["units"]
            #         #     data = data["value"]
            #         self.toml_data[tableName][col_name].append(data)
            #         if (col_name, unit_data) not in self.toml_data["dsi_units"][tableName]:
            #             table_unit_row.append((col_name, unit_data))
            #             self.toml_data["dsi_units"][tableName].append((col_name, unit_data))
            #         row.append(data)
            #     self.add_to_output(row, tableName)
            #     unit_row.append(table_unit_row)
            # if len(next(iter(self.output_collector["dsi_units"].values()))) < 1:
            #         self.add_to_output(unit_row, "dsi_units")

class TextFile(FileReader):
    '''
    Plugin to read in an individual or a set of text files
    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    '''
    def __init__(self, filenames, target_table_prefix = None, **kwargs):
        '''
        `filenames`: one text file or a list of text files to be ingested
        `target_table_prefix`: prefix to be added to every table created to differentiate between other text file sources
        '''
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.text_files = [filenames]
        else:
            self.text_files = filenames
        self.text_file_data = OrderedDict()
        self.target_table_prefix = target_table_prefix

    def add_rows(self) -> None:
        """
        Parses text file data and creates an ordered dict whose keys are table names and values are an ordered dict for each table.
        """
        for filename in self.text_files:
            df = read_csv(filename)
            if self.target_table_prefix is not None:
                self.text_file_data[f"{self.target_table_prefix}__text_file"] = OrderedDict(df.to_dict(orient='list'))
            else:
                self.text_file_data["text_file"] = OrderedDict(df.to_dict(orient='list'))
            self.set_schema_2(self.text_file_data)