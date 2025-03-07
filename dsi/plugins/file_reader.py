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
from datetime import datetime
# import ast

from dsi.plugins.metadata import StructuredMetadata


class FileReader(StructuredMetadata):
    """
    FileReader Plugins keep information about the file that they are ingesting, namely absolute path and hash.
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
    A Structured Data Plugin to ingest CSV data
    """
    def __init__(self, filenames, table_name = None, **kwargs):
        """
        Initializes CSV Reader with user specified filenames and optional table_name.

        `filenames`: Required input. List of CSV files, or just one CSV files to store in DSI. If a list, data in all files must be for the same table

        `table_name`: default None. User can specify table name when loading CSV file. Otherwise DSI uses table_name = "Csv"
        """
        super().__init__(filenames, **kwargs)
        self.csv_data = OrderedDict()
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.table_name = table_name

    def add_rows(self) -> None:
        """ Adds a list containing one or more rows of the CSV along with file_info to output. """

        total_df = DataFrame()
        for filename in self.filenames:
            temp_df = read_csv(filename)
            try:
                total_df = concat([total_df, temp_df], axis=0, ignore_index=True)
            except:
                raise ValueError(f"Error in adding {filename} to the existing csv data. Please recheck column names and data structure")

        table_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in table_data.items():  # replace NaNs with None
            table_data[col] = [None if type(item) == float and isnan(item) else item for item in coldata]
        
        if self.table_name is not None:
            self.csv_data[self.table_name] = table_data
        else:
            self.csv_data = table_data
        
        self.set_schema_2(self.csv_data)

class Bueno(FileReader):
    """
    A Structured Data Plugin to capture performance data from Bueno (github.com/lanl/bueno)

    Bueno outputs performance data in keyvalue pairs in a file. Keys and values are delimited by ``:``. Keyval pairs are delimited by ``\\n``.
    """

    def __init__(self, filenames, **kwargs) -> None:
        """
        `filenames`: one Bueno file or a list of Bueno files to be ingested
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.bueno_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Parses Bueno data and adds a list containing 1 or more rows.
        """
        file_counter = 0
        total_df = DataFrame()
        for filename in self.filenames:
            with open(filename, 'r') as fh:
                file_content = json.load(fh)
            temp_df = DataFrame([file_content])
            total_df = concat([total_df, temp_df], axis=0, ignore_index=True)

        self.bueno_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in self.bueno_data.items():  # replace NaNs with None
            self.bueno_data[col] = [None if type(item) == float and isnan(item) else item for item in coldata]
        
        self.set_schema_2(self.bueno_data)

class JSON(FileReader):
    """
    A Structured Data Plugin to capture JSON data

    The JSON data's keys are used as columns and values are rows
   
    """
    def __init__(self, filenames, **kwargs) -> None:
        """Initializes generic JSON reader with user-specified filenames"""
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
    """
    Structured Data Plugin to parse schema of a data source that will be ingested in same workflow.

    Schema file input should be a JSON file that stores primary and foreign keys for all tables in the data source. 
    Stores all relations in global dsi_relations table used for creating backends/writers
    """
    def __init__(self, filename, target_table_prefix = None, **kwargs):
        """
        `filename`: file name of the json file to be ingested

        `target_table_prefix`: prefix to be added to every table name in the primary and foreign key list
        """
        super().__init__(filename, **kwargs)
        self.schema_file = filename
        self.target_table_prefix = target_table_prefix
        self.schema_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Generates the dsi_relations OrderedDict to be added to the internal DSI abstraction. 

        The Ordered Dict has 2 keys, primary key and foreign key, with their values a list of PK and FK tuples associating tables and columns 
        """
        self.schema_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
        with open(self.schema_file, 'r') as fh:
            schema_content = json.load(fh)

            for tableName, tableData in schema_content.items():
                if self.target_table_prefix is not None:
                    tableName = self.target_table_prefix + "__" + tableName
                
                pkList = []
                if "foreign_key" in tableData.keys():
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
                    self.schema_data["dsi_relations"]["foreign_key"].append((None, None))
            self.set_schema_2(self.schema_data)

class YAML1(FileReader):
    """
    Structured Data Plugin to read in an individual or a set of YAML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    """
    def __init__(self, filenames, target_table_prefix = None, yamlSpace = '  ', **kwargs):
        """
        `filenames`: one yaml file or a list of yaml files to be ingested

        `target_table_prefix`: prefix to be added to every table created to differentiate between other yaml sources

        `yamlSpace`: indent used in ingested yaml files - default 2 spaces but can change to the indentation used in input
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.yaml_files = [filenames]
        else:
            self.yaml_files = filenames
        self.yamlSpace = yamlSpace
        self.yaml_data = OrderedDict()
        self.target_table_prefix = target_table_prefix

    def check_type(self, text):
        """
        Internal helper function that tests input text and returns a predicted compatible SQL Type

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
        Parses YAML data to create a nested Ordered Dict where each table is a key and its data as another Ordered Dict with keys as cols and vals as col data
        """
        file_counter = 0        
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
                    unitsDict = {}
                    for col_name, data in table["columns"].items():
                        unit_data = None
                        if isinstance(data, str) and not isinstance(self.check_type(data[:data.find(" ")]), str):
                            unit_data = data[data.find(" ")+1:]
                            data = self.check_type(data[:data.find(" ")])
                        if col_name not in self.yaml_data[tableName].keys():
                            self.yaml_data[tableName][col_name] = [None] * (file_counter) # Padding new cols in the dict from above
                        self.yaml_data[tableName][col_name].append(data)
                        if unit_data is not None and col_name not in unitsDict.keys():
                            unitsDict[col_name] = unit_data
                    if unitsDict:
                        if tableName not in self.yaml_data["dsi_units"].keys():
                            self.yaml_data["dsi_units"][tableName] = unitsDict
                        else:
                            overlap_cols = set(self.yaml_data["dsi_units"][tableName].keys()) & set(unitsDict)
                            for col in overlap_cols:
                                if self.yaml_data["dsi_units"][tableName][col] != unitsDict[col]:
                                    return (TypeError, f"Cannot have a different set of units for column {col} in {tableName}")
                            self.yaml_data["dsi_units"][tableName].update(unitsDict)

                    max_length = max(len(lst) for lst in self.yaml_data[tableName].values())
                    for key, value in self.yaml_data[tableName].items():
                        if len(value) < max_length:
                            self.yaml_data[tableName][key] = value + [None] * (max_length - len(value)) # Padding old unused cols from below
            file_counter += 1
        
        if len(self.yaml_data["dsi_units"]) == 0:
            del self.yaml_data["dsi_units"]
        self.set_schema_2(self.yaml_data)

        # SAVE FOR PLUGINS TO USE FOR PADDING MISMATCHED COLUMNS- YAML AND TOML USE THIS NOW
        # # Fill the shorter lists with None (or another value) if manually combining 2 data files together without pandas
        # max_length = max(len(lst) for lst in self.bueno_data.values())
        # for key, value in self.bueno_data.items():
        #     if len(value) < max_length:
        #         # Pad the list with None (or any other value)
        #         self.bueno_data[key] = value + [None] * (max_length - len(value))

class TOML1(FileReader):
    """
    Structured Data Plugin to read in an individual or a set of TOML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    """
    def __init__(self, filenames, target_table_prefix = None, **kwargs):
        """
        `filenames`: one toml file or a list of toml files to be ingested

        `target_table_prefix`: prefix to be added to every table created to differentiate between other toml sources
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.toml_files = [filenames]
        else:
            self.toml_files = filenames
        self.toml_data = OrderedDict()
        self.target_table_prefix = target_table_prefix

    def add_rows(self) -> None:
        """
        Parses TOML data and creates an ordered dict whose keys are table names and values are an ordered dict for each table.
        """
        file_counter = 0
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
                unitsDict = {}
                for col_name, data in tableData.items():
                    unit_data = None
                    if isinstance(data, dict):
                        unit_data = data["units"]
                        data = data["value"]
                    # IF statement for manual data parsing for python 3.10 and below
                    # if isinstance(data, str) and data[0] == "{" and data[-1] == "}":
                    #     data = ast.literal_eval(data)
                    #     unit_data = data["units"]
                    #     data = data["value"]
                    if col_name not in self.toml_data[tableName].keys():
                        self.toml_data[tableName][col_name] = [None] * (file_counter) # Padding new cols in the dict from above
                    self.toml_data[tableName][col_name].append(data)
                    if unit_data is not None and col_name not in unitsDict.keys():
                        unitsDict[col_name] = unit_data
                if unitsDict:
                        if tableName not in self.toml_data["dsi_units"].keys():
                            self.toml_data["dsi_units"][tableName] = unitsDict
                        else:
                            overlap_cols = set(self.toml_data["dsi_units"][tableName].keys()) & set(unitsDict)
                            for col in overlap_cols:
                                if self.toml_data["dsi_units"][tableName][col] != unitsDict[col]:
                                    return (TypeError, f"Cannot have a different set of units for column {col} in {tableName}")
                            self.toml_data["dsi_units"][tableName].update(unitsDict)

                max_length = max(len(lst) for lst in self.toml_data[tableName].values())
                for key, value in self.toml_data[tableName].items():
                    if len(value) < max_length:
                        self.toml_data[tableName][key] = value + [None] * (max_length - len(value)) # Padding old unused cols from below
            file_counter += 1

        if len(self.toml_data["dsi_units"]) == 0:
            del self.toml_data["dsi_units"]
        self.set_schema_2(self.toml_data)

class Wildfire(FileReader):
    """
    A Structured Data Plugin to ingest Wildfire data stored as a CSV

    Can be used for other cases if data is post-processed and running only once.
    Can create a manual simulation table
    """
    def __init__(self, filenames, table_name = None, sim_table = False, **kwargs):
        """
        Initializes Wildfire Reader with user specified parameters.

        `filenames`: Required input -- Wildfire data files

        `table_name`: default None. User can specify table name when loading the wildfire file.   

        `sim_table`: default False. Set to True if creating manual simulation table where each row of Wildfire file is a separate sim

            - also creates new column in wildfire data for each row to associate to a corresponding row/simulation in sim_table
        """
        super().__init__(filenames, **kwargs)
        self.csv_data = OrderedDict()
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.table_name = table_name
        self.sim_table = sim_table

    def add_rows(self) -> None:
        """ 
        Creates Ordered Dictionary for the wildfire data. 

        If sim_table = True, a sim_table Ordered Dict also created, and both are nested within a larger Ordered Dict.
        """
        if self.table_name is None:
            self.table_name = "Wildfire"

        total_df = DataFrame()
        for filename in self.filenames:
            temp_df = read_csv(filename)
            try:
                total_df = concat([total_df, temp_df], axis=0, ignore_index=True)
            except:
                return (ValueError, f"Error in adding {filename} to the existing wildfire data. Please recheck column names and data structure")
        
        if self.sim_table:
            total_df['sim_id'] = range(1, len(total_df) + 1)
            total_df = total_df[['sim_id'] + [col for col in total_df.columns if col != 'sim_id']]

        total_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in total_data.items():  # replace NaNs with None
            total_data[col] = [None if type(item) == float and isnan(item) else item for item in coldata]
        
        self.csv_data[self.table_name] = total_data
        
        if self.sim_table:
            sim_list = list(range(1, len(total_df) + 1))
            sim_dict = OrderedDict([('sim_id', sim_list)])
            self.csv_data["simulation"] = sim_dict

            relation_dict = OrderedDict([('primary_key', []), ('foreign_key', [])])
            relation_dict["primary_key"].append(("simulation", "sim_id"))
            relation_dict["foreign_key"].append((self.table_name, "sim_id"))
            self.csv_data["dsi_relations"] = relation_dict
       
        self.set_schema_2(self.csv_data)

class MetadataReader1(FileReader):
    """
    Structured Data Plugin to read in an individual or a set of JSON metadata files
    """
    def __init__(self, filenames, target_table_prefix = None, **kwargs):
        """
        `filenames`: one metadata json file or a list of metadata json files to be ingested

        `target_table_prefix`: prefix to be added to every table created to differentiate between other metadata file sources
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.metadata_files = [filenames]
        else:
            self.metadata_files = filenames
        self.metadata_file_data = OrderedDict()
        self.target_table_prefix = target_table_prefix
    
    def add_rows(self) -> None:
        """
        Parses metadata json files and creates an ordered dict whose keys are file names and values are an ordered dict of that file's data
        """
        file_counter = 0
        for filename in self.metadata_files:
            json_data = OrderedDict()
            with open(filename, 'r') as meta_file:
                file_content = json.load(meta_file)
                for key, col_data in file_content.items():
                    col_name = key
                    if isinstance(col_data, dict):
                        for inner_key, inner_val in col_data.items():
                            old_col_name = col_name
                            col_name = col_name + "__" + inner_key
                            if isinstance(inner_val, dict):
                                for key2, val2 in inner_val.items():
                                    old_col_name2 = col_name
                                    col_name = col_name + "__" + key2
                                    json_data[col_name] = [val2]
                                    col_name = old_col_name2
                            elif isinstance(inner_val, list):
                                json_data[col_name] = [str(inner_val)]
                            else:
                                json_data[col_name] = [inner_val]
                            col_name = old_col_name

                    elif isinstance(col_data, list):
                        json_data[col_name] = [str(col_data)]
                    else:
                        json_data[col_name] = [col_data]

            filename = filename[filename.rfind("/") + 1:]
            filename = filename[:filename.rfind(".")]
            if self.target_table_prefix is not None:
                filename = self.target_table_prefix + "__" + filename
            self.metadata_file_data[filename] = json_data

        self.set_schema_2(self.metadata_file_data)