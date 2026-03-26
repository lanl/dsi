from collections import OrderedDict
from typing import Any, Iterator, List, Optional, Dict
from os.path import abspath
from hashlib import sha1
import json
from math import isnan
from pandas import DataFrame, read_csv, concat
import re
import yaml
try: import ruamel.yaml
except ModuleNotFoundError: ruamel = None
try: import tomllib
except ModuleNotFoundError: import pip._vendor.tomli as tomllib
import os
from pyarrow import parquet as pq
from pydantic import BaseModel, Field
# import ast

from dsi.plugins.metadata import StructuredMetadata


class FileReader(StructuredMetadata):
    """
    FileReaders keep information about the file that they are ingesting, namely absolute path and hash.
    """

    def __init__(self, filenames, **kwargs):
        super().__init__(**kwargs)
        if isinstance(filenames, str):
            self.filenames = [filenames]
        elif isinstance(filenames, list):
            self.filenames = filenames
        elif isinstance(filenames, (dict, DataFrame)):
            self.filenames = filenames
        else:
            raise TypeError
        self.file_info = {}
        for filename in self.filenames:
            sha = sha1(open(filename, 'rb').read())
            self.file_info[abspath(filename)] = sha.hexdigest()
    
    def store_dict(self, reader_name: str, data_dict: dict, expected_columns: list[str]) -> OrderedDict:
        if all(isinstance(val, list) for val in data_dict.values()):
            extra = set(data_dict.keys()) - set(expected_columns)
            if extra:
                raise ValueError(f"Input dictionary for {reader_name} data card reader has extra columns: {', '.join(extra)}")
            
            reordered_dict = OrderedDict((k, data_dict[k]) for k in expected_columns if k in data_dict)
            max_len = max(len(values) for values in reordered_dict.values())
            for value in reordered_dict.values():
                value.extend([None] * (max_len - len(value)))

            return reordered_dict
        elif not any(isinstance(val, (dict,list)) for val in data_dict.values()): # checking if single dict with scalar values (no nested dicts or lists)
            extra = set(data_dict.keys()) - set(expected_columns)
            if extra:
                raise ValueError(f"Input dictionary for {reader_name} data card reader has extra columns: {', '.join(extra)}")
            
            reordered_dict = OrderedDict((k, [data_dict[k]]) for k in expected_columns if k in data_dict) # make each value a list not scalar value
            return reordered_dict
        else:
            raise ValueError("Input dictionary must represent one table of data.")

    def store_dataframe(self, reader_name: str, data_df: DataFrame, expected_columns: list[str]) -> OrderedDict:
        df_cols = set(data_df.columns)
        extra = df_cols - set(expected_columns)
        if extra:
            raise ValueError(f"Input DataFrame for {reader_name} data card reader has extra columns: {', '.join(extra)}")
        
        result = OrderedDict(
            (col, data_df[col].tolist() if col in df_cols else [])
            for col in expected_columns
        )

        max_len = max(len(values) for values in result.values())
        for value in result.values():
            value.extend([None] * (max_len - len(value)))
        
        return result

    def check_type(self, text):
        """
        **Internal helper function** 
        
        Function tests input text and returns a predicted compatible SQL Type

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


class Csv(FileReader):
    """
    A DSI Reader that reads in CSV data
    """
    def __init__(self, filenames, table_name = None, **kwargs):
        """
        Initializes the CSV Reader with user specified filenames and optional table_name.

        `filenames` : str or list of str
            Required. One or more CSV file paths to be loaded into DSI.
            If multiple files are provided, all data must correspond to the same table.

        `table_name` : str, optional
            Optional name to assign to the loaded table.
            If not provided, DSI will default to using "Csv" as the table name.
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
            except Exception:
                raise TypeError(f"Error in adding {filename} to the existing csv data. Please recheck column names and data structure")

        table_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in table_data.items():  # replace NaNs with None
            table_data[col] = [None if isinstance(item, float) and isnan(item) else item for item in coldata]
        
        if self.table_name is not None:
            self.csv_data[self.table_name] = table_data
        else:
            self.csv_data = table_data
        
        self.set_schema_2(self.csv_data)


class Bueno(FileReader):
    """
    A DSI Reader that captures performance data from Bueno (github.com/lanl/bueno)

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
        total_df = DataFrame()
        for filename in self.filenames:
            with open(filename, 'r') as fh:
                file_content = json.load(fh)
            temp_df = DataFrame([file_content])
            total_df = concat([total_df, temp_df], axis=0, ignore_index=True)

        self.bueno_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in self.bueno_data.items():  # replace NaNs with None
            self.bueno_data[col] = [None if isinstance(item, float) and isnan(item) else item for item in coldata]
        
        self.set_schema_2(self.bueno_data)


class JSON(FileReader):
    """
    A DSI Reader for ingesting generic JSON data with flat key-value pairs.
    
    This reader assumes that all values are primitive data types (e.g., str, int, float),
    and that there are no nested dictionaries, arrays, or complex structures.

    The keys in the JSON object are treated as column names, and their corresponding values are interpreted as rows.
    """
    def __init__(self, filenames, table_name = None, **kwargs) -> None:
        """
        Initializes the generic JSON reader with user-specified filenames
        
        `filenames` : str or list of str
            Required input. One or more JSON file paths to be loaded into DSI.
            If multiple files are provided, all data must all correspond to the same table

        `table_name` : str, optional
            Name to assign to the loaded table. If not provided, DSI defaults to using "JSON" as the table name.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.base_dict = OrderedDict()
        self.table_name = table_name

    def add_rows(self) -> None:
        """Parses JSON data and stores data into a table as an Ordered Dictionary."""

        temp_dict = OrderedDict()
        for filename in self.filenames:
            with open(filename, 'r') as fh:
                file_content = json.load(fh)
                for key, val in file_content.items():
                    if not isinstance(val, (str, float, int)):
                        raise TypeError("Generic JSON reader cannot handle nested data, only flat JSON values.")
                    if key not in temp_dict:
                        temp_dict[key] = []
                    temp_dict[key].append(val)

        if self.table_name is None:
            self.base_dict["JSON"] = temp_dict
        else:
            self.base_dict[self.table_name] = temp_dict
        self.set_schema_2(self.base_dict)


class Schema(FileReader):
    """
    DSI Reader for parsing and ingesting a relational schema alongside its associated data.

    Schema file input should be a JSON file that defines primary and foreign key relationships between tables in a data source.
    Parsed relationships are stored in the global `dsi_relations` table, which is used for creating backends and used by writers.

    This reader is essential when working with complex, multi-table data structures. 
    See :ref:`schema_section` to learn how a schema file should be structured
    """
    def __init__(self, filename, target_table_prefix = None, **kwargs):
        """
        Initializes the Schema reader with the specified schema file.

        `filename`: str
            Path to the JSON file containing the schema to be ingested. This file should define
            primary and foreign key relationships between tables.

        `target_table_prefix` : str, optional
            A prefix to prepend to every table name in the primary and foreign key lists.
            Useful for avoiding naming conflicts in shared environments.
        """
        super().__init__(filename, **kwargs)
        self.schema_file = filename
        self.target_table_prefix = target_table_prefix
        self.schema_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Generates a `dsi_relations` OrderedDict to be added to the internal DSI abstraction. 

        The Ordered Dict has 2 keys, primary key and foreign key, with their values a list of PK and FK tuples associating tables and columns 
        """
        self.schema_data["dsi_relations"] = OrderedDict([('primary_key', []), ('foreign_key', [])])
        with open(self.schema_file, 'r') as fh:
            schema_content = json.load(fh)

            pkList = []
            for tableName, tableData in schema_content.items():
                if self.target_table_prefix is not None:
                    tableName = self.target_table_prefix + "__" + tableName
                
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


class YAML(FileReader):
    """
    DSI Reader that reads in an individual or a set of standardized YAML files
    """
    def __init__(self, filenames, table_name = None, yaml_version: str = "1.1", **kwargs):
        """
        Initializes the YAML reader with the specified YAML file(s)

        `filenames` : str or list of str
            One standardized YAML file or a list of standardized YAML files to be loaded into DSI.
        
        `table_name`: str
            Name to assign to the loaded YAML data. If not provided, DSI defaults to using "YAML" as the table name.

        `yaml_version`: str, default = "1.1" 
            Major and minor version of YAML specification.   
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.yaml_files = [filenames]
        else:
            self.yaml_files = filenames

        if ruamel is None and yaml_version == "1.2":
            raise RuntimeError("To use YAML version 1.2, first execute requirements.extras.txt")
        
        self.yaml_data = OrderedDict()
        self.table_name = table_name
        self.yaml_version = yaml_version
        self.ruamel_yaml_safe_loader = None

    def _get_ruamel_yaml_safe_loader(self)-> Any:
        if self.ruamel_yaml_safe_loader is None:
            self.ruamel_yaml_safe_loader = ruamel.yaml.YAML(typ = "safe")
        return self.ruamel_yaml_safe_loader

    def _safe_load(self,data: Any)-> Any:
        match self.yaml_version:
            case "1"|"1.0"|"1.1":
                return_value = yaml.safe_load(data)
            case "1.2":
                return_value = self._get_ruamel_yaml_safe_loader().load(data)
            case _:
                raise ValueError(f"Invalid YAML version {self.yaml_version}. Did you only provide major/minor versions (e.g., '1.1')?")
        return return_value
    
    def _safe_load_all(self,data: Any)-> Iterator[Any]:
        match self.yaml_version:
            case "1"|"1.0"|"1.1":
                return_value = yaml.safe_load_all(data)
            case "1.2":
                return_value = self._get_ruamel_yaml_safe_loader().load_all(data)
            case _:
                raise ValueError(f"Invalid YAML version {self.yaml_version}. Did you only provide major/minor versions (e.g., '1.1')?")
        return return_value
    
    def recursive_yaml(self, d, parent_key=""):
        items = {}
        for k, v in d.items():
            # change amount of _ to separate parent and child key
            new_key = f"{parent_key}_{k}" if parent_key else k

            if isinstance(v, dict):
                items.update(self.recursive_yaml(v, new_key))
            else:
                items[new_key] = v
        return items
    
    def add_rows(self) -> None:
        """
        Parses YAML data for one table and constructs a nested OrderedDict to load into DSI. 

        The resulting structure has:

            - One top-level key -- user input table name
            - Its value is an OrderedDict where:

                - Keys are column names.
                - Values are lists representing column data.
        """
        if self.table_name is None:
            raise ValueError("The base YAML reader requires an input table name")
        
        file_counter = 0
        self.yaml_data = OrderedDict()
        for file in self.yaml_files:
            try:
                with open(file, 'r') as yaml_file:
                    yaml_load_data = list(self._safe_load_all(yaml_file))
            except Exception:
                raise ValueError(f"Error opening YAML file: {file}")
            
            # cannot read in multiple tables per YAML file without multiple input table names
            if len(yaml_load_data) != 1:
                raise ValueError("The base YAML reader can only read one table per file")
            
            file_dict = self.recursive_yaml(yaml_load_data[0])
            for k, v in file_dict.items():
                if k not in self.yaml_data.keys():
                    self.yaml_data[k] = [None] * (file_counter)
                self.yaml_data[k].append(v)
            
            file_counter += 1
        self.set_schema_2(OrderedDict([(self.table_name, self.yaml_data)]))


class YAML1(FileReader):
    """
    DSI Reader that reads in an individual or a set of YAML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    """
    def __init__(self, filenames, target_table_prefix = None, yamlSpace = '  ', **kwargs):
        """
        Initializes the YAML1 reader with the specified YAML file(s)

        `filenames` : str or list of str
            One YAML file or a list of YAML files to be loaded into DSI.

        `target_table_prefix`: str, optional
            A prefix to be added to each table name created from the YAML data.
            Useful for distinguishing between tables from other data sources.

        `yamlSpace` : str, default='  '
            The indentation used in the input YAML files. 
            Defaults to two spaces, but can be customized to match the formatting in certain files.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.yaml_files = [filenames]
        else:
            self.yaml_files = filenames
        self.yamlSpace = yamlSpace
        self.yaml_data = OrderedDict()
        self.target_table_prefix = target_table_prefix

    def add_rows(self) -> None:
        """
        Parses YAML data and constructs a nested OrderedDict to load into DSI.

        The resulting structure has:

            - Top-level keys as table names.
            - Each value is an OrderedDict where:

                - Keys are column names.
                - Values are lists representing column data.
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
                                    raise TypeError(f"Cannot have a different set of units for column {col} in {tableName}")
                            self.yaml_data["dsi_units"][tableName].update(unitsDict)

                    max_length = max(len(lst) for lst in self.yaml_data[tableName].values())
                    for key, value in self.yaml_data[tableName].items():
                        if len(value) < max_length:
                            self.yaml_data[tableName][key] = value + [None] * (max_length - len(value)) # Padding old unused cols from below
            file_counter += 1
        
        if len(self.yaml_data["dsi_units"]) == 0:
            del self.yaml_data["dsi_units"]
        else:
            dsi_unit_data = self.yaml_data["dsi_units"]
            del self.yaml_data["dsi_units"]
            new_unit_dict = OrderedDict([('table_name', []), ('column_name', []), ('unit', [])])
            for table_name, unit_tuple in dsi_unit_data.items():
                for col, unit in unit_tuple.items():
                    new_unit_dict['table_name'].append(table_name)
                    new_unit_dict['column_name'].append(col)
                    new_unit_dict['unit'].append(unit)
            self.yaml_data["dsi_units"] = new_unit_dict

        self.set_schema_2(self.yaml_data)

        # SAVE FOR READERS TO USE FOR PADDING MISMATCHED COLUMNS- YAML AND TOML USE THIS NOW
        # # Fill the shorter lists with None (or another value) if manually combining 2 data files together without pandas
        # max_length = max(len(lst) for lst in self.bueno_data.values())
        # for key, value in self.bueno_data.items():
        #     if len(value) < max_length:
        #         # Pad the list with None (or any other value)
        #         self.bueno_data[key] = value + [None] * (max_length - len(value))


class TOML(FileReader):
    """
    DSI Reader that reads in an individual or a set of TOML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    """
    def __init__(self, filenames, table_name = None, **kwargs):
        """
        `filenames` : str or list of str
            One TOML file or a list of TOML files to be loaded into DSI.

        `table_name`: str
            If only one table in input file(s), this name is assigned to the loaded TOML data in a DSI database. 
            If not provided, DSI defaults to using "TOML" as the table name.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.toml_files = [filenames]
        else:
            self.toml_files = filenames
        self.toml_data = OrderedDict()
        self.table_name = table_name
    
    def has_table_headers(self, toml_text) -> bool:
        _TABLE_HEADER_RE = re.compile(r"^\s*\[(\[[^\]]+\]|[^\]]+)\]\s*(#.*)?$")
        for line in toml_text.splitlines():
            s = line.strip()
            if s and not s.startswith("#") and _TABLE_HEADER_RE.match(line):
                return True
        return False

    def recursive_toml(self, d, parent_key=""):
        items = {}
        for k, v in d.items():
            # change amount of _ to separate parent and child key
            new_key = f"{parent_key}_{k}" if parent_key else k

            if isinstance(v, dict):
                items.update(self.recursive_toml(v, new_key))
            else:
                items[new_key] = v
        return items

    def columnize(self, rows) -> dict:
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())
        return { key: [row.get(key, None) for row in rows] for key in sorted(all_keys) }

    def normalize_toml(self, path) -> tuple[dict, bool]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
            data = tomllib.loads(text)
        except Exception:
            raise ValueError(f"Error opening TOML file: {path}")

        if self.has_table_headers(text):
            bad_keys = [ k for k, v in data.items()
                if not (isinstance(v, dict) or (isinstance(v, list) and all(isinstance(item, dict) for item in v)))
            ]
            if bad_keys:
                raise ValueError("TOML file cannot have loose top-level fields if there are defined tables.")

            out = {}
            for table_name, value in data.items():
                if isinstance(value, dict):
                    rows = [self.recursive_toml(value)]
                else:
                    rows = [self.recursive_toml(row) for row in value]
                out[table_name] = self.columnize(rows)

            return out, True

        flat = self.recursive_toml(data)
        return {key: [value] for key, value in flat.items()}, False

    def add_rows(self) -> None:
        """
        Parses TOML data and constructs a nested OrderedDict to load into DSI.

        The resulting structure has:

            - Top-level keys as table names.
            - Each value is an OrderedDict where:

                - Keys are column names.
                - Values are lists representing column data.
        """
        file_counter = 0
        self.toml_data = OrderedDict()
        for filename in self.toml_files:
            toml_dict, has_tables = self.normalize_toml(filename)

            if has_tables:
                for k,v in toml_dict.items():
                    if k not in self.toml_data.keys():
                        self.toml_data[k] = OrderedDict()
                    for v_key, v_val in v.items(): # v guaranteed to be nested dict
                        if v_key not in self.toml_data[k].keys():
                            self.toml_data[k][v_key] = [None] * (file_counter) #top padding for column
                        #v_val guaranteed to be a list
                        self.toml_data[k][v_key].extend(v_val)
                    
                    # after table is stored, pad bottom of col list in case inconsistent length
                    max_len = max(len(x) for x in self.toml_data[k].values())
                    for k1, v1 in self.toml_data[k].items():
                        self.toml_data[k][k1] = v1 + [None] * (max_len - len(v1))
                
                self.set_schema_2(self.toml_data)
            else:
                if self.table_name is None:
                    raise ValueError(f"TOML file, {filename}, has one table which requires an input table name")
                
                for k,v in toml_dict.items():
                    if k not in self.toml_data.keys():
                        self.toml_data[k] = [None] * (file_counter)
                    self.toml_data[k].append(v)
                
                self.set_schema_2(OrderedDict([(self.table_name, self.toml_data)]))

            file_counter += 1


class TOML1(FileReader):
    """
    DSI Reader that reads in an individual or a set of TOML files

    Table names are the keys for the main ordered dictionary and column names are the keys for each table's nested ordered dictionary
    """
    def __init__(self, filenames, target_table_prefix = None, **kwargs):
        """
        `filenames` : str or list of str
            One TOML file or a list of TOML files to be loaded into DSI.

        `target_table_prefix`: str, optional
            A prefix to be added to each table name created from the TOML data.
            Useful for distinguishing between tables from other data sources.
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
        Parses TOML data and constructs a nested OrderedDict to load into DSI.

        The resulting structure has:

            - Top-level keys as table names.
            - Each value is an OrderedDict where:

                - Keys are column names.
                - Values are lists representing column data.
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
                                    raise TypeError(f"Cannot have a different set of units for column {col} in {tableName}")
                            self.toml_data["dsi_units"][tableName].update(unitsDict)

                max_length = max(len(lst) for lst in self.toml_data[tableName].values())
                for key, value in self.toml_data[tableName].items():
                    if len(value) < max_length:
                        self.toml_data[tableName][key] = value + [None] * (max_length - len(value)) # Padding old unused cols from below
            file_counter += 1

        if len(self.toml_data["dsi_units"]) == 0:
            del self.toml_data["dsi_units"]
        else:
            dsi_unit_data = self.toml_data["dsi_units"]
            del self.toml_data["dsi_units"]
            new_unit_dict = OrderedDict([('table_name', []), ('column_name', []), ('unit', [])])
            for table_name, unit_tuple in dsi_unit_data.items():
                for col, unit in unit_tuple.items():
                    new_unit_dict['table_name'].append(table_name)
                    new_unit_dict['column_name'].append(col)
                    new_unit_dict['unit'].append(unit)
            self.toml_data["dsi_units"] = new_unit_dict

        self.set_schema_2(self.toml_data)


class Parquet(FileReader):
    """
    DSI Reader that loads data stored in a Parquet file as a table. Users can choose to specify the table name upon reading too.
    """
    def __init__(self, filenames, table_name = None, **kwargs):
        """
        Initializes the Parquet Reader with user specified filenames and an optional table_name.

        `filenames` : str or list of str
            Required. One or more Parquet file paths to be loaded into DSI.
            If multiple files are provided, all data must correspond to the same table.

        `table_name` : str, optional
            Optional name to assign to the loaded table.
            If not provided, DSI will default to using "Parquet" as the table name.
        """
        super().__init__(filenames, **kwargs)
        self.parquet_data = OrderedDict()
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.table_name = table_name
    
    def add_rows(self) -> None:
        """Parses Parquet data and stores data into a table as an Ordered Dictionary."""
        total_df = DataFrame()
        
        for filename in self.filenames:
            table = pq.read_table(filename).to_pandas()
            try:
                total_df = concat([total_df, table], axis=0, ignore_index=True)
            except Exception:
                raise TypeError(f"Error in adding {filename} to the existing Parquet data. Please recheck column names and data structure")

        table_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in table_data.items():  # replace NaNs with None
            table_data[col] = [None if isinstance(item, float) and isnan(item) else item for item in coldata]

        if self.table_name is not None:
            self.parquet_data[self.table_name] = table_data
        else:
            self.parquet_data = table_data
        
        self.set_schema_2(self.parquet_data)


class Ensemble(FileReader):
    """
    DSI Reader that loads ensemble simulation data stored in a CSV file.

    Designed to handle simulation outputs where each row represents an individual simulation run. 
    Specifically for single-run use cases when data is post-processed.

    Automatically generates a simulation metadata table to accompany the data.
    """
    def __init__(self, filenames, table_name = None, sim_table = True, **kwargs):
        """
        Initializes Ensemble Reader with user specified parameters.

        `filenames` : str or list of str
            Required input. One or more Ensemble data files in CSV format.
            All files must correspond to the same table.

        `table_name` : str, optional
            Optional name to assign to the table when loading the Ensemble data.
            If not provided, the default table name, 'Ensemble', will be used.

        `sim_table` : bool, default=True
            If True, creates a simulation metadata table (`sim_table`) where each row 
            in the input data table represents a separate simulation run.

            - Adds a new column to the input data to associate each row with its corresponding entry in the simulation table.

            If False, skips creation of the simulation table.
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
        Creates an OrderedDict representation of the Ensemble data to load into DSI.

        When sim_table = True, a sim_table Ordered Dict is created alongside the Ensemble data table OrderedDict.
        Both tables are nested within a larger OrderedDict.
        """
        if self.table_name is None:
            self.table_name = "Ensemble"

        total_df = DataFrame()
        for filename in self.filenames:
            temp_df = read_csv(filename)
            try:
                total_df = concat([total_df, temp_df], axis=0, ignore_index=True)
            except Exception:
                raise ValueError(f"Error in adding {filename} to existing Ensemble data. Please column names and data structure again.")
        
        if self.sim_table:
            total_df['sim_id'] = range(1, len(total_df) + 1)
            total_df = total_df[['sim_id'] + [col for col in total_df.columns if col != 'sim_id']]

        total_data = OrderedDict(total_df.to_dict(orient='list'))
        for col, coldata in total_data.items():  # replace NaNs with None
            total_data[col] = [None if isinstance(item, float) and isnan(item) else item for item in coldata]
        
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


class Cloverleaf(FileReader):
    """
    DSI Reader that stores input and output Cloverleaf data from a directory for each simulation run
    """
    def __init__(self, folder_path, **kwargs):
        """
        `folder_path` : str
            Filepath to the directory where the Cloverleaf data is stored. 
            The directory should have a subfolder for each simulation run, each containing input and output data
        """
        if folder_path[-1] != '/':
            self.folder_path = folder_path
        else:
            self.folder_path = folder_path[:-1]
        self.cloverleaf_data = OrderedDict()
            
    def add_rows(self) -> None:
        """
        Flattens data from each simulation's input file as a row in the `input` table.
        Flattens data from each simulation's output file as a row in the `output` table.
        Creates a simulation table which is stores each simulation's number and execution datetime.
        """
        input_dict = OrderedDict({'sim_id': []})
        output_dict = OrderedDict({'sim_id': []})
        viz_dict = OrderedDict({'sim_id': [], 'image_filepath': []})
        simulation_dict = OrderedDict({'sim_id': [], 'sim_datetime': []})

        sim_num = 1
        all_runs = sorted([f.name for f in os.scandir(self.folder_path) if f.is_dir() and not f.name.startswith('.') ])
        for run_name in all_runs:
            input_file = f"{self.folder_path}/{run_name}/clover.in"
            
            input_dict["sim_id"].append(sim_num)
            with open(input_file, 'r') as f:
                input_lines = [line.strip() for line in f if line.strip()]

            num_timesteps = 0
            for line in input_lines:
                if line.startswith("*"):
                    continue
                if "test_problem" in line:
                    test_line = line.strip().lower().split()
                    if test_line[0] not in input_dict.keys():
                        input_dict[test_line[0]] = []
                    input_dict[test_line[0]].append(self.check_type(test_line[1]))
                elif '=' not in line:
                    continue

                if line.startswith("state 1"):
                    prefix = "state1_"
                    tokens = line.replace("state 1", "").strip().split()
                elif line.startswith("state 2"):
                    prefix = "state2_"
                    tokens = line.replace("state 2", "").strip().split()
                else:
                    prefix = ""
                    tokens = line.split()
                
                for token in tokens:
                    if '=' in token:
                        key, value = token.split('=', 1)
                        full_key = prefix.lower() + key.lower()
                        if full_key not in input_dict.keys():
                            input_dict[full_key] = []
                        input_dict[full_key].append(self.check_type(value))
                        if full_key == "end_step":
                            num_timesteps = self.check_type(value)
            
            output_file = f"{self.folder_path}/{run_name}/clover.out"
            with open(output_file, 'r') as f:
                output_lines = [line.strip() for line in f if line.strip()]
            
            for index, line in enumerate(output_lines):
                if line[:6] != "Step  ":
                    continue
                output_dict["sim_id"].append(sim_num)

                next_line = index
                total_line = line.strip().split()
                if total_line[1] == str(num_timesteps):
                    next_line = index + 10
                elif total_line[1][-1] == "0":
                    next_line = index + 3
                
                wall_line = output_lines[next_line+1].strip().split()
                wall_line[0] = f"{wall_line[0]}_{wall_line[1]}"
                if next_line == index + 10:
                    total_line.extend([wall_line[0], wall_line[2], "Average_time_per_cell", None, "Step_time_per_cell", None])
                else:
                    avg_line = output_lines[next_line+2].strip().split()
                    avg_line[0] = f"{avg_line[0]}_{avg_line[1]}_{avg_line[2]}_{avg_line[3]}"
                    step_t_line = output_lines[next_line+3].strip().split()
                    step_t_line[0] = f"{step_t_line[0]}_{step_t_line[1]}_{step_t_line[2]}_{step_t_line[3]}"
                    total_line.extend([wall_line[0], wall_line[2], avg_line[0], avg_line[4], step_t_line[0], step_t_line[4]])
                for out_key, out_val in zip(total_line[::2], total_line[1::2]):
                    if out_key == '1,':
                        continue
                    if out_key.lower() not in output_dict.keys():
                        output_dict[out_key.lower()] = []
                    if out_val is not None:
                        output_dict[out_key.lower()].append(self.check_type(out_val))
                    else:
                        output_dict[out_key.lower()].append(out_val)

            for filename in os.listdir(f"{self.folder_path}/{run_name}"):
                if "vtk" in filename:
                    viz_dict["sim_id"].append(sim_num)
                    viz_dict["image_filepath"].append(f"{run_name}/{filename}")
            viz_dict["image_filepath"] = sorted(viz_dict["image_filepath"])

            simulation_dict["sim_id"].append(sim_num)
            with open(f"{self.folder_path}/{run_name}/timestamp.txt", 'r') as f:
                sim_line = [line.strip() for line in f if line.strip()]
            simulation_dict['sim_datetime'].append(sim_line[0])

            sim_num+=1

        self.cloverleaf_data["input"] = input_dict
        self.cloverleaf_data["output"] = output_dict
        self.cloverleaf_data["simulation"] = simulation_dict
        self.cloverleaf_data["viz_files"] = viz_dict
        self.set_schema_2(self.cloverleaf_data)


class Oceans11Datacard(YAML):
    """
    DSI Reader that stores a dataset's data card as a row in the `oceans11_datacard` table.
    Input datacard should follow template in `examples/test/template_dc_oceans11.yml`
    """
    def __init__(self, filenames, **kwargs):
        """
        `filenames` : str, list of str, or data object (dict or pandas DataFrame)
            File name(s) of YAML data card files to ingest. Each file must adhere to the
            Oceans 11 Server metadata standard.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.datacard_files = [filenames]
        else:
            self.datacard_files = filenames
        self.datacard_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Flattens data in the input data card as a row in the `oceans11_datacard` table
        """
        expected_columns = ["title", "description", "keywords", "instructions_of_use", "authors", "release_date", "la_ur", 
                            "funding", "rights", "file_types", "file_size", "num_files", "dataset_size", "version", "doi"]
        if isinstance(self.datacard_files, dict):
            self.set_schema_2(OrderedDict([("oceans11_datacard", self.store_dict("Oceans11", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, DataFrame):
            self.set_schema_2(OrderedDict([("oceans11_datacard", self.store_dataframe("Oceans11", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, list):

            temp_data = OrderedDict()
            for filename in self.datacard_files:
                with open(filename, 'r') as yaml_file:
                    data = self._safe_load(yaml_file)
                    
                field_names = []
                for element, val in data.items():
                    if element not in ['authorship', 'data']:
                        if isinstance(val, list):
                            val = ",, ".join(val)
                        if element not in temp_data.keys():
                            temp_data[element] = [val]
                        else:
                            temp_data[element].append(val)
                        field_names.append(element)
                    else:
                        for field, val2 in val.items():
                            if isinstance(val2, list):
                                val2 = ",, ".join(val2)
                            if field not in temp_data.keys():
                                temp_data[field] = [val2]
                            else:
                                temp_data[field].append(val2)
                            field_names.append(field)

                if sorted(field_names) != sorted(["title", "description", "keywords", "instructions_of_use", "authors", 
                                                "release_date", "la_ur", "funding", "rights", "file_types", 
                                                "file_size", "num_files", "dataset_size", "version", "doi"]):
                    raise ValueError(f"Error in reading {filename} data card. Please ensure all fields match the Oceans11 template")

            self.datacard_data["oceans11_datacard"] = temp_data
            self.set_schema_2(self.datacard_data)
        else:
            raise ValueError("Input for the Oceans11Datacard reader must be a YAML file, dictionary or pandas DataFrame")


class DublinCoreDatacard(FileReader):
    """
    DSI Reader that stores a dataset's data card as a row in the `dublin_core_datacard` table.
    Input datacard should follow template in `examples/test/template_dc_dublin_core.xml`
    """
    def __init__(self, filenames, **kwargs):
        """
        `filenames` : str, list of str, or data object (dict or pandas DataFrame)
            File name(s) of XML data card files to ingest. Each file must adhere to the
            Dublin Core metadata standard.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.datacard_files = [filenames]
        else:
            self.datacard_files = filenames
        self.datacard_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Flattens data in the input data card as a row in the `dublin_core_datacard` table
        """
        expected_columns = ['Creator', 'Contributor', 'Publisher', 'Title', 'Date', 'Language', 'Format', 'Subject', 
                            'Description', 'Identifier', 'Relation', 'Source', 'Type', 'Coverage', 'Rights']
        if isinstance(self.datacard_files, dict):
            self.set_schema_2(OrderedDict([("dublin_core_datacard", self.store_dict("Dublin Core", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, DataFrame):
            self.set_schema_2(OrderedDict([("dublin_core_datacard", self.store_dataframe("Dublin Core", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, list):

            import xmltodict
            temp_data = OrderedDict()
            for filename in self.datacard_files:
                with open(filename, 'r', encoding="utf-8") as xml_file:
                    xml_data = xml_file.read()
                    data = xmltodict.parse(xml_data)
                    
                field_names = []
                for element, val in next(iter(data.values())).items():
                    if val is None:
                        val = ""
                    if element not in temp_data.keys():
                        temp_data[element] = [val]
                    else:
                        temp_data[element].append(val)
                    field_names.append(element)
                if sorted(field_names) != sorted(expected_columns):
                    raise ValueError(f"Error in reading {filename} data card. Please ensure all fields match the Dublin Core template")

            self.datacard_data["dublin_core_datacard"] = temp_data
            self.set_schema_2(self.datacard_data)
        else:
            raise ValueError("Input for the DublinCoreDatacard reader must be an XML file, dictionary or pandas DataFrame")


class SchemaOrgDatacard(FileReader):
    """
    DSI Reader that stores a dataset's data card as a row in the `schema_org_datacard` table.
    Input datacard should follow template in `examples/test/template_dc_schema_org.json`
    """
    def __init__(self, filenames, **kwargs):
        """
        `filenames` : str, list of str, or data object (dict or pandas DataFrame)
            File name(s) of JSON data card files to ingest. Each file must adhere to the
            Schema.org metadata standard.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.datacard_files = [filenames]
        else:
            self.datacard_files = filenames
        self.datacard_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Flattens data in the input data card as a row in the `schema_org_datacard` table
        """
        expected_columns = ["@type", "name", "description", "keywords", "creator", "audience",  "expires",  "isBasedOn", "isPartOf", 
                            "accountablePerson", "publisher", "editor", "funder", "funding", "dateCreated", "dateModified", 
                            "datePublished", "countryOfOrigin", "locationCreated", "sourceOrganization", "url", "version", 
                            "creditText", "license", "citation", "copyrightHolder", "copyrightNotice", "copyrightYear"]
        if isinstance(self.datacard_files, dict):
            self.set_schema_2(OrderedDict([("schema_org_datacard", self.store_dict("Schema.org", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, DataFrame):
            self.set_schema_2(OrderedDict([("schema_org_datacard", self.store_dataframe("Schema.org", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, list):

            temp_data = OrderedDict()
            for filename in self.datacard_files:
                with open(filename, 'r') as schema_file:
                    data = json.load(schema_file)
                    
                field_names = []
                for element, val in data.items():
                    if element == "@type" and val.lower() == "dataset":
                        field_names.append(element)
                        continue
                    elif element == "@type" and val.lower() != "dataset":
                        raise KeyError(f"{filename} must have key '@type' with value of 'Dataset' to match Schema.org requirements")
                    if element not in temp_data.keys():
                        temp_data[element] = [val]
                    else:
                        temp_data[element].append(val)
                    field_names.append(element)
                if sorted(field_names) != sorted(expected_columns):
                    raise ValueError(f"Error in reading {filename} data card. Please ensure all fields match the Schema.org template")

            self.datacard_data["schema_org_datacard"] = temp_data
            self.set_schema_2(self.datacard_data)
        else:
            raise ValueError("Input for the SchemaOrgDatacard reader must be an JSON file, dictionary or pandas DataFrame")


class GoogleDatacard(YAML):
    """
    DSI Reader that stores a dataset's data card as a row in the `google_datacard` table.
    Input datacard should follow template in `examples/test/template_dc_google.yml`
    """
    def __init__(self, filenames, **kwargs):
        """
        `filenames` : str, list of str, or data object (dict or pandas DataFrame)
            File name(s) of YAML data card files to ingest. 
            Each file must adhere to the Google Data Cards Playbook metadata standard.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.datacard_files = [filenames]
        else:
            self.datacard_files = filenames
        self.datacard_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Flattens data in the input data card as a row in the `google_datacard` table
        """
        expected_columns = ['dataset_name', 'summary', 'dataset_link', 'documentation_link', 'datacard_author1', 'datacard_author2', 
                            'datacard_author3', 'publishing_organization', 'publishing_POC', 'publishing_POC_affiliation', 
                            'publishing_POC_contact', 'dataset_owner1', 'dataset_owner2', 'dataset_owner3', 'dataset_owners_affiliation', 
                            'dataset_owners_contact', 'funding_institution', 'funding_summary', 'data_subjects', 'data_sensitivity', 
                            'version', 'maintenance_status', 'last_updated', 'release_date', 'motivation', 'dataset_uses', 
                            'citation_guidelines', 'citation_bibtex', 'collection_methods_used', 'source', 'platform', 
                            'dates_of_collection', 'type_of_data', 'data_selection', 'data_inclusion', 'data_exclusion']
        if isinstance(self.datacard_files, dict):
            self.set_schema_2(OrderedDict([("google_datacard", self.store_dict("Google", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, DataFrame):
            self.set_schema_2(OrderedDict([("google_datacard", self.store_dataframe("Google", self.datacard_files, expected_columns))]))
        elif isinstance(self.datacard_files, list):

            temp_data = OrderedDict()

            for filename in self.datacard_files:
                with open(filename, 'r') as yaml_file:
                    data = self._safe_load(yaml_file)
                
                section_headers = ["summary", "authorship", "overview", "provenance", "sampling_methods", "known_applications_and_benchmarks"]
                if not set(data.keys()).issubset(section_headers):
                    raise KeyError(f"Error in reading {filename} data card. Please ensure section names match the ones in the Google template")
                
                field_names = []
                sampling_fields = []
                ml_fields = []
                for element, val in data.items():
                    for inner_key, inner_val in val.items():
                        if inner_key not in temp_data.keys():
                            temp_data[inner_key] = [inner_val]
                        else:
                            temp_data[inner_key].append(inner_val)
                            
                        if element == "sampling_methods":
                            sampling_fields.append(inner_key)
                        elif element == "known_applications_and_benchmarks":
                            ml_fields.append(inner_key)
                        else:
                            field_names.append(inner_key)

                if not set(field_names).issubset(expected_columns):
                    raise ValueError(f"Error in reading {filename} data card. Ensure all fields match the Google dc template")
                
                if not set(sampling_fields).issubset(['sampling_method_used', 'sampling_criteria1', 'sampling_criteria2', 'sampling_criteria3']):
                    raise KeyError(f"Error in reading {filename} data card. Ensure all fields in 'sampling_methods' match the Google dc template")
                
                if not set(ml_fields).issubset(['ml_applications', 'ml_model_name', 'evaluation_accuracy', 'evaluation_precision', 
                                                    'evaluation_recall', 'evaluation_performance_metric']):
                    raise KeyError(f"Error reading {filename}. Ensure all fields in 'known_applications_and_benchmarks' match the Google dc template")
            self.datacard_data["google_datacard"] = temp_data
            self.set_schema_2(self.datacard_data)
        else:
            raise ValueError("Input for the GoogleDatacard reader must be an YAML file, dictionary or pandas DataFrame")


class GenesisDatacard(FileReader):
    """
    DSI Reader that stores a dataset's data card as a row in the `genesis_datacard` table.
    Input datacard should follow distributed template model card.
    """
    def __init__(self, filenames, **kwargs):
        """
        `filenames` : str or list of str
            File name(s) of Markdown files to ingest. Each file must adhere to the
            Genesis metadata standard.
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.markdown_files = [filenames]
        else:
            self.markdown_files = filenames
        self.genesis_datacard_data = OrderedDict()

    def extract_yaml_block(self, content: str) -> Optional[str]:
        """
        Extract YAML frontmatter from markdown content.
        Expects YAML between '---' delimiters.
        
        Args:
            content: Markdown content string
            
        Returns:
            YAML text or None if not found
        """
        # Pattern to match YAML frontmatter between --- delimiters
        pattern = r'^---\s*\n(.*?)\n---\s*\n'
        match = re.search(pattern, content, re.DOTALL | re.MULTILINE)
        
        if match:
            return match.group(1), content[match.end():]
        return None, None
    
    def get_value_by_partial_key(self, d, query):
        for k, v in d.items():
            if query in k:
                return k, v
        return None, None

    def protect_headers_in_code_blocks(self, text: str) -> str:
        code_block_pattern = re.compile(r'(^```[^\n]*\n)(.*?)(^```[ \t]*$)', re.MULTILINE | re.DOTALL)

        def rewrite_code_block(match: re.Match) -> str:
            opening = match.group(1)
            body = match.group(2)
            closing = match.group(3)

            body = re.sub(r'^(#{2,3}\s)', r' \1', body, flags=re.MULTILINE)
            return opening + body + closing

        return code_block_pattern.sub(rewrite_code_block, text)

    def parse_markdown_sections(self, text: str) -> dict[str, str]:
        # Match lines that are exactly "## header" or "### header"
        header_pattern = re.compile(r'^(#{2,3})\s+(.+?)\s*$', re.MULTILINE)

        matches = list(header_pattern.finditer(text))
        result = {}

        for i, match in enumerate(matches):
            header = match.group(2)

            value_start = match.end()
            if i + 1 < len(matches):
                value_end = matches[i + 1].start()
            else:
                # For the last header, stop at a line containing only ---
                end_match = re.search(r'^---\s*$', text[value_start:], re.MULTILINE)
                value_end = value_start + end_match.start() if end_match else len(text)

            value = text[value_start:value_end]
            if value.startswith('\n'):
                value = value[1:]
            value = value.rstrip()
            if not value.strip():
                value = ''
            result[header.lower()] = value

        return result

    def get_tag_value(self, tags: list, item: str) -> str:
        """Extract tag value from tags"""
        for tag in tags:
            if tag.startswith(item):
                return tag.split(':')[1]
        return ""
    
    class DataCardSchema(BaseModel):
        """
        Complete schema for scientific dataset datacards.
        Matches YAML frontmatter structure from datacard_template.md.
        """
        
        # Core identification and metadata
        language: List[str] = Field(default_factory=list, description="Languages used in dataset/documentation")
        datacard_version: str = Field(..., description="Version of the datacard schema")
        dataset_id: str = Field(..., description="Source-prefixed ID: OSTI:1234567, SNL-DS-2024-XXXXX, LANL:LAUR-XX-XXXXX, etc.")
        name: str = Field(..., description="Human-readable dataset name")
        project_tag: str = Field(default="", description="Project name that must be included on ALL Genesis datasets")
        type_tag: str = Field(default="", description="Type of project (e.g., dataset, agent, eval, framework, model, etc.)")
        science_tag: str = Field(default="", description="The science this is used for (e.g., materials, biology, lightsource, fusion, climate, etc.)")
        risk_tag: str = Field(default="", description="Indicates level of risk review {general, reviewed, restricted}")

        # Licensing
        license: str = Field(default="", description="SPDX license identifier (e.g., CC-BY-4.0)")
        license_name: str = Field(default="", description="Full license name")
        license_link: str = Field(default="", description="URL to license text or LICENSE file")
        
        # Authors and contact
        authors: List[Dict[str, Any]] = Field(default_factory=list, description="Dataset authors with name, affiliation, orcid, email")

        contact_name: Optional[str] = Field("", description="Primary contact name")
        contact_email: Optional[str] = Field("", description="Primary contact email")
        sponsor_orgs: List[str] = Field(default_factory=list, description="Organizations sponsoring/funding the collection of data")
        research_orgs: List[str] = Field(default_factory=list, description="Research organizations affiliated with this data")
        
        # Dataset characteristics
        dataset_type: Optional[str] = Field("", description="Type of dataset: experimental, simulation, observational, etc.")
        description: Optional[str] = Field("", description="Comprehensive dataset description")
        issue_date: Optional[str] = Field("", description="Date this data was published. Publication date in YYYY-MM-DD format")
        report_number: Optional[str] = Field("", description="Lab release number (SAND for Sandia, LAUR for LANL, etc.). Report number for only this data card")
        doi: Optional[str] = Field("", description="Digital Object Identifier for the dataset")
        
        # Data access and location
        access_url: Optional[str] = Field("", description="Primary URL to access/download the data")
        download_url: Optional[str] = Field("", description="Direct download URL for dataset files")
        landing_page: Optional[str] = Field("", description="Human-readable landing page URL")
        repository: Optional[str] = Field("", description="Repository or archive name (e.g., OSTI, PDS, Zenodo)")
        repository_url: Optional[str] = Field("", description="URL to repository record")
        data_location: Optional[str] = Field("", description="Internal storage location (path, bucket, filesystem)")
        access_protocol: Optional[str] = Field("", description="Access protocol (https, ftp, s3, lustre, nfs, etc.)")
        access_restrictions: Optional[str] = Field("", description="Access restrictions or requirements")
        file_format: Optional[str] = Field(default="", description="Primary file format(s)")
        data_size: Optional[str] = Field("", description="Total dataset size")
        checksum_algorithm: Optional[str] = Field("", description="Checksum algorithm (md5, sha256, etc.)")
        checksum_value: Optional[str] = Field("", description="Checksum value for verification")
        
        # Security classification and sensitivity (REQUIRED for national lab datasets)
        classification: Optional[str] = Field("", description="Security classification (U, CUI, C, S, TS)")
        marking: Optional[str] = Field("", description="Classification marking (UUR, CUI, CUI//SP-PRVCY, etc.)")
        distribution_statement: Optional[str] = Field("", description="Distribution limitation statement")
        export_control: Optional[str] = Field("", description="Export control status (none, EAR, ITAR)")
        export_control_number: Optional[str] = Field("", description="ECCN or USML category if applicable")
        sensitivity_level: Optional[str] = Field("", description="Data sensitivity (public, internal, confidential, restricted)")
        data_rights: Optional[str] = Field("", description="Data rights and ownership information")
        classification_reason: Optional[str] = Field("", description="Reason for classification")
        declassification_date: Optional[str] = Field("", description="Date when data will be declassified")
        
        # API access information (optional)
        api_endpoint: Optional[str] = Field("", description="Base URL for API access")
        api_documentation: Optional[str] = Field("", description="URL to API documentation")
        api_authentication: Optional[str] = Field("", description="Authentication method (none, api_key, oauth2, certificate)")
        api_version: Optional[str] = Field("", description="API version")
        api_rate_limit: Optional[str] = Field("", description="Rate limiting information")
        

        overview: str = Field(default="", description="Detailed overview of the dataset, including its scientific context and significance")

        # part of data structure section
        file_organization: Optional[str] = Field(default="", description="")
        data_format: Optional[str] = Field(default="", description="")

        variables: Optional[str] = Field(default="", description="")

        # part of api access section
        api_endpoint_info: Optional[str] = Field(default="", description="")
        api_authentication_info: Optional[str] = Field(default="", description="")
        api_rate_limit_info: Optional[str] = Field(default="", description="")
        api_example_usage: Optional[str] = Field(default="", description="")
        api_available_endpoints: Optional[str] = Field(default="", description="")

        # part of usage guidelines section
        access_citation: Optional[str] = Field(default="", description="")
        licensing_terms: Optional[str] = Field(default="", description="")
        usage_example_code: Optional[str] = Field(default="", description="")

        provenance: Optional[str] = Field(default="", description="")
        
        #part of dataseheet for dataset section
        dataset_motivation: Optional[str] = Field(default="", description="")
        dataset_composition: Optional[str] = Field(default="", description="")
        dataset_collection: Optional[str] = Field(default="", description="")
        dataset_preprocessing: Optional[str] = Field(default="", description="")
        dataset_uses: Optional[str] = Field(default="", description="")
        dataset_distribution: Optional[str] = Field(default="", description="")
        dataset_maintenance: Optional[str] = Field(default="", description="")
        dataset_resources: Optional[str] = Field(default="", description="")
        dataset_contact_info: Optional[str] = Field(default="", description="")

        # part of compliance section
        osti_requirements: Optional[str] = Field(default="", description="")


    def parse_datacard(self, content: str) -> Optional[DataCardSchema]:
        """
        Parse markdown content into a DataCardSchema object.
        
        Args:
            content: Complete markdown content with YAML frontmatter
            
        Returns:
            DataCardSchema object or None if parsing fails
        """
        yaml_text, free_form_text = self.extract_yaml_block(content)
        
        if yaml_text:
            try:
                data = yaml.safe_load(yaml_text)
                if isinstance(data, dict):
                    datacard = self.DataCardSchema(
                        language=data.get('language', []),
                        datacard_version=data.get('datacard_version', ''),
                        dataset_id=data.get('dataset_id', ''),
                        name=data.get('name', ''),

                        license=data.get('license', ''),
                        license_name=data.get('license_name', ''),
                        license_link=data.get('license_link', ''),

                        authors=data.get('authors', []),
                        contact_name=data.get('contact_name', ''),
                        contact_email=data.get('contact_email', ''),
                        sponsor_orgs=data.get('sponsor_orgs', []),
                        research_orgs=data.get('research_orgs', []),

                        dataset_type=data.get('dataset_type', ''),
                        description=data.get('description', ''),
                        issue_date=data.get('issue_date', ''),
                        report_number=data.get('report_number', ''),
                        doi=data.get('doi', ''),

                        access_url=data.get('access_url', ''),
                        download_url=data.get('download_url', ''),
                        landing_page=data.get('landing_page', ''),
                        repository=data.get('repository', ''),
                        repository_url=data.get('repository_url', ''),
                        data_location=data.get('data_location', ''),
                        access_protocol=data.get('access_protocol', ''),
                        access_restrictions=data.get('access_restrictions', ''),
                        file_format=data.get('file_format', ''),
                        data_size=data.get('data_size', ''),
                        checksum_algorithm=data.get('checksum_algorithm', ''),
                        checksum_value=data.get('checksum_value', ''),

                        classification=data.get('classification', ''),
                        marking=data.get('marking', ''),
                        distribution_statement=data.get('distribution_statement', ''),
                        export_control=data.get('export_control', ''),
                        export_control_number=data.get('export_control_number', ''),
                        sensitivity_level=data.get('sensitivity_level', ''),
                        data_rights=data.get('data_rights', ''),
                        classification_reason=data.get('classification_reason', ''),
                        declassification_date=data.get('declassification_date', ''),

                        api_endpoint=data.get('api_endpoint', ''),
                        api_documentation=data.get('api_documentation', ''),
                        api_authentication=data.get('api_authentication', ''),
                        api_version=data.get('api_version', ''),
                        api_rate_limit=data.get('api_rate_limit', '')
                    )
                    # tags formatting
                    tags=data.get('tags', [])
                    datacard.project_tag = self.get_tag_value(tags, "project:")
                    datacard.type_tag = self.get_tag_value(tags, "type:")
                    datacard.science_tag = self.get_tag_value(tags, "science:")
                    datacard.risk_tag = self.get_tag_value(tags, "risk:")

                    # free form text formatting
                    safe_text = self.protect_headers_in_code_blocks(free_form_text)
                    free_form_dict = self.parse_markdown_sections(safe_text)

                    field_map = {
                        "overview": "overview",
                        "file organization": "file_organization",
                        "data format": "data_format",
                        "variables": "variables",
                        "endpoint information": "api_endpoint_info",
                        "authentication": "api_authentication_info",
                        "rate limits": "api_rate_limit_info",
                        "example usage": "api_example_usage",
                        "available endpoints": "api_available_endpoints",
                        "access and citation": "access_citation",
                        "licensing terms": "licensing_terms",
                        "example code": "usage_example_code",
                        "provenance": "provenance",
                        "motivation": "dataset_motivation",
                        "composition": "dataset_composition",
                        "collection process": "dataset_collection",
                        "preprocessing": "dataset_preprocessing",
                        "cleaning": "dataset_preprocessing",
                        "labeling": "dataset_preprocessing",
                        "uses": "dataset_uses",
                        "distribution": "dataset_distribution",
                        "maintenance": "dataset_maintenance",
                        "related resources": "dataset_resources",
                        "contact information": "dataset_contact_info",
                        "osti requirements": "osti_requirements",
                    }

                    for query, attr in field_map.items():
                        actual_key, actual_val = self.get_value_by_partial_key(free_form_dict, query)
                        if actual_key is not None:
                            current = getattr(datacard, attr, "")
                            if current:
                                new_value = current + "\n" + actual_val
                            else:
                                new_value = actual_val

                            setattr(datacard, attr, new_value)
                            del free_form_dict[actual_key]    

                    # remove expected unused headers
                    expected_unused_headers = ['data structure', 'api access', 'usage guidelines', 'datasheet for dataset', 'compliance']
                    for header in expected_unused_headers:
                        actual_key, _ = self.get_value_by_partial_key(free_form_dict, header)
                        if actual_key is not None:
                            del free_form_dict[actual_key]
                    

                    return datacard, free_form_dict
            except yaml.YAMLError as e:
                if e.args:
                    e.args = (f'YAML parsing error: {str(e.args[0])}',)
                raise
            except Exception as e:
                if e.args:
                    e.args = (f'Validation error: {str(e.args[0])}',)
                raise
        
        return None, None

    def add_rows(self) -> None:
        """
        Flattens data in the input data card as a row in the `genesis_datacard` table
        """
        for filename in self.markdown_files:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            datacard, remaining_dict = self.parse_datacard(content)
            if datacard:
                for name, value in vars(datacard).items():
                    if name not in self.genesis_datacard_data.keys():
                        self.genesis_datacard_data[name] = []
                    self.genesis_datacard_data[name].append(value)
            else:
                raise ValueError(f"Failed to parse datacard from {filename}")
            
            if remaining_dict:
                max_len = max(len(v) for v in self.genesis_datacard_data.values()) - 1 # -1 so don't include newest additions
                for name, value in remaining_dict.items():
                    if name not in self.genesis_datacard_data.keys():
                        self.genesis_datacard_data[name] = [None] * max_len # padding this with None at top
                    self.genesis_datacard_data[name].append(value)
        
        self.set_schema_2(OrderedDict([("genesis_datacard", self.genesis_datacard_data)]))