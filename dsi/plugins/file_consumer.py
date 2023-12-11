from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json
from math import isnan
from pandas import DataFrame, read_csv, concat

from dsi.plugins.metadata import StructuredMetadata


class FileConsumer(StructuredMetadata):
    """
    FileConsumer Plugins keep information about the file that
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


class Csv(FileConsumer):
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


class Bueno(FileConsumer):
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
            for col, rows in self.output_collector.items():
                self.output_collector[col] = rows[0] + rows[1]
        except IndexError:
            # First pass. Nothing to do.
            pass
