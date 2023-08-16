from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
from pandas import DataFrame, read_csv, concat
from math import isnan

from dsi.plugins.metadata import StructuredMetadata


class FileConsumer(StructuredMetadata):
    """
    FileConsumer Plugins keep information about the file that
    they are ingesting, namely absolute path and hash.
    """

    def __init__(self, filenames):
        super().__init__()
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

    def __init__(self, filenames, **kwargs):
        super().__init__(filenames)
        self.csv_data = {}
        self.reader_options = kwargs

    def pack_header(self) -> None:
        """ Set schema based on the CSV columns """
        column_names = list(self.file_info.keys()) + list(self.csv_data.keys())
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """ Adds a list containing one or more rows of the CSV along with file_info to output. """
        if not self.schema_is_set():
            # use Pandas to append all CSVs together as a
            # dataframe, then convert to dict
            total_df = DataFrame()
            for filename in self.filenames:
                temp_df = read_csv(filename)
                total_df = concat([total_df, temp_df])

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
        super().__init__(filenames)
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
                    file_content = fh.read()
                keyval_pairs = file_content.split('\n')
                # Remove blank lines from the file
                def _valid_line(x): return x != ''  # noqa
                drop_blank = list(filter(_valid_line, keyval_pairs))
                keyval_pairs = drop_blank
                # Each row contains a keyval pair
                for keyval_pair in keyval_pairs:
                    colon_split = keyval_pair.split(':', maxsplit=1)
                    # If a row does not have a : delimiter, raise error.
                    if len(colon_split) != 2:
                        raise TypeError
                    # Check if column already exists
                    if colon_split[0] not in self.bueno_data:
                        # Initialize empty column if first time seeing it
                        self.bueno_data[colon_split[0]] = [None] \
                            * len(self.filenames)
                    # Set the appropriate row index value for this keyval_pair
                    self.bueno_data[colon_split[0]][idx] = colon_split[1]
            self.pack_header()

        rows = list(self.bueno_data.values())
        self.add_to_output(rows)
        # Flatten multiple samples of the same file. 
        try:
            for col,rows in self.output_collector.items():
                self.output_collector[col] = rows[0]+rows[1]
        except IndexError:
            # First pass. Nothing to do.
            pass
