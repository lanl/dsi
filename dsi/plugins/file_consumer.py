from collections import OrderedDict
from csv import reader
from os.path import abspath
from hashlib import sha1

from dsi.plugins.metadata import StructuredMetadata


class FileConsumer(StructuredMetadata):
    """
    FileConsumer Plugins keep information about the file that
    they are ingesting, namely absolute path and hash.
    """

    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.file_info = OrderedDict()
        self.file_info['abspath'] = abspath(filename)
        sha = sha1(open(filename, 'rb').read())
        self.file_info['sha1'] = sha.hexdigest()


class Csv(FileConsumer):
    """
    A Plugin to ingest CSV data
    """

    def __init__(self, filename, **kwargs):
        super().__init__(filename)
        self.csv_data = []
        self.csv_col_names = []
        self.reader_options = kwargs

    def pack_header(self) -> None:
        """ Set schema based on the CSV columns """
        column_names = list(self.file_info.keys()) + self.csv_col_names
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """ Adds a list containing one or more rows of the CSV along with file_info to output. """
        if not self.schema_is_set():
            with open(self.filename, 'r') as f:
                r = reader(f, **self.reader_options)
                for i, line in enumerate(r):
                    if i == 0:
                        self.csv_col_names = line
                    else:
                        self.csv_data.append(line)
            self.pack_header()

        for line in self.csv_data:
            rows = list(self.file_info.values()) + line
            self.add_to_output(rows)


class Bueno(FileConsumer):
    """
    A Plugin to capture performance data from Bueno (github.com/lanl/bueno)

    Bueno outputs performance data in keyvalue pairs in a file. Keys and values
    are delimited by ``:``. Keyval pairs are delimited by ``\\n``.
    """
    def __init__(self, filenames, **kwargs) -> None:
        super().__init__()
        self.bueno_data = OrderedDict()
        if type(filenames)==str:
            self.filenames = [filenames]
        elif type(filenames)==list:
            self.filenames = filenames
        else:
            raise TypeError

    def pack_header(self) -> None:
        """Set schema with POSIX and Bueno data."""
        column_names = list(self.bueno_data.keys())
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """Parses Bueno data and adds a list containing 1 or more rows."""
        for idx,filename in enumerate(self.filenames):
            if not self.schema_is_set():
                with open(filename, 'r') as fh:
                    file_content = fh.read()
                keyval_pairs = file_content.split('\n')
                # Remove blank lines from the file
                def _valid_line(x):
                    return(x != '')
                drop_blank = list(filter(_valid_line,keyval_pairs))
                keyval_pairs = drop_blank
                # Each row contains a keyval pair
                for keyval_pair in keyval_pairs:
                    colon_split = keyval_pair.split(':', maxsplit=1)
                    # If a row does not have a : delimiter, raise error.
                    if len(colon_split) != 2:
                        raise TypeError
                    # Check if column already exists
                    try:
                        self.bueno_data[colon_split[0]]
                    # Initialize empty column if first time seeing it
                    except KeyError:
                        self.bueno_data[colon_split[0]] = [None]*len(self.filenames)
                    # Set the appropriate row index value for this keyval_pair
                    finally:
                        self.bueno_data[colon_split[0]][idx]= colon_split[1]
            self.pack_header()
        rows = list(self.bueno_data.values())
        self.add_to_output(rows)

