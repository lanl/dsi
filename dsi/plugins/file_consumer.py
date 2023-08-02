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
        self.csv_data = []
        self.csv_col_names = []
        self.reader_options = kwargs

    def pack_header(self) -> None:
        """ Set schema based on the CSV columns """
        column_names = ['metadata_source', 'metadata_source_sha'] + self.csv_col_names
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """ Adds a list containing one or more rows of the CSV along with file_info to output. """
        if not self.schema_is_set():
            # TODO: Csv FileConsumer only supports reading a single file. See filename index below.
            with open(self.filenames[0], 'r') as f:
                r = reader(f, **self.reader_options)
                for i, line in enumerate(r):
                    if i == 0:
                        self.csv_col_names = line
                    else:
                        self.csv_data.append([self.filenames[0]] + [self.file_info[self.filenames[0]]] + line)
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
        super().__init__(filenames)
        self.bueno_data = OrderedDict()

    def pack_header(self) -> None:
        """Set schema with POSIX and Bueno data."""
        column_names = list(self.bueno_data.keys())
        self.set_schema(column_names)

    def add_rows(self) -> None:
        """Parses Bueno data and adds a list containing 1 or more rows."""
        for idx, filename in enumerate(self.filenames):
            if not self.schema_is_set():
                with open(filename, 'r') as fh:
                    file_content = fh.read()
                keyval_pairs = file_content.split('\n')
                # Remove blank lines from the file
                _valid_line = lambda x: x != '' # noqa
                drop_blank = list(filter(_valid_line, keyval_pairs))
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
                        self.bueno_data[colon_split[0]] = [None] * len(self.filenames)
                    # Set the appropriate row index value for this keyval_pair
                    finally:
                        self.bueno_data[colon_split[0]][idx] = colon_split[1]
        self.pack_header()
        rows = list(self.bueno_data.values())
        self.add_to_output(rows)
