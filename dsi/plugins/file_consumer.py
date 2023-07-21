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


class CSV(FileConsumer):
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

    def add_row(self) -> None:
        """ Adds each row of the CSV along with file_info to output. """
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
            row = list(self.file_info.values()) + line
            self.add_to_output(row)


class Bueno(FileConsumer):
    """
    A Plugin to capture performance data from Bueno (github.com/lanl/bueno)

    Bueno outputs performance data in keyvalue pairs in a file. Keys and values
    are delimited by ``:``. Keyval pairs are delimited by ``\\n``.
    """

    def __init__(self, filename, **kwargs) -> None:
        super().__init__(filename)
        self.bueno_data = OrderedDict()

    def pack_header(self) -> None:
        """Set schema with Bueno data."""
        column_names = list(self.file_info.keys()) + \
            list(self.bueno_data.keys())
        self.set_schema(column_names)

    def add_row(self) -> None:
        """Parses Bueno data and adds the row."""
        if not self.schema_is_set():
            with open(self.filename, 'r') as fh:
                file_content = (fh.read())
            rows = file_content.split('\n')
            drop_rows = [row for row in rows if row != '']
            rows = drop_rows
            for row in rows:
                colon_split = row.split(':', maxsplit=1)
                if len(colon_split) != 2:
                    raise TypeError
                self.bueno_data[colon_split[0]] = colon_split[1]
            self.pack_header()

        row = list(self.file_info.values()) + \
            list(self.bueno_data.values())
        self.add_to_output(row)
