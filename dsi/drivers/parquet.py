import pyarrow as pa
from pyarrow import parquet as pq

from dsi.drivers.filesystem import Filesystem

class Parquet(Filesystem):
    """
    Support for a Parquet back-end Driver.

    Parquet is a convenient format when metadata are larger than SQLite supports.
    """

    def __init__(self, filename, **kwargs):
        super().__init__(filename=filename)
        self.filename=filename
        try:
            self.compression = kwargs['compression']
        except KeyError:
            self.compression = None
        
    def get_artifacts(self):
        """Get Parquet data from filename."""
        table = pq.read_table(self.filename)
        resout = table.to_pydict()
        return resout

    def put_artifacts(self, collection):
        """Put artifacts into file at filename path."""
        table = pa.table(collection)
        pq.write_table(table, self.filename, compression=self.compression)


