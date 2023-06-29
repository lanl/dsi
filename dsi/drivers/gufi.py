import subprocess

from dsi.drivers.filesystem import Filesystem

# Holds table name and data properties


class DataType:
    name = "DEFAULT"
    properties = {}
    units = {}


class Gufi(Filesystem):
    prefix = ""
    index = ""
    dbfile = ""
    table = ""
    column = ""
    isVerbose = False

    """
    prefix: prefix to GUFI commands
    index: directory with GUFI indexes
    dbfile: sqlite db file from DSI
    table: table name from the DSI db we want to join on
    column: column name from the DSI db to join on
    """

    def __init__(self, prefix, index, dbfile, table, column, verbose=False):
        super().__init__(dbfile)
        # prefix is the prefix to the GUFI installation
        self.prefix = prefix
        # index is the directory where the GUFI indexes are stored
        self.index = index
        # dbfile is the dsi database file that we wish to use
        self.dbfile = dbfile
        # table is the dsi database table name that we wish to use
        self.table = table
        # column is the dsi column name for a file's inode
        self.column = column

        self.isVerbose = verbose

    # Query GUFI and DSI db
    def get_artifacts(self, query):
        resout = self._run_gufi_query(query)
        if self.isVerbose:
            print(resout)

        return resout

    def put_artifacts(self, query):
        pass

    # Runs the gufi query command
    def _run_gufi_query(self, sqlstring):
        # Create the string for the -Q option that specifies the dsi db file,
        # the dsi db table name, and the dsi db inode column name.
        # qstr = self.dbfile + " " + self.table + " " + self.column + " inode"

        # Run the GUFI query command
        result = subprocess.run([self.prefix + "/gufi_query",
                                 "-d", "|", "-Q", self.dbfile, self.table, self.column,
                                 "inode", "-E", sqlstring, self.index], capture_output=True,
                                text=True)
        return result.stdout

    def close(self):
        pass