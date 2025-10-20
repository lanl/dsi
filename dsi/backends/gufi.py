import subprocess


# Holds table name and data properties
from dsi.backends.filesystem import Filesystem


class DataType:
    name = "DEFAULT"
    properties = {}
    units = {}


class Gufi(Filesystem):
    '''
    GUFI Datastore
    '''
    prefix = ""
    index = ""
    dbfile = ""
    table = ""
    column = ""
    isVerbose = False

    def __init__(self, prefix, index, dbfile, table, column, verbose=False):
        '''
        `prefix`: prefix to GUFI commands

        `index`: directory with GUFI indexes

        `dbfile`: sqlite db file from DSI

        `table`: table name from the DSI db we want to join on

        `column`: column name from the DSI db to join on

        `verbose`: print debugging statements or not
        '''

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

    # OLD NAME OF query_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def get_artifacts(self, query):
        return self.query_artifacts(query)
    
    # Query GUFI and DSI db
    def query_artifacts(self, query):
        '''
        Retrieves GUFI's metadata joined with a dsi database
        query: an sql query into the dsi_entries table
        '''

        resout = self._run_gufi_query(query)
        if self.isVerbose:
            print(resout)

        return resout

    # OLD NAME OF ingest_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def put_artifacts(self, query):
        return self.ingest_artifacts(query)
    
    def ingest_artifacts(self, query):
        pass

    # Runs the gufi query command
    def _run_gufi_query(self, sqlstring):
        '''
        Calls the qufy_query command to run the sql query
        sqlstring: the query into the dsi_entries table
        '''

        # Create the string for the -Q option that specifies the dsi db file,
        # the dsi db table name, and the dsi db inode column name.
        _ = self.dbfile + " " + self.table + " " + self.column + " inode"

        # Run the GUFI query command
        result = subprocess.run([self.prefix + "/gufi_query", "-d", "|", "-Q", self.dbfile,
                                self.table, self.column, "inode", "-E", sqlstring, self.index],
                                capture_output=True, text=True)
        return result.stdout

    def close(self):
        pass
