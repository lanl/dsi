import sqlite3
import os

# Holds table name and data properties
from dsi.backends.filesystem import Filesystem
from pathlib import Path
import subprocess

class DataType:
    name = "DEFAULT"
    properties = {}
    units = {}


class Gufi(Filesystem):
    '''
    GUFI Datastore
    '''
    gufi_prefix = ""
    gufi_index = ""
    dsi_table_name = ""
    dsi_columns = ""
    gufi_columns = ""
    collection_name = ""
    dsi_db = None
    isVerbose = False

    def __init__(self, gufi_prefix, gufi_index, dsi_table_name, dsi_columns, gufi_columns,
                 collection_name, dsi_db, verbose=False):
        '''
        `gufi_prefix`: the directory where GUFI is installed

        `gufi_index`: the directory where GUFI's indexes are

        `dsi_table_name`: the DSI table name that has the UUID for each file as a column

        `dsi_columns`: the DSI table columns that should be included in the join with GUFI

        `gufi_columns`: the GUFI columns that should be included in the join with DSI

        `collection_name`: the name that identifies the collection that the DSI database belongs to

        `dsi_db`: the path to the dsi db

        `verbose`: print debugging statements or not
        '''

        # prefix is the prefix to the GUFI installation
        self.gufi_prefix = gufi_prefix
        self.gufi_index = gufi_index
        self.dsi_table_name = dsi_table_name
        self.dsi_columns = dsi_columns
        self.gufi_columns = gufi_columns
        self.collection_name = collection_name
        self.dsi_db = dsi_db
        self.isVerbose = verbose

    # OLD NAME OF query_artifacts(). TO BE DEPRECATED IN FUTURE DSI RELEASE
    def get_artifacts(self, query):
        return self.query_artifacts(query)
    
    # Query GUFI and DSI db ˘
    def query_artifacts(self, query):
        '''
        Retrieves GUFI's metadata joined with a dsi database
        query: an sql query into the dsi_entries table
        '''

        try:
            resout = self._run_gufi_query(query)
            if self.isVerbose:
                print(resout)

            return resout
        except:
            print("Error running GUFI query")

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

        metadata = []
        with sqlite3.connect(":memory:") as con:
            con.enable_load_extension(True)

            # alternatively you can load the extension using an API call:
            con.load_extension(self.gufi_prefix + "/lib/gufi_vt")

            # disable extension loading again
            con.enable_load_extension(False)

            username = os.environ["USER"]
            dsi_column_names = ",".join(self.dsi_columns)
            gufi_column_names = ",".join((["rpath(sname, sroll, name) AS fullpath"] + self.gufi_columns[1:]
                                    if self.gufi_columns[0] == "fullpath" else self.gufi_columns))
#            query=f"""
#            CREATE VIRTUAL TABLE uview USING gufi_vt(
#                threads=2, 
#                E='SELECT rpath(sname, sroll, name), xattr_name, xattr_value FROM vrxpentries;',
#                setup_res_col_type="ATTACH ':memory:' AS '{self.collection_name}'; CREATE TABLE {self.collection_name}.{self.dsi_table_name} ({dsi_column_names});",
#                index='{self.gufi_index}',
#                plugin='gufi_plugin_operations:/usr/projects/systems/gufi/lib/libdsi_querying.so'
#            );"""

            query=f"""
            CREATE VIRTUAL TABLE uview USING gufi_vt(
                threads=64,
                E="SELECT {gufi_column_names}, dsi_uuid(xattr_name, xattr_value) AS uuid FROM vrxpentries WHERE uuid IS NOT NULL;",
                index='{self.gufi_index}',
                plugin='gufi_plugin_operations:{self.gufi_prefix}/lib/libdsi_querying.so'
            );
            """

            print("query: ", query)
            # example from SQLite wiki
            cur = con.execute(query)
            query=f"""
                ATTACH '{self.dsi_db}' AS
                {self.collection_name};
            """
            cur = con.execute(query)
            print("query: ", query)

            query = f"""
                SELECT uview.*, {dsi_column_names} FROM uview JOIN ATLAS_UUID.zarr_metadata_uuid ON uview.uuid == ATLAS_UUID.zarr_metadata_uuid.uuid;
            """
            cur.execute(query)
            print("query: ", query)
            rows = cur.fetchall()
            for row in rows:
                print(row)
                metadata.append(row)

        return metadata

    def close(self):
        pass
    def display(self):
        pass
    def find(self):
        pass
    def find_cell(self):
        pass
    def find_column(self):
        pass
    def find_relation(self):
        pass
    def find_table(self):
        pass
    def get_schema(self):
        pass
    def get_table(self):
        pass
    def get_table_names(self):
        pass
    def list(self):
        pass
    def notebook(self):
        pass
    def num_tables(self):
        pass
    def overwrite_table(self):
        pass
    def process_artifacts(self):
        pass
    def summary(self):
        pass
