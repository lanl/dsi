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
    gufi_dsi_tag_tool_path = None
    files_with_uuids = None
    dsi_db = None
    isVerbose = False

    def __init__(self, gufi_prefix, gufi_index, dsi_table_name, dsi_columns, gufi_columns,
                 collection_name, gufi_dsi_tag_tool_path, files_with_uuids, dsi_db, verbose=False):
        '''
        `gufi_prefix`: the directory where GUFI is installed

        `gufi_index`: the directory where GUFI's indexes are

        `dsi_table_name`: the DSI table name that has the UUID for each file as a column

        `dsi_columns`: the DSI table columns that should be included in the join with GUFI

        `gufi_columns`: the GUFI columns that should be included in the join with DSI

        `collection_name`: the name that identifies the collection that the DSI database belongs to

        `gufi_dsi_tag_tool_path`: the path the gufi dsi tag tool

        `files_with_uuids`: a dictionary with LOCAL_PATH and UUID keys with a lists representing the local path and uuids

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
        self.gufi_dsi_tag_tool_path = gufi_dsi_tag_tool_path
        self.files_with_uuids = files_with_uuids
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

        self._run_gufi_dsi_tag_tool()
        metadata = []
<<<<<<< Updated upstream
=======
        print("sql query")
>>>>>>> Stashed changes
        with sqlite3.connect(":memory:") as con:
            con.enable_load_extension(True)

            # alternatively you can load the extension using an API call:
            con.load_extension(self.gufi_prefix + "/src/gufi_vt.so")

            # disable extension loading again
            con.enable_load_extension(False)

            username = os.environ["USER"]
            dsi_column_names = ",".join(self.dsi_columns + ['uuid'])
            all_columns = ",".join(self.dsi_columns +
                                   (["rpath(sname, sroll, name) AS fullpath"] + self.gufi_columns[1:]
                                    if self.gufi_columns[0] == "fullpath" else self.gufi_columns))
            query=f"""
            CREATE VIRTUAL TABLE uview USING gufi_vt(
                remote_cmd='ssh', 
                remote_arg='{username}@localhost', 
                threads=2, 
                E='SELECT {all_columns} FROM vrxpentries JOIN {self.collection_name}.{self.dsi_table_name} AS dsi ON dsi_uuid(xattr_name, xattr_value) == dsi.uuid;',
                setup_res_col_type="ATTACH ':memory:' AS '{self.collection_name}'; CREATE TABLE {self.collection_name}.{self.dsi_table_name} ({dsi_column_names});",
                index='{self.gufi_index}',
                plugin='{self.gufi_prefix}/contrib/plugins/libdsi_querying.so',
            );"""
            print("query: ", query)
            # example from SQLite wiki
            cur = con.execute(query)
            
            cur.execute(sqlstring)
            rows = cur.fetchall()
            for row in rows:
                print(row)
            
        return metadata

    def _run_gufi_dsi_tag_tool(self):
         # Runs the gufi tag command
        '''
        Calls the qufy_tag command to tag files
        files_with_uuids: a dict with keys LOCAL_PATH and UUID
        '''

        local_files = self.files_with_uuids['LOCAL_PATH']
        for idx, f in enumerate(local_files):
            if f is None:
                continue
<<<<<<< Updated upstream
            if self.files_with_uuids['UUID'][idx] is None:
                continue
            
=======
            print(idx, f)
            if self.files_with_uuids['UUID'][idx] is None:
                continue
            
#            print(self.dsi_db, self.gufi_dsi_tag_tool_path, self.collection_name, self.files_with_uuids)
>>>>>>> Stashed changes
            # Run the GUFI query command
            result = subprocess.run([self.gufi_dsi_tag_tool_path, "set", f, "--collection-id", self.collection_name,
                                     "--db-path", self.dsi_db, "--file-uuid", self.files_with_uuids['UUID'][idx]],
                                    capture_output=True, text=True)
            print("Result", result)
        return result.stdout

    def close(self):
        pass
