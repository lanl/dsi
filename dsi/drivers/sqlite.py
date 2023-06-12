import os
import sqlite3
import csv
from dsi.drivers.filesystem_driver import fsstore

# Holds table name and data properties 
class data_type:
    name = "DEFAULT"
    properties = {}
    units = {}

# Main storage class, interfaces with SQL
class store(fsstore):
    filename = "fs.db"
    table_name = "filesystem"
    datapath = ""
    file_list = {}
    con = None
    cur = None
    table_types = {}
    #Verbose
    isVerbose = 0
    
    def __init__(self, filename, datapath, file_list):
        super().__init__(fsstore.SQLITE_STORE)
        self.filename = filename
        self.datapath = datapath
        self.con = sqlite3.connect(filename)
        self.cur = self.con.cursor()
        self.create_table_types()
        self.create_filesystem_table()
        self.add_files(file_list)
        self.put_filesystem_metadata()

    # Closes connection to server
    def close(self):
        self.con.close()

    def add_files(self, file_list):
        # Do a quick validation of group permissions
        for file in file_list:
            #utils.isgroupreadable(file) # quick test
            filepath = os.path.join(self.datapath, file)
            st = os.stat(filepath)
            #print(os.stat(filepath).st_size)
            self.file_list[file] = st

    def create_table_types(self):
        self.table_types["file"] = fsstore.STRING
        self.table_types["st_mode"] = fsstore.DOUBLE
        self.table_types["st_ino"] = fsstore.DOUBLE
        self.table_types["st_dev"] = fsstore.DOUBLE
        self.table_types["st_nlink"] = fsstore.DOUBLE
        self.table_types["st_uid"] = fsstore.DOUBLE
        self.table_types["st_gid"] = fsstore.DOUBLE
        self.table_types["st_size"] = fsstore.DOUBLE
        self.table_types["st_atime"] = fsstore.DOUBLE
        self.table_types["st_mtime"] = fsstore.DOUBLE
        self.table_types["st_ctime"] = fsstore.DOUBLE

    # Adds columns to table and their types
    def create_filesystem_table(self):
        str_query = "CREATE TABLE IF NOT EXISTS " + self.table_name + " ( "
        for key, value in self.table_types.items():
            str_query = str_query + str(key) + " " + str(value)
            str_query = str_query +  ","

        str_query = str_query.rstrip(',')
        str_query = str_query + " )"

        if self.isVerbose:
            print(str_query)
        
        self.cur.execute(str_query)
        self.con.commit()

    # Adds rows to the columns defined previously
    def put_filesystem_metadata(self):
        for fname, st in self.file_list.items():
            str_query = "INSERT INTO " + self.table_name + " VALUES ( "
            str_query = str_query + " '" + fname + "' ,"
            for sval in st:
                str_query = str_query + " " + str(sval) + ","

            str_query = str_query.rstrip(',')
            str_query = str_query + " )"

            if self.isVerbose:
                print(str_query)
        
            self.cur.execute(str_query)
            self.con.commit()

    # ------- Query related functions -----
    # Raw sql query
    def sqlquery(self,query):
        if self.isVerbose:
            print(query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(query)
        resout = self.res.fetchall()

        if self.isVerbose:
            print(resout)

        return resout

    # Given an output of a sql query, reformat and write a csv of the subset data
    def export_csv(self,query,fname):
        if self.isVerbose:
            print(query)

        self.cur = self.con.cursor()
        cdata = self.con.execute(f'PRAGMA table_info({self.table_name});').fetchall()
        cnames = [entry[1] for entry in cdata]
        if self.isVerbose:
            print(cnames)

        with open(fname,"w+") as ocsv:
            csvWriter = csv.writer(ocsv,delimiter=',')
            csvWriter.writerow(cnames)

            for row in query:
                print(row)
                csvWriter.writerow(row)
        
        return 1

    # Query file name
    def query_fname(self, name ):
        str_query = "SELECT * FROM " + self.table_name + " WHERE file LIKE '%" + str(name) +"%'"
        if self.isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()
        
        if self.isVerbose:
            print(resout)

        return resout


    # Query file size
    def query_fsize(self, operator, size ):
        str_query = "SELECT * FROM " + table_name + " WHERE st_size " + str(operator) + " " + str(size)
        if self.isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()
        
        if self.isVerbose:
            print(resout)

        return resout

    # Query file creation time
    def query_fctime(self, operator, size ):
        str_query = "SELECT * FROM " + table_name + " WHERE st_ctime " + str(operator) + " " + str(size)
        if self.isVerbose:
            print(str_query)

        self.cur = self.con.cursor()
        self.res = self.cur.execute(str_query)
        resout = self.res.fetchall()
        
        if self.isVerbose:
            print(resout)

        return resout
