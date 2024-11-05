from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json, csv
from math import isnan
import sqlite3
import subprocess
import os
from matplotlib import pyplot as plt

from dsi.plugins.metadata import StructuredMetadata

class FileWriter(StructuredMetadata):
    """
    FileWriter Plugins keep information about the file that
    they are ingesting, namely absolute path and hash.
    """
    def __init__(self, filenames, **kwargs):
        super().__init__(**kwargs)
        if type(filenames) == str:
            self.filenames = [filenames]
        elif type(filenames) == list:
            self.filenames = filenames
        else:
            raise TypeError
        
        '''self.file_info = {}
        for filename in self.filenames:
            sha = sha1(open(filename, 'rb').read())
            self.file_info[abspath(filename)] = sha.hexdigest()'''

class ER_Diagram(FileWriter):

    def __init__(self, filename, dbname = None, target_table_prefix = None, **kwargs):
        super().__init__(filename, **kwargs)
        self.output_filename = filename
        self.target_table_prefix = target_table_prefix
        #COMMENT OUT DBNAME VARIABLE ONCE DELETING export_to_erd
        self.dbname = dbname

    def get_rows(self, collection) -> None:
        """
        Function that outputs a ER diagram for the given database.

        `dbname`: database to create an ER diagram for

        `fname`: name (including path) of the image file that contains the generated ER diagram - default png if not specified

        `return`: none
        """
        # if self.dbname is not None:
        #     self.export_erd(self.dbname, self.output_filename)
        #     return

        # else:
        file_type = ".png"
        if self.output_filename[-4:] == ".png" or self.output_filename[-4:] == ".pdf" or self.output_filename[-4:] == ".jpg":
            file_type = self.output_filename[-4:]
            self.output_filename = self.output_filename[:-4]
        elif self.output_filename[-5:] == ".jpeg":
            file_type = self.output_filename[-5:]
            self.output_filename = self.output_filename[:-5]

        if self.target_table_prefix is not None and not any(self.target_table_prefix in element for element in collection.keys()):
            raise ValueError("Your input for target_table_prefix does not exist in the database. Please enter a valid prefix for table names.")

        dot_file = open(self.output_filename + ".dot", "w")

        num_tbl_cols = 1
        dot_file.write("digraph workflow_schema { ")
        if self.target_table_prefix is not None:
            dot_file.write(f'label="ER Diagram for {self.target_table_prefix} tables"; ')
            dot_file.write('labelloc="t"; ')
        dot_file.write("node [shape=plaintext]; ")
        dot_file.write("rankdir=LR ")
        dot_file.write("splines=true ")
        dot_file.write("overlap=false ")

        for tableName, tableData in collection.items():
            if tableName == "dsi_relations" or (self.target_table_prefix is not None and self.target_table_prefix not in tableName):
                continue

            dot_file.write(f"{tableName} [label=<<TABLE CELLSPACING=\"0\"><TR><TD COLSPAN=\"{num_tbl_cols}\"><B>{tableName}</B></TD></TR>")

            curr_row = 0
            inner_brace = 0
            for col_name in tableData.keys():
                if curr_row % num_tbl_cols == 0:
                    inner_brace = 1
                    dot_file.write("<TR>")

                dot_file.write(f"<TD PORT=\"{col_name}\">{col_name}</TD>")
                curr_row += 1
                if curr_row % num_tbl_cols == 0:
                    inner_brace = 0
                    dot_file.write("</TR>")

            if inner_brace:
                dot_file.write("</TR>")
            dot_file.write("</TABLE>>]; ")

        for f_table, f_col in collection["dsi_relations"]["foreign_key"]:
            if self.target_table_prefix is not None and self.target_table_prefix not in f_table:
                continue
            if f_table != "NULL":
                foreignIndex = collection["dsi_relations"]["foreign_key"].index((f_table, f_col))
                dot_file.write(f"{f_table}:{f_col} -> {collection['dsi_relations']['primary_key'][foreignIndex][0]}: {collection['dsi_relations']['primary_key'][foreignIndex][1]}; ")

        dot_file.write("}")
        dot_file.close()

        subprocess.run(["dot", "-T", file_type[1:], "-o", self.output_filename + file_type, self.output_filename + ".dot"])
        os.remove(self.output_filename + ".dot")
    
    # def export_erd(self, dbname, fname):
    #     """
    #     Function that outputs a ER diagram for the given database.

    #     `dbname`: database to create an ER diagram for

    #     `fname`: name (including path) of the image file that contains the generated ER diagram - default png if not specified

    #     `return`: none
    #     """
        
    #     db = sqlite3.connect(dbname)

    #     file_type = ".png"
    #     if fname[-4:] == ".png" or fname[-4:] == ".pdf" or fname[-4:] == ".jpg":
    #         file_type = fname[-4:]
    #         fname = fname[:-4]
    #     elif fname[-5:] == ".jpeg":
    #         file_type = fname[-5:]
    #         fname = fname[:-5]

    #     dot_file = open(fname + ".dot", "w")

    #     numColsERD = 1

    #     dot_file.write("digraph sqliteschema { ")
    #     dot_file.write("node [shape=plaintext]; ")
    #     dot_file.write("rankdir=LR ")
    #     dot_file.write("splines=true ")
    #     dot_file.write("overlap=false ")

    #     list_db_tbls = "SELECT tbl_name, NULL AS label, NULL AS color, NULL AS clusterid FROM sqlite_master WHERE type='table'"
    #     try:    
    #         tbl_list_stmt = db.execute(list_db_tbls)
    #     except sqlite3.Error as er:
    #         dot_file.write(er.sqlite_errorname)
    #         dot_file.write("Can't prepare table list statement")
    #         db.close()
    #         dot_file.close()

    #     for row in tbl_list_stmt:
    #         tbl_name = row[0]

    #         tbl_info_sql = f"PRAGMA table_info({tbl_name})"
    #         try:
    #             tbl_info_stmt = db.execute(tbl_info_sql)
    #         except sqlite3.Error as er:
    #             dot_file.write(er.sqlite_errorname)
    #             dot_file.write(f"Can't prepare table info statement on table {tbl_name}") 
    #             db.close()
    #             dot_file.close()

    #         dot_file.write(f"{tbl_name} [label=<<TABLE CELLSPACING=\"0\"><TR><TD COLSPAN=\"{numColsERD}\"><B>{tbl_name}</B></TD></TR>")

    #         curr_row = 0
    #         inner_brace = 0
    #         for info_row in tbl_info_stmt:
    #             if curr_row % numColsERD == 0:
    #                 inner_brace = 1
    #                 dot_file.write("<TR>")

    #             dot_file.write(f"<TD PORT=\"{info_row[1]}\">{info_row[1]}</TD>")
    #             curr_row += 1
    #             if curr_row % numColsERD == 0:
    #                 inner_brace = 0
    #                 dot_file.write("</TR>")

    #         if inner_brace:
    #             dot_file.write("</TR>")
    #         dot_file.write("</TABLE>>]; ")

    #     tbl_list_stmt = db.execute(list_db_tbls)
    #     for row in tbl_list_stmt:
    #         tbl_name = row[0]

    #         fkey_info_sql = f"PRAGMA foreign_key_list({tbl_name})"
    #         try:
    #             fkey_info_stmt = db.execute(fkey_info_sql)
    #         except sqlite3.Error as er:
    #             dot_file.write(er.sqlite_errorname)
    #             dot_file.write(f"Can't prepare foreign key statement on table {tbl_name}")
    #             db.close()
    #             dot_file.close()

    #         for fkey_row in fkey_info_stmt:
    #             dot_file.write(f"{tbl_name}:{fkey_row[3]} -> {fkey_row[2]}:{fkey_row[4]}; ")

    #     dot_file.write("}")
    #     db.close()
    #     dot_file.close()

    #     subprocess.run(["dot", "-T", file_type[1:], "-o", fname + file_type, fname + ".dot"])
    #     os.remove(fname + ".dot")

class Csv_Writer(FileWriter):
    """
    A Plugin to output queries as CSV data
    """

    # This turns on strict_mode when reading in multiple csv files that need matching schemas.
    # Default value is False.
    strict_mode = False

    def __init__(self, filenames, **kwargs):
        super().__init__(filenames, **kwargs)
        self.csv_data = {}

    def pack_header(self) -> None:
        """ Set schema based on the CSV columns """

        column_names = list(self.file_info.keys()) + list(self.csv_data.keys())
        self.set_schema(column_names)

    # Given an output of a sql query, reformat and write a csv of the subset data
    def export_csv_query(self,query,fname):
        """
        Function that outputs a csv file of a return query, given an initial query.

        `query`: raw SQL query to be executed on current table

        `fname`: target filename (including path) that will output the return query as a csv file

        `return`: none
        """
        #if isVerbose:
        #   print(query)

        # Parse the table out of the query
        tname = query.split("FROM ",1)[1]
        tname = tname[:-1]

        self.cur = self.con.cursor()
        cdata = self.con.execute(f'PRAGMA table_info({tname});').fetchall()
        cnames = [entry[1] for entry in cdata]
        #if isVerbose:
        #    print(cnames)

        with open(fname,"w+") as ocsv:
            csvWriter = csv.writer(ocsv,delimiter=',')
            csvWriter.writerow(cnames)

            for row in query:
                print(row)
                csvWriter.writerow(row)
        
        return 1
    
    # Given an output of a list, reformat and write a csv of the subset data
    def export_csv(self,qlist,tname,fname):
        """
        Function that outputs a csv file of a return query, given an initial query.

        `qlist`: a python list to be executed on current table

        `tname`: a sql table name that originated qlist

        `fname`: target filename (including path) that will output the return query as a csv file

        `return`: none
        """

        self.cur = self.con.cursor()
        cdata = self.con.execute(f'PRAGMA table_info({tname});').fetchall()
        cnames = [entry[1] for entry in cdata]

        with open(fname,"w+") as ocsv:
            csvWriter = csv.writer(ocsv,delimiter=',')
            csvWriter.writerow(cnames)

            for row in qlist:
                print(row)
                csvWriter.writerow(row)
        
        return 1

class Table_Plot(FileWriter):
    '''
    Plugin that plots all numeric column data for a specified table
    '''
    def __init__(self, table_name, filename, **kwargs):
        '''
        `table_name`: name of table to be plotted
        `filename`: name of output file that plot be stored in
        '''
        super().__init__(filename, **kwargs)
        self.output_name = filename
        self.table_name = table_name

    def get_rows(self, collection) -> None:
        numeric_cols = []
        col_len = None
        for colName, colData in collection[self.table_name].items():
            if col_len == None:
                col_len = len(colData)
            if isinstance(colData[0], str) == False:
                if self.table_name + "_units" in collection.keys() and collection[self.table_name + "_units"][colName][0] != "NULL":
                    numeric_cols.append((colName + f" ({collection[self.table_name + '_units'][colName][0]})", colData))
                else:
                    numeric_cols.append((colName, colData))

        sim_list = list(range(1, col_len + 1))

        for colName, colData in numeric_cols:
            plt.plot(sim_list, colData, label = colName)
        plt.xticks(sim_list)
        plt.xlabel("Sim Number")
        plt.ylabel("Values")
        plt.title(f"{self.table_name} Values")
        plt.legend()
        plt.savefig(f"{self.table_name} Values", bbox_inches='tight')