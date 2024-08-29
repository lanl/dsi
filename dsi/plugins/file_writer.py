from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json, csv
from math import isnan
import sqlite3
import subprocess
import os

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
        self.file_info = {}
        for filename in self.filenames:
            sha = sha1(open(filename, 'rb').read())
            self.file_info[abspath(filename)] = sha.hexdigest()

class ER_Diagram(FileWriter):

    def __init__(self, filenames, **kwargs):
        super().__init__(filenames, **kwargs)

    def export_erd(self, dbname, fname):
        """
        Function that outputs a dot file for the given database.

        `dbname`: database to create an ER diagram for

        `fname`: name (including path) of the png file that contains the generated ER diagram

        `return`: none
        """
        db = sqlite3.connect(dbname)
        
        # if fname[-4:] == ".dot":
        #     fname = fname[:-4]

        file_type = ".png"
        if fname[-4:] == ".png" or fname[-4:] == ".pdf" or fname[-4:] == ".jpg":
            file_type = fname[-4:]
            fname = fname[:-4]
        elif fname[-5:] == ".jpeg":
            file_type = fname[-5:]
            fname = fname[:-5]

        # if fname[-4:] == ".dot":
        #     fname = fname[:-4]
        dot_file = open(fname + ".dot", "w")

        numColsERD = 1

        dot_file.write("digraph sqliteschema { ")
        dot_file.write("node [shape=plaintext]; ")
        dot_file.write("rankdir=LR ")
        dot_file.write("splines=true ")
        dot_file.write("overlap=false ")

        list_db_tbls = "SELECT tbl_name, NULL AS label, NULL AS color, NULL AS clusterid FROM sqlite_master WHERE type='table'"
        try:    
            tbl_list_stmt = db.execute(list_db_tbls)
        except sqlite3.Error as er:
            dot_file.write(er.sqlite_errorname)
            dot_file.write("Can't prepare table list statement")
            db.close()
            dot_file.close()

        for row in tbl_list_stmt:
            tbl_name = row[0]

            tbl_info_sql = f"PRAGMA table_info({tbl_name})"
            try:
                tbl_info_stmt = db.execute(tbl_info_sql)
            except sqlite3.Error as er:
                dot_file.write(er.sqlite_errorname)
                dot_file.write(f"Can't prepare table info statement on table {tbl_name}") 
                db.close()
                dot_file.close()

            dot_file.write(f"{tbl_name} [label=<<TABLE CELLSPACING=\"0\"><TR><TD COLSPAN=\"{numColsERD}\"><B>{tbl_name}</B></TD></TR>")

            curr_row = 0
            inner_brace = 0
            for info_row in tbl_info_stmt:
                if curr_row % numColsERD == 0:
                    inner_brace = 1
                    dot_file.write("<TR>")

                dot_file.write(f"<TD PORT=\"{info_row[1]}\">{info_row[1]}</TD>")
                curr_row += 1
                if curr_row % numColsERD == 0:
                    inner_brace = 0
                    dot_file.write("</TR>")

            if inner_brace:
                dot_file.write("</TR>")
            dot_file.write("</TABLE>>]; ")

        tbl_list_stmt = db.execute(list_db_tbls)
        for row in tbl_list_stmt:
            tbl_name = row[0]

            fkey_info_sql = f"PRAGMA foreign_key_list({tbl_name})"
            try:
                fkey_info_stmt = db.execute(fkey_info_sql)
            except sqlite3.Error as er:
                dot_file.write(er.sqlite_errorname)
                dot_file.write(f"Can't prepare foreign key statement on table {tbl_name}")
                db.close()
                dot_file.close()

            for fkey_row in fkey_info_stmt:
                dot_file.write(f"{tbl_name}:{fkey_row[3]} -> {fkey_row[2]}:{fkey_row[4]}; ")

        dot_file.write("}")
        db.close()
        dot_file.close()

        subprocess.run(["dot", "-T", file_type[1:], "-o", fname + file_type, fname + ".dot"])
        os.remove(fname + ".dot")

class Csv(FileWriter):
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
