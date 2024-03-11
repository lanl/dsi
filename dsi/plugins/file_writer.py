from collections import OrderedDict
from os.path import abspath
from hashlib import sha1
import json, csv
from math import isnan

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
