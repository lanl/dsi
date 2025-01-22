import pandas as pd
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

class ER_Diagram(FileWriter):

    def __init__(self, filename, target_table_prefix = None, **kwargs):
        super().__init__(filename, **kwargs)
        self.output_filename = filename
        self.target_table_prefix = target_table_prefix

    def get_rows(self, collection) -> None:
        file_type = ".png"
        if self.output_filename[-4:] == ".png" or self.output_filename[-4:] == ".pdf" or self.output_filename[-4:] == ".jpg":
            file_type = self.output_filename[-4:]
            self.output_filename = self.output_filename[:-4]
        elif self.output_filename[-5:] == ".jpeg":
            file_type = self.output_filename[-5:]
            self.output_filename = self.output_filename[:-5]

        if self.target_table_prefix is not None and not any(self.target_table_prefix in element for element in collection.keys()):
            raise ValueError("Your input for target_table_prefix does not exist in the database. Please enter a valid prefix for table names.")
        
        manual_dot = False
        try: from graphviz import Digraph
        except ModuleNotFoundError: 
            manual_dot = True
            import subprocess
            import os

        if manual_dot:
            dot_file = open(self.output_filename + ".dot", "w")
            dot_file.write("digraph workflow_schema { ")
            if self.target_table_prefix is not None:
                dot_file.write(f'label="ER Diagram for {self.target_table_prefix} tables"; labelloc="t"; ')
            dot_file.write("node [shape=plaintext]; dpi=300 rankdir=LR splines=true overlap=false ")
        else:
            dot = Digraph('workflow_schema', format = file_type[1:])
            if self.target_table_prefix is not None:
                dot.attr(label = f'ER Diagram for {self.target_table_prefix} tables', labelloc='t')
            dot.attr('node', shape='plaintext')
            dot.attr(dpi='300', rankdir='LR', splines='true', overlap='false')

        num_tbl_cols = 1
        for tableName, tableData in collection.items():
            if tableName == "dsi_relations" or tableName == "sqlite_sequence":
                continue
            elif self.target_table_prefix is not None and self.target_table_prefix not in tableName:
                continue
            
            html_table = ""
            if manual_dot:
                html_table = f"{tableName} [label="
            html_table += f"<<TABLE CELLSPACING=\"0\"><TR><TD COLSPAN=\"{num_tbl_cols}\"><B>{tableName}</B></TD></TR>"
            
            col_list = tableData.keys()
            if tableName == "dsi_units":
                col_list = ["table_name", "column_and_unit"]
            curr_row = 0
            inner_brace = 0
            for col_name in col_list:
                if curr_row % num_tbl_cols == 0:
                    inner_brace = 1
                    html_table += "<TR>"
                html_table += f"<TD PORT=\"{col_name}\">{col_name}</TD>"
                curr_row += 1
                if curr_row % num_tbl_cols == 0:
                    inner_brace = 0
                    html_table += "</TR>"
            
            if inner_brace:
                html_table += "</TR>"
            html_table += "</TABLE>>"

            if manual_dot: dot_file.write(html_table+"]; ")
            else: dot.node(tableName, label = html_table)

        for f_table, f_col in collection["dsi_relations"]["foreign_key"]:
            if self.target_table_prefix is not None and self.target_table_prefix not in f_table:
                continue
            if f_table != "NULL":
                foreignIndex = collection["dsi_relations"]["foreign_key"].index((f_table, f_col))
                fk_string = f"{f_table}:{f_col}"
                pk_string = f"{collection['dsi_relations']['primary_key'][foreignIndex][0]}:{collection['dsi_relations']['primary_key'][foreignIndex][1]}"
                
                if manual_dot: dot_file.write(f"{fk_string} -> {pk_string}; ")
                else: dot.edge(fk_string, pk_string)

        if manual_dot:
            dot_file.write("}")
            dot_file.close()
            subprocess.run(["dot", "-T", file_type[1:], "-o", self.output_filename + file_type, self.output_filename + ".dot"])
            os.remove(self.output_filename + ".dot")
        else:
            dot.render(self.output_filename, cleanup=True)

class Csv_Writer(FileWriter):
    """
    A Plugin to output queries as CSV data
    """
    def __init__(self, table_name, filename, cols_to_export = None, **kwargs):
        '''
        `table_name`: name of table to be exported to a csv
        `filename`: name of the output file where the table will be stored
        '''
        super().__init__(filename, **kwargs)
        self.csv_file_name = filename
        self.table_name = table_name
        self.export_cols = cols_to_export

    def get_rows(self, collection) -> None:
        if self.table_name not in collection.keys():
            raise ValueError(f"{self.table_name} does not exist in this database")
        
        df = pd.DataFrame(collection[self.table_name])
        
        if self.export_cols is not None:
            try:
                df = df.iloc[:, self.export_cols]
            except:
                try:
                    df = df[self.export_cols]
                except:
                    raise ValueError(f"Could not export to csv as specified column input {self.export_cols} is incorrect")
        df.to_csv(self.csv_file_name, index=False)

class Table_Plot(FileWriter):
    '''
    Plugin that plots all numeric column data for a specified table
    '''
    def __init__(self, table_name, filename, display_cols = None, **kwargs):
        '''
        `table_name`: name of table to be plotted
        `filename`: name of output file that plot be stored in
        '''
        super().__init__(filename, **kwargs)
        self.output_name = filename
        self.table_name = table_name
        self.display_cols = display_cols

    def get_rows(self, collection) -> None:
        if self.table_name not in collection.keys():
            raise ValueError(f"{self.table_name} not in the dataset")
        if self.display_cols is not None and not set(self.display_cols).issubset(set(collection[self.table_name].keys())):
            raise ValueError(f"Inputted list of columns to plot for {self.table_name} is incorrect")
        
        numeric_cols = []
        col_len = None
        for colName, colData in collection[self.table_name].items():
            if colName == "run_id" or (self.display_cols is not None and colName not in self.display_cols):
                continue
            if col_len == None:
                col_len = len(colData)
            if isinstance(colData[0], str) == False:
                unit_tuple = "NULL"
                if "dsi_units" in collection.keys() and self.table_name in collection["dsi_units"].keys() and colName in collection["dsi_units"][self.table_name].keys():
                    unit_tuple = collection["dsi_units"][self.table_name][colName]
                    # unit_tuple = next((unit for col, unit in collection["dsi_units"][self.table_name].items() if col == colName), "NULL")
                if unit_tuple != "NULL":
                    numeric_cols.append((colName + f" ({unit_tuple})", colData))
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