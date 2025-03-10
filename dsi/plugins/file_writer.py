import pandas as pd
from matplotlib import pyplot as plt

from dsi.plugins.metadata import StructuredMetadata

class FileWriter(StructuredMetadata):
    """
    FileWriter Plugins keep information about the file that they are ingesting, namely absolute path and hash.
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
    """
    Plugin that generates an ER Diagram from the current data in the DSI abstraction
    """

    def __init__(self, filename, target_table_prefix = None, **kwargs):
        """
        `filename`: file name of the ER Diagram to be generated

        `target_table_prefix`: if generating diagram for only a select set of tables, can specify prefix to search for all alike tables

            - Ex: prefix = "student" so only "student__address", "student__math", "student__physics" tables are displayed here
        """
        super().__init__(filename, **kwargs)
        self.output_filename = filename
        self.target_table_prefix = target_table_prefix

    def get_rows(self, collection) -> None:
        """
        Function called in core.py that generates the ER Diagram.

        `collection`: representation of internal DSI abstraction. It is a nested Ordered Dict, with table names as keys, and table data as Ordered Dicts
        
        `return`: None. Only returns if error. Message is sent back to core to print along with error type. Ex: (ValueError, "error message")
        """
        file_type = ".png"
        if len(self.output_filename) > 4 and self.output_filename[-4:] in [".png", ".pdf", ".jpg"]:
            file_type = self.output_filename[-4:]
            self.output_filename = self.output_filename[:-4]
        elif len(self.output_filename) > 5 and self.output_filename[-5:] == ".jpeg":
            file_type = self.output_filename[-5:]
            self.output_filename = self.output_filename[:-5]
        elif len(self.output_filename) > 4 and self.output_filename[-4:] == ".svg":
            return (ValueError, "ER Diagram writer cannot generate a .SVG file due to issue with graphviz")

        if self.target_table_prefix is not None and not any(self.target_table_prefix in element for element in collection.keys()):
            return (ValueError, "Your input for target_table_prefix does not exist in memory. Please enter a valid prefix for table names.")
        
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
            if f_table != None:
                foreignIndex = collection["dsi_relations"]["foreign_key"].index((f_table, f_col))
                fk_string = f"{f_table}:{f_col}"
                pk_string = f"{collection['dsi_relations']['primary_key'][foreignIndex][0]}:{collection['dsi_relations']['primary_key'][foreignIndex][1]}"
                
                if manual_dot: dot_file.write(f"{pk_string} -> {fk_string}; ")
                else: dot.edge(pk_string, fk_string)

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
    def __init__(self, table_name, filename, export_cols = None, **kwargs):
        """
        `table_name`: name of table to be exported to a csv

        `filename`: name of the CSV file that will be generated

        `export_cols`: default None. When specified, this must be a list of column names to keep in output csv file

            - Ex: all columns are [a, b, c, d, e]. export_cols = [a, c, e]
        """
        super().__init__(filename, **kwargs)
        self.csv_file_name = filename
        self.table_name = table_name
        self.export_cols = export_cols

    def get_rows(self, collection) -> None:
        """
        Function called in core.py that generates the output CSV file.

        `collection`: representation of internal DSI abstraction. It is a nested Ordered Dict, with table names as keys, and table data as Ordered Dicts

        `return`: None. Only returns if error. Message is sent back to core to print along with error type. Ex: (ValueError, "error message")
        """
        if self.table_name not in collection.keys():
            return (ValueError, f"{self.table_name} does not exist in memory")
        if self.export_cols is not None and not set(self.export_cols).issubset(set(collection[self.table_name].keys())):
            return (ValueError, f"Inputted list of column names to plot for {self.table_name} is incorrect")
        
        df = pd.DataFrame(collection[self.table_name])
        
        if self.export_cols is not None:
            try:
                df = df[self.export_cols]
            except:
                return (ValueError, f"Could not export to csv as the specified column input {self.export_cols} is incorrect")
        df.to_csv(self.csv_file_name, index=False)

class Table_Plot(FileWriter):
    """
    Plugin that plots all numeric column data for a specified table
    """
    def __init__(self, table_name, filename, display_cols = None, **kwargs):
        """
        `table_name`: name of table to be plotted

        `filename`: name of output file the plot will be stored in
        
        `display_cols`: default None. When specified, must be a list of column names, whose data is NUMERICAL, to plot
        """
        super().__init__(filename, **kwargs)
        self.output_name = filename
        self.table_name = table_name
        self.display_cols = display_cols

    def get_rows(self, collection) -> None:
        """
        Function called in core.py that generates the table plot image file.

        `collection`: representation of internal DSI abstraction. It is a nested Ordered Dict, with table names as keys, and table data as Ordered Dicts

        `return`: None. Only returns if error. Message is sent back to core to print along with error type. Ex: (ValueError, "error message")
        """
        if self.table_name not in collection.keys():
            return (ValueError, f"{self.table_name} does not exist in memory")
        if self.table_name in ["dsi_units", "dsi_relations", "sqlite_sequence"]:
            return (ValueError, f"Cannot plot the units or relations table")
        if self.display_cols is not None and not set(self.display_cols).issubset(set(collection[self.table_name].keys())):
            return (ValueError, f"Inputted list of columns to plot for {self.table_name} is incorrect")
        
        numeric_cols = []
        not_plot_cols = []
        col_len = None
        for colName, colData in collection[self.table_name].items():
            if colName == "run_id" or (self.display_cols is not None and colName not in self.display_cols):
                continue
            if col_len == None:
                col_len = len(colData)
            if not any(isinstance(item, str) for item in colData):
                all_num_col = [0 if item is None else item for item in colData]
                unit = ""
                if "dsi_units" in collection.keys() and self.table_name in collection["dsi_units"].keys() and colName in collection["dsi_units"][self.table_name].keys():
                    unit = collection["dsi_units"][self.table_name][colName]
                    unit = f" ({unit})"
                numeric_cols.append((colName + unit, all_num_col))
            elif self.display_cols is not None and colName in self.display_cols:
                not_plot_cols.append(colName)

        sim_list = list(range(1, col_len + 1))

        for colName, colData in numeric_cols:
            plt.plot(sim_list, colData, label = colName)
        plt.xticks(sim_list)
        plt.xlabel("Sim Number")
        plt.ylabel("Units")
        plt.title(f"{self.table_name} Values")
        plt.legend()
        plt.savefig(self.output_name, bbox_inches='tight')

        if len(not_plot_cols) > 1:
            return ("Warning", f"Even though {not_plot_cols} are in display_cols, they are not numeric and cannot be plotted")
        elif len(not_plot_cols) == 1:
            return ("Warning", f"Even though '{not_plot_cols[0]}' is in display_cols, it is not numeric and cannot be plotted")