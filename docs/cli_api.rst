.. _cli_api_label:

Command Line Interface API
==========================

Users can interact with DSI Readers, Writers and Backends even easier with DSI's Command Line Interace (CLI).
While slightly more restrictive than the Python API, the CLI allows users to interact with DSI without any knowledge of Python.

Users can store several files in DSI, and query/find/export loaded data to other formats.
Users can also write loaded data to a permanent database store for post-analysis.

The CLI actions and example workflows are shown below.

CLI Setup and Actions
---------------------
Once a user has successfully installed DSI, they can active the CLI environment by entering ``dsi`` in their command line.
This automatically creates a hidden Sqlite database that users can interact with. 

However, if a user wants to use DuckDB instead, they should activate the CLI with ``dsi -b duckdb`` in their command line. 
From here on out, all actions will be using a hidden DuckDB database.

A comprehensive list of all actions within the CLI environment are:

help
    Displays a help menu for CLI actions and their inputs.

display <table name> [-n num rows] [-e filename]
    Displays data from a specified table, with optional num rows and export filename.

    - `table_name` is a mandatory input to display that table.
    - `num_rows` is optional and limits the display to the first N rows.
    - `filename` is optional and exports the display to a CSV or Parquet file.

exit
    Exits the CLI and closes all active DSI Readers/Writers/Backends.

draw [-f filename]
    Draws an ER diagram of all data loaded into DSI.

    - `filename` is optional; default is `er_diagram.png`.

find <var>
    Searches for an input variable from all data loaded into DSI. Can match table names, columns, or data values.

list
    Lists the names of all tables and their dimensions.

read <filename> [-t table name]
    Reads specified data into DSI
    
    - `filename` is a mandatory input of data to ingest. Accepted formats: 
    
        - CSV, TOML, YAML, Parquet, SQLite databases, DuckDB databases
        - URL of data stored online that is in one of the above formats

    - `table_name` is optional. If reading a CSV or Parquet, users can specify table_name

plot_table <table_name> [-f filename]
    Plots numerical data from the specified table.

    - `table_name` is a mandatory input to know what table to plot
    - `filename` is optional; default is `<table_name>_plot.png`.

query <SQL query> [-n num rows] [-e filename]
    Runs the specified query with optional row display and export file.

    - `SQL query` is mandatory and must match SQLite or DuckDB syntax.
    - `num_rows` is optional; can specify to limit number of rows in result.
    - `filename` is optional; can export the result as CSV or Parquet file.

summary [-t table] [-n num_rows]
    Displays statistics of all tables or a specified table.

    - `table` is optional and limits output to one table.
    - `num_rows` optionally prints rows from the specified table.

write <filename>
    Writes the hidden DSI database to a persistent location. Must match the current DSI database (e.g., SQLite must be `.sqlite`).

Users can also expect basic unix commands such as ``cd`` (change directory), ``ls`` (list all files) and ``clear`` (clear command line view).

CLI Examples
------------
The terminal output below displays various ways users can utilize DSI's CLI to simplify data science workflows.

.. literalinclude:: images/cli_output.txt