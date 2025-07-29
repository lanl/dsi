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

To view all available CLI actions without launching the CLI, users can enter ``dsi help`` in their command line.

A comprehensive list of all actions in the CLI environment is as follows:

help
    Displays a help menu for CLI actions and their inputs.

display <table name> [-n num rows] [-e filename]
    Displays data from a specified table, with optional arguments.

    - `table_name` is a mandatory input to display that table.
    - `num_rows` is optional and only displays the first N rows.
    - `filename` is optional and exports the table to a CSV or Parquet file.

draw [-f filename]
    Draws an ER diagram of all data loaded into DSI.

    - `filename` is optional; default is `er_diagram.png`.

exit
    Exits the CLI and closes all active DSI modules.

find <condition>
    Finds all rows of a table that match the condition in the format: [column] [operator] [value]. 
    Ex: find 'age = 6'

    Valid operators:

        - age > 4 
        - age < 4 
        - age >= 4 
        - age <= 4 
        - age = 4 
        - age == 4
        - age ~ 4    --> column age contains the number 4
        - age ~~ 4   --> column age contains the number 4
        - age != 4 
        - age (4, 8) --> all values in 'age' between 4 and 8 (inclusive)

list
    Lists the names of all tables and their dimensions.

plot_table <table_name> [-f filename]
    Plots numerical data from the specified table.

    - `table_name` is a mandatory input to plot that table
    - `filename` is optional; default is `<table_name>_plot.png`.

query <SQL query> [-n num rows] [-e filename]
    Executes a specified query (in quotes) and prints the result with optional arguments.

    - `SQL query` is mandatory and must match SQLite or DuckDB syntax.
    - `num_rows` is optional; prints the first N rows of the result.
    - `filename` is optional; export the result as CSV or Parquet file.

read <filename> [-t table name]
    Reads specified data into DSI
    
    - `filename` is a mandatory input of data to ingest. Accepted formats: 
    
        - CSV, JSON, TOML, YAML, Parquet, SQLite databases, DuckDB databases
        - URL pointing to data stored in one of the above formats

    - `table_name` is optional. If reading a CSV, JSON, or Parquet, users can specify table_name

search <value>
    Searches for an input value across all data loaded into DSI. Can be a number or text.

summary [-t table_name]
    Displays numerical statistics of all tables or a specified table.

    - `table_name` is optional and summarizes only that specified table.

write <filename>
    Writes the hidden DSI backend to a designated location. This permanent file will be of the same type as the hidden backend.

Users can also expect basic unix commands such as ``cd`` (change directory), ``ls`` (list all files) and ``clear`` (clear command line view).

CLI Example
------------
The terminal output below displays various ways users can utilize DSI's CLI for seamless data science analysis.

.. literalinclude:: images/cli_output.txt