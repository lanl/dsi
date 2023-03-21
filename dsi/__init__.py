name = "dsi"
"""
The DSI module provides an abstraction layer to developers to interface with databases such as SQLite and GUFI, to easily create a custom database with customizable schemas/tables.

The Filesystem crawler captures file properties and attributes into an SQL database using the library above, and provides helper functions to easily parse the data ingested into the database.

# Demo

The driver for this project with examples on how to use the DSI library can be found in fs_test.py. This driver script first file-crawls a root directory of an example dataset and captures filesystem information using the os.stat command. The os.stat python command captures filesystem properties such as file-permissions, file creation and modification dates, and file sizes.

Once fs information is captured, an instance of the DSI class is created and examples are given on how to declare the location of the database on-disk, table name, and schema used. When a schema type is declared, a loop is used to ingest os.stat information into the database via the API.

The final portion of the driver gives a few examples on how to perform queries using the abstraction layer. Users can use a sqlite command passthrough for raw queries, or helper functions that relate to filesystem properties and sample operators.


# Requirements

* python3
* access a yellow HPC system, i.e. sn-fey
* member of dsi_re group

# How-to-run

To run, simply execute:

ssh user@sn-fey.lanl.gov

git clone https://re-git.lanl.gov/dsi/fssql.git

python3 fs_test.py
"""