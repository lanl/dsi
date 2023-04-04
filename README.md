# DSI / DSIpy
 
DSIpy is part of LANL's Data Science Infrastructure project providing several purposes:

The DSI module provides an abstraction layer to developers to interface with SQLite tools to easily create a custom database with customizable schemas/tables.

The Filesystem crawler captures file properties and attributes into an SQL database using the library above, and provides helper functions to easily parse the data ingested into the database.

# Demo

The driver for this project with examples on how to use the DSI library can be found in dsi_wildfire and fs_test.py. This driver script first file-crawls a root directory of an example dataset and captures filesystem information using the os.stat command. The os.stat python command captures filesystem properties such as file-permissions, file creation and modification dates, and file sizes.

Once fs information is captured, an instance of the DSI class is created and examples are given on how to declare the location of the database on-disk, table name, and schema used. When a schema type is declared, a loop is used to ingest os.stat information into the database via the API.

The final portion of the driver gives a few examples on how to perform queries using the abstraction layer. Users can use a sqlite command passthrough for raw queries, or helper functions that relate to filesystem properties and sample operators.

# Requirements

* python3 (3.8 recommended)

# How-to-run

To run, simply execute:

```
git clone https://github.com/lanl/dsi.git
cd dsi/
cd examples/
python3 dsi_wildfire.py
python3 dsi_wildfire_query.py
```

# Copyright and License

This program is open source under the BSD-3 License.

© 2023. Triad National Security, LLC. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted
provided that the following conditions are met:

1.Redistributions of source code must retain the above copyright notice, this list of conditions and
the following disclaimer.
 
2.Redistributions in binary form must reproduce the above copyright notice, this list of conditions
and the following disclaimer in the documentation and/or other materials provided with the
distribution.
 
3.Neither the name of the copyright holder nor the names of its contributors may be used to endorse
or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
