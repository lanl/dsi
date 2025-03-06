Core
====

The DSI Core middleware defines the Terminal concept. An instantiated Terminal is the human/machine DSI interface. The person setting up a Core Terminal only needs to know how they want to ask questions, and what metadata they want to ask questions about. If they don’t see an option to ask questions the way they like, or they don’t see the metadata they want to ask questions about, then they should ask a Backend Contributor or a Plugin Contributor, respectively.

Core: Terminal
--------------

The Terminal class is a structure through which users can interact with Plugins (Readers/Writers) and Backends as "module" objects. Each reader/writer/backend can be "loaded" to make ready for use and users can further interact with backends by ingesting, querying, or processing data as well as generating an interactive notebook with data. All relevant functions have been listed below for further clarity.

Notes for users:
      - All plugin writers that are loaded must be followed by calling transload() after to execute them. Readers are automatically executed upon loading.
      - Terminal.load_module: if user wants to relate tables of data from a plugin reader under the same name, they can use the `target_table_prefix`` input to specify a prefix.

            - users must note that if accessing data from these tables they must remember the table names will include specified prefix. Ex: collection1__math, collection1_english 
      - Terminal.artifact_handler: 'notebook' interaction_type stores data from first loaded backend, not existing DSI abstraction, in new notebook file
      - Terminal find functions only access the first loaded backend
      - Terminal.unload_module: removes last loaded backend of specified mod_name. ex: 2 loaded Sqlite backends, second is unloaded
      - Terminal handles errors from any loaded DSI/user-written modules (plugins/backends). If writing an external plugin/backend, return a caught error as a tuple (error, error_message_string). Do not print in a new class
.. autoclass:: dsi.core.Terminal
      :members:

Core: Sync
----------

The DSI Core middleware also defines data management functionality in ``Sync``. The purpose of ``Sync`` is to provide file metadata documentation and data movement capabilities when moving data to/from local and remote locations. The purpose of data documentation is to capture and archive metadata (i.e. location of local file structure, their access permissions, file sizes, and creation/access/modification dates) and track their movement to the remote location for future access. The primary functions, ``Copy``, ``Move``, and ``Get`` serve as mechanisms to copy data, move data, or retrieve data from remote locations by creating a DSI database in the process, or retrieving an existing DSI database that contains the location(s) of the target data.

.. autoclass:: dsi.core.Sync
      :members:
  
Examples
--------
Before interacting with the plugins and backends, they must each be loaded. Examples below display various ways that users can incorporate DSI into their data science workflows.

Example 1: Baseline use of DSI to list Modules

.. literalinclude:: ../examples/core/baseline.py

Example 2: Ingesting data from a Reader to a backend

.. literalinclude:: ../examples/core/ingest.py

Example 3: Querying data from a backend

.. literalinclude:: ../examples/core/query.py

Example 4: Processing data from a backend to generate an Entity Relationship diagram

.. literalinclude:: ../examples/core/process.py

Example 5: Generating a python notebook file (mostly Jupyter notebook) from a backend to view data interactively

.. literalinclude:: ../examples/core/notebook.py

Example 6: Finding data from a backend - tables, columns, cells, or all matches

.. literalinclude:: ../examples/core/find.py

Example 7: Loading an external plugin from another python file

.. literalinclude:: ../examples/core/external_plugin.py