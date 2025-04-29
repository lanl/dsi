Core
====

The DSI Core middleware defines the `DSI`, `Terminal` and `Sync` concepts. 
An instantiated `DSI` is a simplified user-level interface for the `Terminal` and `Sync` classes.
An instantiated `Terminal` is the human/machine connection for Reader/Writer plugins and DSI backends.
An instantiated `Sync` supports data movement capabilities between local and remote locations and captures metadata documentation

Core: DSI
----------
The DSI class is a user-level class that encapsulates the Terminal and Sync classes within Core. 
DSI calls several functions within Terminal and Sync without requiring the user to differentiate them.
The names/actions of functions have also been simplified to improve user experience and reduce complexity.

Notes for users:
      - Users must call sqlbackend() prior to ingest() to first load a SQLite backend
      - Need to call DSI Reader functions prior to calling sqlbackend() and ingest() which stores data from Readers into a SQLite backend
      - process() and DSI Writer functions require a user to first call open() to load a SQLite READING backend
      - Files inputted into DSI Readers that are datacards (oceans11, dublin, schema.org) must exactly follow examples in examples/data

.. autoclass:: dsi.core.DSI
      :members:

Core: Terminal
--------------

The Terminal class is a structure through which users can interact with Plugins (Readers/Writers) and Backends as "module" objects. 
Each reader/writer/backend can be "loaded" to be ready for use and users can interact with backends by ingesting, querying, processing, or finding data, 
as well as generating an interactive notebook of the data. 

All relevant functions have been listed below for further clarity. :ref:`example_section_label` displays different workflows using this Terminal class.

Notes for users:
      - All plugin writers that are loaded must be followed by calling ``Terminal.transload`` after to execute them. Readers are automatically executed upon loading.
      - ``Terminal.load_module``: if users wants to group related tables of data from a plugin reader under the same name, 
        they can use the `target_table_prefix` input to specify a shared prefix.

            - users must remember that when accessing data from these tables, their names will include the specified prefix. Ex: collection1__math, collection1__english 
      - ``Terminal.artifact_handler``: 'notebook' interaction_type stores data from first loaded backend, NOT the existing DSI abstraction, in new notebook file
      - ``Terminal.artifact_handler``: review this function description below to clarify which backends are targeted by which interaction_types
      - Terminal find functions only access the first loaded backend
      - ``Terminal.unload_module``: removes last loaded backend of specified mod_name. Ex: if there are 2 loaded Sqlite backends, second is unloaded
      - Terminal handles errors from any loaded DSI/user-written modules (plugins/backends). 
      
            - If writing an external plugin/backend, return any intentionally caught errors as a tuple (error type, error message). 
              Ex: (ValueError, "this is an error") 
.. autoclass:: dsi.core.Terminal
      :members:
      :special-members: __init__

Core: Sync
----------

The DSI Core middleware also defines data management functionality in ``Sync``. 
The purpose of ``Sync`` is to provide file metadata documentation and data movement capabilities when moving data to/from local and remote locations. 
The purpose of data documentation is to capture and archive metadata 
(i.e. location of local file structure, their access permissions, file sizes, and creation/access/modification dates) 
and track their movement to the remote location for future access. 
The primary functions, ``Copy``, ``Move``, and ``Get`` serve as mechanisms to copy data, move data, or retrieve data from remote locations by creating a DSI database in the process, 
or retrieving an existing DSI database that contains the location(s) of the target data.

.. autoclass:: dsi.core.Sync
      :members:
      :special-members: __init__

.. _example_section_label:
Examples
--------
Before interacting with the plugins and backends, they must each be loaded. 
Examples below display various ways users can incorporate DSI into their data science workflows.
They can be found and run in ``examples/core/``

Example 1: Intro use case
~~~~~~~~~~
Baseline use of DSI to list Modules

.. literalinclude:: ../examples/core/baseline.py

.. _example2_label:
Example 2: Ingest data
~~~~~~~~~~
Ingesting data from a Reader to a backend

.. literalinclude:: ../examples/core/ingest.py

Example 2.5: Ingest complex schema and data
~~~~~~~~~~
Ingesting data from a Reader to a backend with a complex schema stored in a separate JSON file. 
Read :ref:`schema_section` to understand how to structure this schema JSON file for the Schema Reader

.. literalinclude:: ../examples/core/ingest_schema.py

Example 3: Query data
~~~~~~~~~~
Querying data from a backend

.. literalinclude:: ../examples/core/query.py

.. _example4_label:
Example 4: Process data
~~~~~~~~~~
Processing data from a backend to generate an Entity Relationship diagram using a Writer

.. literalinclude:: ../examples/core/process.py

Example 5: Generate notebook
~~~~~~~~~~
Generating a python notebook file (mostly Jupyter notebook) from a backend to view data interactively

.. literalinclude:: ../examples/core/notebook.py

Example 6: Find data
~~~~~~~~~~
Finding data from a backend - tables, columns, cells, or all matches

.. literalinclude:: ../examples/core/find.py

Example 7: External plugin
~~~~~~~~~~
Loading an external python plugin reader from a separate file:

.. literalinclude:: ../examples/core/external_plugin.py

``text_file_reader``:

.. literalinclude:: ../examples/core/text_file_reader.py