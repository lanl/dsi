.. _core_page:

Core
====

The DSI Core middleware defines the `Terminal` and `Sync` concepts. 
An instantiated `Terminal` is the human/machine DSI interface to connect Readers/Writers and DSI backends.
An instantiated `Sync` supports data movement capabilities between local and remote locations and captures metadata documentation

Core: Terminal
--------------

The Terminal class is a structure through which users can interact with Readers/Writers and Backends as "module" objects. 
Each reader/writer/backend can be "loaded" to be ready for use and users can interact with backends by ingesting, querying, processing, or finding data, 
as well as generating an interactive notebook of the data. 

All relevant functions have been listed below for further clarity. :ref:`example_section_label` displays different workflows using this Terminal class.

Notes for users:
      - All DSI Writers that are loaded must be followed by calling ``Terminal.transload`` after to execute them. 
        Readers are automatically executed upon loading.
      - ``Terminal.load_module``: if users wants to group related tables of data from a DSI Reader under the same name, 
        they can use the `target_table_prefix` input to specify a shared prefix.

            - users must remember that when accessing data from these tables, their names will include the specified prefix. 
              Ex: collection1__math, collection1__english 
      - ``Terminal.artifact_handler``: 'notebook' interaction_type stores data from first loaded backend, NOT existing DSI abstraction, in new notebook file
      - ``Terminal.artifact_handler``: review this function description below to clarify which backends are targeted by which interaction_types
      - Terminal find functions only access the first loaded backend
      - ``Terminal.unload_module``: removes last loaded backend of specified mod_name. Ex: if there are 2 loaded Sqlite backends, second is unloaded
      - Terminal handles errors from any loaded DSI/user-written modules (Readers/Writers/backends). 
      
            - If writing an external Reader/Writer/backend, return any intentionally caught errors as a tuple (error type, error message). 
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

The primary functions, ``Copy``, ``Move``, and ``Get`` serve as mechanisms to copy data, move data, or retrieve data from remote locations 
by creating a DSI database in the process, or retrieving an existing DSI database that contains the location(s) of the target data.

.. autoclass:: dsi.core.Sync
      :members:
      :special-members: __init__


.. _example_section_label:

Examples
--------
Examples below display various ways users can incorporate DSI into their data science workflows.
They are located in ``examples/developer/`` and must be run from that directory.

Most of them either load or refer to data from ``examples/clover3d/``.
If the directory does not exist, users must first call ``core.Sync().move()`` to ensure the data is stored locally for these examples.

Example 1: Intro use case
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Baseline use of DSI to list Modules

.. literalinclude:: ../examples/developer/1.baseline.py


.. _ingest_label:

Example 2: Ingest data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Loading a Cloverleaf reader and ingesting that data into a Sqlite DSI backend

.. literalinclude:: ../examples/developer/2.ingest.py

Example 3: Complex schema with data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ingesting data from a Cloverleaf Reader to a DSI backend along with a complex schema stored in a JSON file. 
Read :ref:`schema_section` to better understand how to structure this schema JSON file for the Schema Reader

.. literalinclude:: ../examples/developer/3.schema.py


.. _process_label:

Example 4: Process and Write data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Processing data from a Sqlite DSI backend and useing a Writer to generate an ER diagram, table plot and CSV file.

.. literalinclude:: ../examples/developer/4.process.py

Example 5: Find data
~~~~~~~~~~~~~~~~~~~~
Finding data from a Sqlite DSI backend - tables, columns, cells, or all matches.
If matching data found, it is returned as a list of ValueObject(). 
Refer to each backend's ValueObject() description in :ref:`backend_section_label`, as its structure varies by backend.

.. literalinclude:: ../examples/developer/5.find.py

Example 6: Query data
~~~~~~~~~~~~~~~~~~~~~~
Querying data from a Sqlite DSI backend.
Users can either use ``artifact_handler('query', SQL_query)`` to store certain data, or ``get_table()`` to retrieve all data from a specified table.

.. literalinclude:: ../examples/developer/6.query.py

Example 7: Overwrite data
~~~~~~~~~~~~~~~~~~~~~~~~~~
Overwriting a table using modified data from ``get_table()``

.. literalinclude:: ../examples/developer/7.overwrite.py

Example 8: Visualize data
~~~~~~~~~~~~~~~~~~~~~~~~~~
Printing various data and metadata from a DSI backend - number of tables, list of tables, actual table data, and summary of table statistics

.. literalinclude:: ../examples/developer/8.visualize.py


.. _external_readers_writers_label:

Example 9: External Readers/Writers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Temporarily adding an external data reader to DSI, allowing DSI to interact with the associated data across all actions.
In this instance, the data is ingested into a backend and viewed using ``display()``

.. literalinclude:: ../examples/developer/9.external_plugin.py

``text_file_reader``:

.. literalinclude:: ../examples/developer/text_file_reader.py

Example 10: Generate notebook
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Generating a python notebook (mostly Jupyter notebook) from a Sqlite DSI backend to view data interactively.

.. literalinclude:: ../examples/developer/10.notebook.py