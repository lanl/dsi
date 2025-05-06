.. _python_api_label:

Python API
===========

Users can interact with DSI modules using the DSI class which provides an interface for Readers, Writers, and Backends. 
This can be seen below and in ``dsi/dsi.py``. Example workflows using these functions can be seen in the following section: :ref:`user_example_section_label`

Dsi: DSI
----------
The DSI class is a user-level class that encapsulates the Terminal and Sync classes from DSI Core. 
DSI interacts with several functions within Terminal and Sync without requiring the user to differentiate them.
The functionality has also been simplified to improve user experience and reduce complexity.

Users should call ``read()`` to load data from external data files into DSI. ``list_readers()`` prints all valid readers and a short description of each one

Users should call ``write()`` to export data from DSI into external formats. ``list_writers()`` prints all valid writers and a short description of each one.

Users should call ``backend()`` to activate either a Sqlite or DuckDB backend. ``list_writers()`` prints the valid backends and differences between them.

``ingest()``, ``query()``, ``process()`` are considered backend interactions, and require an active backend to work. 
Therefore, ``backend()`` must be called before them.

``findt()``, ``findc()``, ``find()`` also require an active backend as they locate and print where a input search term matches 
tables/columns/datapoints respectively.

``list()``, ``num_tables()``, ``display()``, ``summary()`` all print various information from an active backend. Differences are explained below.

Notes for users:
      - Must call ``reader()`` prior to ``ingest()`` to ensure there is actual data ingested into a backend
      - If there is no data in DSI memory, ie. read() was never called, process() MUST be called on an active backend 
        to ensure data can be exported with write()
      - Refer to the :ref:`datacard_section_label` section to learn which/how datacard files are read into DSI 
        Inputs to the datacard readers - Oceans11Datacard, DublinCoreDatacard, SchemaOrgDatacard - must all follow the formats found in dsi/examples/data/

.. autoclass:: dsi.dsi.DSI 
      :members:


.. _datacard_section_label:

DSI Data Cards
---------------

DSI is expanding its support of several dataset metadata standards. The current supported standards are for:

      - `Dublin Core <https://www.dublincore.org/resources/metadata-basics/>`_
      - `Schema.org Dataset <https://schema.org/Dataset>`_
      - `Oceans11 DSI Data Server <https://oceans11.lanl.gov/>`_

Template file structures can be copied and found in ``dsi/examples/data/``. 
The fields in a user's data card must exactly match its respective template to be compatible with DSI.
However, fields can be empty if a user does not have particular information about that dataset.

The supported datacards can be read into DSI by creating an instance of DSI() and calling:

      - ``read(filenames="file/path/to/datacard.XML", reader_name='DublinCoreDatacard')``
      - ``read(filenames="file/path/to/datacardh.JSON", reader_name='SchemaOrgDatacard')``
      - ``read(filenames="file/path/to/datacard.YAML", reader_name='Oceans11Datacard')``

Completed examples of each metadata standard for the Wildfire dataset can also be found in ``dsi/examples/wildfire/`` 


.. _user_example_section_label:

User Examples
--------------
Examples below display various ways users can incorporate DSI into their data science workflows.
They can be found and run in ``examples/user/``

Example 1: Intro use case
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Baseline use of DSI to list all valid Readers, Writers, and Backends, and descriptions of each.

.. literalinclude:: ../examples/user/1.baseline.py

Example 2: Ingest data
~~~~~~~~~~~~~~~~~~~~~~
Loading data from a Reader, ingesting it into a backend and displaying some of that data

.. literalinclude:: ../examples/user/2.ingest.py

Example 3: Find data
~~~~~~~~~~~~~~~~~~~~
Finding data from an active backend - tables, columns, datapoints matches

.. literalinclude:: ../examples/user/3.find.py

Example 4: Process data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Processing (reading) data from a backend and load DSI writers to generate an Entity Relationship diagram, plot a table's data, and export to a CSV

.. literalinclude:: ../examples/user/4.process.py

Example 5: Query data
~~~~~~~~~~~~~~~~~~~~~
Querying data from a backend

.. literalinclude:: ../examples/user/5.query.py

Example 6: Visualizing a database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Printing different data and metadata from a database - number of tables, dimensions of tables, actual data in tables, and statistics from each table 

.. literalinclude:: ../examples/user/6.visualize.py

Example 7: Ingest complex schema with data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Using the Schema Reader to load a complex JSON schema, loading the relevant data, and viewing difference between databases with a schema and no schema
Read :ref:`schema_section` to understand how to structure this schema JSON file for the Schema Reader

.. literalinclude:: ../examples/user/7.schema.py