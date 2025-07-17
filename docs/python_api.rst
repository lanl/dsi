.. _python_api_label:

Python API
===========

Users can interact with DSI modules using the DSI class which provides an interface for Readers, Writers, and Backends. 
This can be seen below and in ``dsi/dsi.py``. Example workflows using these functions can be seen in the following section: :ref:`user_example_section_label`

Dsi: DSI
----------
The DSI class is a user-level class that encapsulates the Terminal and Sync classes from DSI Core. 
DSI interacts with several functions within Terminal and Sync without requiring the user to differentiate them.
The functionality has been simplified to improve user experience and reduce complexity.

When creating an instance of DSI(), users can optionally specify the type of backend and filename to use
If neither is provided, a temporary backend is automatically created, allowing users to interact with their data.
Read the ``__init__`` documentation below for more details on the supported backend types.

Users should use ``read()`` to load data into DSI and ``write()`` to export data from DSI into supported external formats.
Their respective list functions print all valid readers/writers that can be used.

The primary backend interactions are ``find()`` , ``query()``, and ``get_table()`` where users can print a search result, or retrieve the result as a collection of data.
      
      - If users modify these collections, they can call ``update()`` to apply the changes to the active backend.
        Users must NOT edit any columns beginning with **`dsi_`**. Read ``update()`` below to better understand its behavior.

Users can also view various data/metadata of an active backend with ``list()``, ``num_tables()``, ``display()``, ``summary()``

Notes for users:
      - When using a complex schema, must call ``schema()`` prior to ``read()`` to store the relations with the associated data.
      - If input to ``update()`` is a modified output from ``query()``, the existing table will be **overwritten**. 
        Ensure data is secure or add `backup` flag in ``update()`` to create a backup database.
      - Read the :ref:`datacard_section_label` section to learn which data card standards are supported and where to find templates compatible with DSI. 

.. autoclass:: dsi.dsi.DSI 
      :members:
      :special-members: __init__


.. _datacard_section_label:

DSI Data Cards
---------------

DSI is expanding its support of several dataset metadata standards. Currently supported standards include:

      - `Dublin Core <https://www.dublincore.org/resources/metadata-basics/>`_
      - `Schema.org's Dataset object <https://schema.org/Dataset>`_
      - `Google Data Cards Playbook <https://sites.research.google/datacardsplaybook/>`_
      - `Oceans11 DSI Data Server <https://oceans11.lanl.gov/>`_

Template file structures can be found and copied in ``examples/test/``.

To be compatible with DSI, a user's data card must contain all the fields in its corresponding template.
However, if certain metadata is not available for a dataset, the values of those fields may be left empty.

The supported datacards can be read into DSI by creating an instance of DSI() and calling:

      - ``read("file/path/to/datacard.XML", 'DublinCoreDatacard')``
      - ``read("file/path/to/datacardh.JSON", 'SchemaOrgDatacard')``
      - ``read("file/path/to/datacard.YAML", 'GoogleDatacard')``
      - ``read("file/path/to/datacard.YAML", 'Oceans11Datacard')``

Examples of each data card standard for the Wildfire dataset can be found in ``examples/wildfire/`` 


.. _user_example_section_label:

User Examples
--------------
Examples below display various ways users can incorporate DSI into their data science workflows.
They are located in ``examples/user/`` and must be run from that directory.

All of them either load or refer to data in ``examples/clover3d/``. 

Example 1: Intro use case
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Baseline use of DSI to list all valid Readers, Writers, and Backends, and descriptions of each.

.. literalinclude:: ../examples/user/1.baseline.py

Example 2: Read data
~~~~~~~~~~~~~~~~~~~~~~
Reading Cloverleaf data into a DSI backend, and displaying some of that data

.. literalinclude:: ../examples/user/2.read.py

Example 3: Visualize data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Printing various data and metadata from a DSI backend - number of tables, list of tables, actual table data, and summary of table statistics

.. literalinclude:: ../examples/user/3.visualize.py

Example 4: Find data
~~~~~~~~~~~~~~~~~~~~
Finding data from an active DSI backend that matches an input query - a string or a number.
Prints all matches by default. If ``True`` is passed as an additional argument, returns rows of the first table that satisfies the query.

.. literalinclude:: ../examples/user/4.find.py

Example 5: Update data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Updating data from the edited output of ``find()``. Input can be output of either ``find()``, ``query()``, or ``get_table()``.
Users must NOT change metadata columns starting with **`dsi_`** even if adding new rows.

.. literalinclude:: ../examples/user/5.update.py

Example 6: Query data
~~~~~~~~~~~~~~~~~~~~~
Querying data from an active DSI backend. 
Users can either use ``query()`` to view specific data with a SQL statement, or ``get_table()`` to view all data from a specified table.

.. literalinclude:: ../examples/user/6.query.py

Example 7: Complex schema with data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Loading a complex JSON file with ``schema()``, the associated Cloverleaf data with ``read()``, and an ER Diagram to display the data relations.

Read :ref:`user_schema_example_label` to learn how to structure a DSI-compatible input file for ``schema()``

.. literalinclude:: ../examples/user/7.schema.py

Example 8: Write data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Writing data from a DSI backend as an Entity Relationship diagram, table plot, and CSV.

.. literalinclude:: ../examples/user/8.write.py

.. _user_external_reader:

Example 9: Load an external Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Loading an external DSI-compatible Reader and its associated data into DSI to interact with and/or visualize the data.
For more information on creating an external Reader/Writer, view :ref:`custom_reader` and :ref:`custom_writer`.

.. literalinclude:: ../examples/user/9.external_reader.py

``text_file_reader``:

.. literalinclude:: ../examples/test/text_file_reader.py