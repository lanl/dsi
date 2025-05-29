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

Users must first call ``backend()`` to activate a backend to read data into and interact with.

Users should use ``read()`` to load data into DSI and ``write()`` to export data from DSI into supported external formats.
Their respective list functions print all valid readers/writers that can be used.

The primary backend interactions are ``query()``, ``get_table()``, and ``find()`` where users can print a search result, or retrieve the result as a collection of data.

      - If users manipulate these collections, they can call ``update()`` to update the respective data in the activated backend.
        Read ``update()`` to understand its accepted inputs and behavior.

Users can also view various data/metadata of an active backend with ``list()``, ``num_tables()``, ``display()``, ``summary()``

Notes for users:
      - When using a complex schema, must call ``schema()`` prior to ``read()`` to store the associated data and relations together.
      - If input to ``update()`` contains edited data for a user-defined primary key column, rows in that table might be reordered.
      - If input to ``update()`` is a single Pandas.DataFrame, the existing table in the backend will be **overwritten**. Ensure data is secure.
      - Read the :ref:`datacard_section_label` section to learn which data card standards are supported and where to find templates compatible with DSI. 

.. autoclass:: dsi.dsi.DSI 
      :members:


.. _datacard_section_label:

DSI Data Cards
---------------

DSI is expanding its support of several dataset metadata standards. The current supported standards are for:

      - `Dublin Core <https://www.dublincore.org/resources/metadata-basics/>`_
      - `Schema.org's Dataset object <https://schema.org/Dataset>`_
      - `Google Data Cards Playbook <https://sites.research.google/datacardsplaybook/>`_
      - `Oceans11 DSI Data Server <https://oceans11.lanl.gov/>`_

Template file structures can be found and copied in ``examples/data/``. 
The fields in a user's data card must exactly match its respective template to be compatible with DSI.
However, fields can be empty if a user does not have that particular information about the dataset.

The supported datacards can be read into DSI by creating an instance of DSI() and calling:

      - ``read("file/path/to/datacard.XML", 'DublinCoreDatacard')``
      - ``read("file/path/to/datacardh.JSON", 'SchemaOrgDatacard')``
      - ``read("file/path/to/datacard.YAML", 'GoogleDatacard')``
      - ``read("file/path/to/datacard.YAML", 'Oceans11Datacard')``

Completed examples of each metadata standard for the Wildfire dataset can also be found in ``examples/wildfire/`` 


.. _user_example_section_label:

User Examples
--------------
Examples below display various ways users can incorporate DSI into their data science workflows.
They can be found and run in ``examples/user/``

Example 1: Intro use case
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Baseline use of DSI to list all valid Readers, Writers, and Backends, and descriptions of each.

.. literalinclude:: ../examples/user/1.baseline.py

Example 2: Read data
~~~~~~~~~~~~~~~~~~~~~~
Reading data from a YAML file into a DSI backend, and displaying some of that data

.. literalinclude:: ../examples/user/2.read.py

Example 3: Find data
~~~~~~~~~~~~~~~~~~~~
Finding data from an active backend - tables, columns, datapoints matches

.. literalinclude:: ../examples/user/3.find.py

Example 4: Writing data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Writing data from a DSI backend as an Entity Relationship diagram, table plot, and CSV.

.. literalinclude:: ../examples/user/4.write.py

Example 5: Query data
~~~~~~~~~~~~~~~~~~~~~
Querying data from an active backend

.. literalinclude:: ../examples/user/5.query.py

Example 6: Visualizing a database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Printing different data and metadata from a database - number of tables, dimensions of tables, actual data in tables, and statistics from each table 

.. literalinclude:: ../examples/user/6.visualize.py

Example 7: Complex schema with data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Loading complex JSON schema with ``schema()``, loading associated data with ``read()``, and an ER Diagram to display the relations.
Read :ref:`user_schema_example_label` to understand how to structure a DSI-compatible input file for ``schema()``

.. literalinclude:: ../examples/user/7.schema.py