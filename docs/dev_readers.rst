.. _custom_reader:

====================================
Custom DSI Reader
====================================

DSI Readers are the primary way to convert outside data to metadata that DSI can ingest, and they must must include 2 methods, ``__init__``, and ``add_rows``.

Loading Custom Reader into DSI
------------------------------
Before explaining the structure of Readers, it is important to note there are two ways to load your Reader, externally and internally.

 - If your Reader is intended for use within your own code base and not added to DSI's modules or for public use, you can load it externally. 
   Doing so allows you to store your Reader separately from DSI yet compatible with all versions of DSI.

    - With the ``Core.Terminal.add_external_python_module()`` method, you can make your Reader temporarily accessible to DSI in a workflow and load normally.
    - This example can be better seen at :ref:`external_readers_writers_label` where you can try loading an external TextFile reader class.

 - If you want your Reader loadable internally with the rest of the provided implementations (in 
   `dsi/plugins <https://github.com/lanl/dsi/tree/main/dsi/plugins>`_), it must be registered in the ``VALID_READERS`` class variable of ``Terminal`` in 
   `dsi/core.py <https://github.com/lanl/dsi/blob/main/dsi/core.py>`_. 
   If this is done correctly, your Reader will be loadable by the ``load_module()`` method of ``Terminal``.

Initializer: ``__init__(self) -> None:``
-------------------------------------------
``__init__`` is where you can include all of your initialization logic, and specify the parameters needed for a given application. 

Example ``__init__``: ::

  def __init__(self, filenames) -> None:
    # see "plugins" to determine which superclass your Reader should extend
    super().__init__()

    # allow users to read multiple files at once, or just one file at a time
    if isinstance(filenames, str): 
        self.filenames = [filenames]
    else:
        self.filenames = filenames

    # data structure to load data into that is compatible with DSI
    self.data_dict = OrderedDict() 

.. Pack Header: ``pack_header(self) -> None``
.. ---------------------------------------------

.. ``pack_header`` is responsible for setting a schema, registering which columns 
.. will be populated by the reader. The ``set_schema(self, table_data: list, validation_model=None) -> None`` method 
.. is available to subclasses of ``StructuredMetadata``, which allows one to simply give a list of column names to register. 
.. ``validation_model`` is an pydantic model that can help you enforce types, but is completely optional.

.. Example ``pack_header``: ::

..   def pack_header(self) -> None:
..     column_names = ["foo", "bar", "baz"]
..     self.set_schema(column_names)

.. Add Rows: ``add_rows(self) -> None``
.. -------------------------------------

.. ``add_rows`` is responsible for appending to the internal metadata buffer. 
.. Whatever data is being ingested, it's done here. The ``add_to_output(self, row: list) -> None`` method is available to subclasses 
.. of ``StructuredMetadata``, which takes a list of data that matches the schema and appends it to the internal metadata buffer.

.. Note: ``pack_header`` must be called before metadata is appended in ``add_rows``. Another helper method of 
.. ``StructuredMetadata`` is ``schema_is_set``, which provides a way to tell if this restriction is met.

.. Example ``add_rows``: ::

..   def add_rows(self) -> None:
..     if not self.schema_is_set():
..       self.pack_header()

..     # data parsing can go here (or abstracted to other functions)
..     my_data = [1, 2, 3]

..     self.add_to_output(my_data)

Add Rows: ``add_rows(self) -> None``
--------------------------------------------
``add_rows`` is responsible for appending to the internal DSI metadata abstraction. 
This function should ensure the data that is loaded is in the form of an OrderedDict (the internal DSI data structure). 

After converting all data to be in an Ordered Dictionary, users must call ``set_schema_2()`` to assign the data to the internal DSI abstaction layer.
You can pass data through ``set_schema_2(self, collection) -> None`` by using the ``collection`` variable, assuming your data is an OrderedDict.

If you have multiple tables of data loaded at once, you can create a nested OrderedDict.
In this case, each table's data is still an OrderedDict and is now a value in a larger OrderedDict whose keys are each table's name.
Ex: OrderedDict( table1: OrderedDict(), table2: OrderedDict() )

``add_rows`` example: ::

  def add_rows(self) -> None:

    # data is stored as an OrderedDict so use set_schema2
    my_data = OrderedDict()
    my_data["jack"] = 10
    my_data["joey"] = 20
    my_data["amy"] = 30

    self.set_schema_2(my_data)

Contributing Your Reader
--------------------------
If your Reader is helpful and acceptable for public use, you should consider making a pull request (PR) into DSI.

Please note that any accepted PRs into DSI should satisfy the following:
 - Passes all tests in ``dsi/plugins/tests``
 - Has no ``pylama`` errors/warnings (see `dsi/.githooks <https://github.com/lanl/dsi/tree/main/.githooks>`_)

Examples
----------
Full Reader examples in-code, can be found in `dsi/plugins/file_reader.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/file_reader.py>`_.
``Csv`` is an especially simple example to view for loading one table. 
``YAML1`` and ``TOML1`` are more complex examples with loading multiple tables of data with units