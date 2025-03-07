====================================
Making a Reader for Your Application
====================================

DSI readers are the primary way to transform outside data to metadata that DSI can ingest. 
Readers are Python classes that must include a few methods, namely ``__init__``, ``pack_header``, and ``add_rows``.

Initializer: ``__init__(self) -> None:``
-------------------------------------------
``__init__`` is where you can include all of your initialization logic, just make sure to initialize your superclass. 
Note: ``__init__`` can also take whatever parameters needed for a given application.

Example ``__init__``: ::

  def __init__(self) -> None:
    super().__init__()  # see "plugins" to determine which superclass your reader should extend

Pack Header: ``pack_header(self) -> None``
---------------------------------------------

``pack_header`` is responsible for setting a schema, registering which columns 
will be populated by the reader. The ``set_schema(self, table_data: list, validation_model=None) -> None`` method 
is available to subclasses of ``StructuredMetadata``, which allows one to simply give a list of column names to register. 
``validation_model`` is an pydantic model that can help you enforce types, but is completely optional.

Example ``pack_header``: ::

  def pack_header(self) -> None:
    column_names = ["foo", "bar", "baz"]
    self.set_schema(column_names)

Add Rows: ``add_rows(self) -> None``
-------------------------------------

``add_rows`` is responsible for appending to the internal metadata buffer. 
Whatever data is being ingested, it's done here. The ``add_to_output(self, row: list) -> None`` method is available to subclasses 
of ``StructuredMetadata``, which takes a list of data that matches the schema and appends it to the internal metadata buffer.

Note: ``pack_header`` must be called before metadata is appended in ``add_rows``. Another helper method of 
``StructuredMetadata`` is ``schema_is_set``, which provides a way to tell if this restriction is met.

Example ``add_rows``: ::

  def add_rows(self) -> None:
    if not self.schema_is_set():
      self.pack_header()

    # data parsing can go here (or abstracted to other functions)
    my_data = [1, 2, 3]

    self.add_to_output(my_data)

*Newer* Add Rows: ``add_rows(self) -> None``
-------------------------------------
If you are confident that the the data you read in ``add_rows`` is in the form of an OrderedDict (the data structure used to store all ingested data), you can bypass the use of ``pack_header`` and ``add_to_output`` with an alternate ``set_schema`` function.

This function, ``set_schema_2(self, collection, validation_model=None) -> None``, directly assigns the data you read in ``add_rows`` to the internal DSI abstraction layer, provided that the data you pass as the ``collection`` variable is an OrderedDict. This method allows you to quickly append data to the abstraction wholesale, rather than row-by-row.

Example alternate ``add_rows``: ::

  def add_rows(self) -> None:

    # data is stored as an OrderedDict so can use set_schema2
    my_data = OrderedDict()
    my_data["jack"] = 10
    my_data["joey"] = 20
    my_data["amy"] = 30

    self.set_schema_2(my_data)

Implemented Examples
--------------------------------
If you want to see some full reader examples in-code, some can be found in 
`dsi/plugins/file_reader.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/file_reader.py>`_.
``Csv`` is an especially simple example to go off of. 

Loading Your Reader
-------------------------
There are two ways to load your reader, internally and externally.

 - Internally: If you want your reader loadable internally with the rest of the provided implementations (in `dsi/plugins <https://github.com/lanl/dsi/tree/main/dsi/plugins>`_), it must be registered in the class variables of ``Terminal`` in `dsi/core.py <https://github.com/lanl/dsi/blob/main/dsi/core.py>`_. If this is done correctly, your reader will be loadable by the ``load_module`` method of ``Terminal``.
 - Externally: If your reader is not along side the other provided implementations, possibly somewhere else on the filesystem, your reader will be loaded externally. This is done by using the ``add_external_python_module`` method of ``Terminal``. If you load an external Python module this way (ex. ``term.add_external_python_module('plugin','my_python_file','/the/path/to/my_python_file.py')``), your reader will then be loadable by the ``load_module`` method of ``Terminal``.
 

Contributing Your Reader
--------------------------
If your reader is helpful and acceptable for public use, you should consider making a pull request (PR) into DSI.

Please note that any accepted PRs into DSI should satisfy the following:
 - Passes all tests in ``dsi/plugins/tests``
 - Has no ``pylama`` errors/warnings (see `dsi/.githooks <https://github.com/lanl/dsi/tree/main/.githooks>`_)
