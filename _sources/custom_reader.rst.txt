.. _custom_reader:

====================================
Custom DSI Reader
====================================

DSI Readers are the primary way to translate external data to metadata consistent with DSI. 
All Readers must be structured as a Class with 2 mandatory methods: ``__init__``, and ``add_rows``.

Loading Custom Reader into DSI
------------------------------
Before understanding the structure of Readers, it is important to know how they can be loaded via the User API and the Contributor API:

- **User API**: Users loading a custom external Reader can use the ``read()`` method from the DSI class.
  Unlike a normal ``read()``, the second argument should be the path to the Python script containing the user's custom Reader.
  
  This can be better seen in :ref:`user_external_reader` where a custom TextFile Reader is loaded into DSI with its data.
  
- **Contributor API**: Users loading a custom external Reader must first call ``Terminal.add_external_python_module()`` to temporarily register the Reader
  with DSI before loading the Reader and its data normally. For detailed instructions, follow :ref:`external_readers_writers_label`.

  Users intending to add the custom Reader to DSI's codebase must include the file in the `dsi/plugins <https://github.com/lanl/dsi/tree/main/dsi/plugins>`_ 
  directory and include the Reader name in the ``Terminal.VALID_READERS`` class variable of `dsi/core.py <https://github.com/lanl/dsi/blob/main/dsi/core.py>`_.
  If done correctly, the Reader will be accessible by ``Terminal.load_module()``.

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
Examples of DSI Readers can be found in `dsi/plugins/file_reader.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/file_reader.py>`_.
``Csv`` is an especially simple example to view for loading one table. 
``YAML1`` and ``TOML1`` are more complex examples with loading multiple tables of data with units