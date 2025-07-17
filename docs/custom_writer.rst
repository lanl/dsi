.. _custom_writer:

====================================
Custom DSI Writer
====================================

DSI Writers are the primary way to translate data from DSI to an external format.
All Writers must be structured as a Class with 2 mandatory methods: ``__init__``, and ``get_rows``.

Loading Custom Writer into DSI
------------------------------
Before understanding the structure of Writers, it is important to know how they can be loaded via the User API and the Contributor API:

- **User API**: Users loading a custom external Writer can use the ``write()`` method from the DSI class.
  Unlike a normal ``write()``, the second argument should be the path to the Python script containing the user's custom Writer.
  
  While there currently isn't an example of loading an external Writer, :ref:`user_external_reader` has a similar process to load an external Reader.

- **Contributor API**: Users loading a custom external Writer must first call ``Terminal.add_external_python_module()`` to temporarily register the Writer
  with DSI before loading the Writer and its data normally. For detailed instructions, follow :ref:`external_readers_writers_label`.

  Users intending to add the custom Writer to DSI's codebase must include the file in the `dsi/plugins <https://github.com/lanl/dsi/tree/main/dsi/plugins>`_ 
  directory and include the Writer name in the ``Terminal.VALID_WRITERS`` class variable of `dsi/core.py <https://github.com/lanl/dsi/blob/main/dsi/core.py>`_.
  If done correctly, the Writer will be accessible by ``Terminal.load_module()``.

Initializer: ``__init__(self) -> None:``
-----------------------------------------
``__init__`` is where you can include all of your initialization logic, and specify the parameters needed for a given application. 

Example ``__init__``: ::

  def __init__(self, filename, table_name) -> None:
    # see "plugins" to determine which superclass your Writer should extend
    super().__init__()

    self.output_filename = filename

    # a Writer might be table-specific and require a table_name input. Ex: Plotting a table's data.
    self.table_name = table_name

Get Rows: ``get_rows(self, collection) -> None``
------------------------------------------------
``get_rows`` is responsible for converting data from DSI's internal abstraction, an OrderedDict, to whichever format this Writer's goal is.

It is important to note that ``collection`` will be a nested OrderedDict with at least one table of data. 
Essentially, each entry in the larger OrderedDict has a table's name as a key and its data as a value. 
That table's data will be an inner OrderedDict.

Unlike DSI Readers, there is no standard structure for ``get_rows`` as each Writer can have a vastly different output. 
If a Writer requires units of data, or primary key/foreign key relations, they are stored in tables named 'dsi_units' and 'dsi_relations' respectively.

Various examples of ``get_rows`` can be found in `dsi/plugins/file_writer.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/file_writer.py>`_.


Contributing Your Writer
--------------------------
If your Writer is helpful and acceptable for public use, you should consider making a pull request (PR) into DSI.

Please note that any accepted PRs into DSI should satisfy the following:
 - Passes all tests in ``dsi/plugins/tests``
 - Has no ``pylama`` errors/warnings (see `dsi/.githooks <https://github.com/lanl/dsi/tree/main/.githooks>`_)