====================================
Custom DSI Writer
====================================

DSI Writers are the primary way to convert data from DSI to an external format, and they must each include 2 methods, ``__init__``, and ``get_rows``.

Loading Custom Writer into DSI
------------------------------
Before explaining the structure of Writers, it is important to note there are two ways to load your Writer, externally and internally.

 - If your Writer is intended for use within your own code base and not added to DSI's modules or for public use, you must load it externally.
   Doing so allows you to store your Writer separately from DSI yet compatible with all versions of DSI.

    - With the ``Core.Terminal.add_external_python_module`` method, you can make your Writer temporarily accessible to DSI in a workflow and load normally.
    - A similar example can be better seen at :ref:`external_readers_writers_label` where you can try loading an external TextFile reader class.
      While that example is meant for DSI Readers, the process to load them is the same

 - If you want your Writer loadable internally with the rest of the provided implementations (in 
   `dsi/plugins <https://github.com/lanl/dsi/tree/main/dsi/plugins>`_), it must be registered in the ``VALID_WRITERS`` class variable of ``Terminal`` in 
   `dsi/core.py <https://github.com/lanl/dsi/blob/main/dsi/core.py>`_. 
   If this is done correctly, your Writers will be loadable by the ``load_module`` method of ``Terminal``.

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
If a Writer requires units of data, or primary key/foreign key relations, they are stored in tables named 'dsi_units' amd 'dsi_relations' respectively.

Various examples of ``get_rows`` can be found in `dsi/plugins/file_writer.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/file_writer.py>`_.


Contributing Your Writer
--------------------------
If your Writer is helpful and acceptable for public use, you should consider making a pull request (PR) into DSI.

Please note that any accepted PRs into DSI should satisfy the following:
 - Passes all tests in ``dsi/plugins/tests``
 - Has no ``pylama`` errors/warnings (see `dsi/.githooks <https://github.com/lanl/dsi/tree/main/.githooks>`_)