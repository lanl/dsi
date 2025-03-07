Plugins
=======
Plugins connect data-producing applications to DSI core functionalities. Plugins have *writers* or *readers* functions. 
A Plugin reader function deals with existing data files or input streams. 
A Plugin writer deals with generating new data. Plugins are modular to support user contribution.

Plugin contributors are encouraged to offer custom Plugin abstract classes and Plugin implementations. 
A contributed Plugin abstract class may extend another plugin to inherit the properties of the parent. 
In order to be compatible with DSI core, Plugins should produce data in Python built-in data structures or data structures sourced from the Python ``collections`` library.

Note that any contributed plugins or extension should include unit tests in  ``plugins/tests`` to demonstrate the new Plugin capability.

..  figure:: PluginClassHierarchy.png
    :alt: Figure depicting the current plugin class hierarchy.
    :class: with-shadow
    :scale: 70%

    Figure depicts prominent portion of the current DSI plugin class hierarchy.

.. automodule:: dsi.plugins.plugin
   :members:
   :special-members: __init__

Metadata Processing
-------------------

**Note for users:** StructuredMetadata class is used to assign data from a file_reader to the DSI abstraction in core. 
If data in a user-written reader is restructured as an OrderedDict, only need to call set_schema_2() at bottom of add_rows().

.. automodule:: dsi.plugins.metadata
   :members:
   :special-members: __init__

File Readers
------------

Note for users:
   - Assume names of data structure from all data sources are consistent/stable. Ex: table/column names are consistent. Number of columns in table CAN vary.
   - Plugin readers in DSI repo can/should handle data files with mismatched number of columns. Ex: file1: table1 has columns a, b, c. file2: table1 has columns a, b, d
   
      - if only reading in one table, users can utilize python pandas to stack mulutiple dataframes vertically (CSV reader)
      - if ingesting multiple tables at a time, users must pad tables with null data (YAML1 uses this and has example code at bottom of add_row() to implement this)
.. automodule:: dsi.plugins.file_reader
   :members:
   :special-members: __init__
   :exclude-members: FileReader

File Writers
------------

Note for users:
   - If runTable flag is True in Terminal instantiation, the run table is only included in ER Diagram writer if data is processed from a backend. 
     View :ref:`example4_label` to see an instance of a runTable included in an ER diagram
.. automodule:: dsi.plugins.file_writer
   :members:
   :special-members: __init__
   :exclude-members: FileWriter

Environment Plugins
-------------------
.. automodule:: dsi.plugins.env
   :members:
   :special-members: __init__

Optional Plugin Type Enforcement
--------------------------------

Plugins take data in an arbitrary format, and transform it into metadata which is queriable in DSI. Plugins may enforce types, but they are not required to enforce types. Plugin type enforcement can be static, like the Hostname default plugin. Plugin type enforcement can also be dynamic, like the Bueno default plugin.


.. automodule:: dsi.plugins.plugin_models
   :members: