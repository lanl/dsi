DSI Readers/Writers
=====================
Readers/Writers connect data-producing applications to DSI core functionalities.  
A Reader function deals with existing data files or input streams. 
A Writer function deals with generating new data formats.

Readers/Writers are modular to support user contribution and contributors are encouraged to offer custom Readers/Writers abstract classes and implementations. 
A contributed Reader/Writer abstract class may either extend another Reader/Writer to inherit the properties of the parent, or be a completely new structure.

In order to be compatible with DSI, Readers should store data in data structures sourced from the Python ``collections`` library (OrderedDict).
Similarly, Writers should be compatible by accepting data structures from Python ``collections`` (OrderedDict) to export data/generate an image.

Any contributed Readers/Writers, or extension of one, should include unit tests in  ``plugins/tests`` to demonstrate the new capability.

..  figure:: images/PluginClassHierarchy.png
    :alt: Figure depicting the current Readers/Writers class hierarchy.
    :class: with-shadow
    :scale: 70%

    Figure depicts prominent portion of the current DSI Readers/Writers class hierarchy.

.. automodule:: dsi.plugins.plugin
   :members:
   :special-members: __init__

Metadata Processing
-------------------

**Note for users:** ``StructuredMetadata`` class is used to assign data from a `file_reader` to the DSI abstraction in core. 
If data in a user-written reader is structured as an OrderedDict, only need to call ``set_schema_2()`` at the end of the reader's ``add_rows()``

.. automodule:: dsi.plugins.metadata
   :members:
   :special-members: __init__

File Readers
------------

Note for users:
   - DSI assumes data structure from all data sources are consistent/stable. Ex: table/column names MUST be consistent. 
     Number of columns in a table CAN vary.
   - DSI Readers can handle data files with mismatched number of columns. Ex: file1: table1 has columns a, b, c. file2: table1 has columns a, b, d
   
      - if only reading in one table at a time, users can utilize python pandas to stack mulutiple dataframes vertically (ex: CSV reader)
      - if multiple tables in a file, users must pad tables with null values (ex: YAML1, which has example code at bottom of ``add_rows()`` to implement this)

.. automodule:: dsi.plugins.file_reader
   :members:
   :special-members: __init__
   :exclude-members: FileReader

File Writers
------------

Note for users:
   - DSI's `runTable` is only included in File Writers if data was previously ingested into a backend in a Core.Terminal workflow 
     where `runTable` was set to True.
   
      - Ex: in :ref:`process_label`, `runTable` is included in a generated ER Diagram since it uses ingested data from 
        :ref:`ingest_label` where `runTable` = True
     
.. automodule:: dsi.plugins.file_writer
   :members:
   :special-members: __init__
   :exclude-members: FileWriter

Collection Readers
------------------

These Readers are created to load data from an in-memory data structure. Currently supports a Python ``dict`` or an ``OrderedDict`` from the ``collection`` module.

.. automodule:: dsi.plugins.collection_reader
   :members:
   :special-members: __init__

Environment Plugins
-------------------
.. automodule:: dsi.plugins.env
   :members:
   :special-members: __init__

Optional Plugin Type Enforcement
--------------------------------

Plugins take data in an arbitrary format, and transform it into metadata which is queriable in DSI. 
Plugins may enforce types, but they are not required to enforce types. 

Plugin type enforcement can be static, like the Hostname default plugin. 
Plugin type enforcement can also be dynamic, like the Bueno default plugin.


.. automodule:: dsi.plugins.plugin_models
   :members: