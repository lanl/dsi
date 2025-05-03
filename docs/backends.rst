Backends
========

Backends connect users to DSI Core middleware and allow DSI middleware data structures to read and write to persistent external storage. 

Backends are modular to support user contribution, and users are encouraged to offer custom backend abstract classes and backend implementations.  
A contributed backend abstract class may extend another backend to inherit the properties of the parent. 
In order to be compatible with DSI core middleware, backends need to interface with Python built-in data structures and with the Python ``collections`` library. 

Note that any contributed backends or extensions must include unit tests in ``backends/tests`` to demonstrate new Backend capability. 
We can not accept pull requests that are not tested.

.. figure:: images/BackendClassHierarchy.png
   :alt: Figure depicting the current backend class hierarchy.
   :class: with-shadow
   :scale: 100%
   :align: center

   Figure depicts the current DSI backend class hierarchy.

.. automodule:: dsi.backends.filesystem
   :members:

SQLite
------

.. automodule:: dsi.backends.sqlite
   :members:
   :special-members: __init__

DuckDB
------

.. automodule:: dsi.backends.duckdb
   :members:
   :special-members: __init__

SQLAlchemy
-----------

.. automodule:: dsi.backends.sqlalchemy
   :members:
   :special-members: __init__

GUFI
------

.. automodule:: dsi.backends.gufi
   :members:
   :special-members: __init__

Parquet
--------

.. automodule:: dsi.backends.parquet
   :members:
   :special-members: __init__
