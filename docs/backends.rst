Backends
========

Backends connect users to DSI Core middleware and backends allow DSI middleware data structures to read and write to persistent external storage. 
Backends are modular to support user contribution.  Backend contributors are encouraged to offer custom backend abstract classes and backend implementations.  
A contributed backend abstract class may extend another backend to inherit the properties of the parent. 
In order to be compatible with DSI core middleware, backends should create an interface to Python built-in data structures or data structures from the Python ``collections`` library. 
Backend extensions will be accepted conditional to the extention of ``backends/tests`` to demonstrate new Backend capability. 
We can not accept pull requests that are not tested.

Note that any contributed backends or extensions should include unit tests in  ``backends/tests`` to demonstrate the new Backend capability.

.. figure:: BackendClassHierarchy.png
   :alt: Figure depicting the current backend class hierarchy.
   :class: with-shadow
   :scale: 100%

   Figure depicts the current DSI backend class hierarchy.

.. automodule:: dsi.backends.filesystem
   :members:

SQLite
------

.. automodule:: dsi.backends.sqlite
   :members:

SQLAlchemy
------

.. automodule:: dsi.backends.sqlalchemy
   :members:

GUFI
------

.. automodule:: dsi.backends.gufi
   :members:

Parquet
------

.. automodule:: dsi.backends.parquet
   :members:
