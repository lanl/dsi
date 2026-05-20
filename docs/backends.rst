.. _backend_section_label:

Backends
========

Backends connect users to DSI Core middleware and allow DSI middleware data structures to read and write to persistent external storage. 

Backends are modular to support user contribution, and users are encouraged to offer custom backend abstract classes and backend implementations.  
A contributed backend abstract class may extend another backend to inherit the properties of the parent. 

In order to be compatible with DSI core middleware, backends need to interface with Python built-in data structures and with the Python ``collections`` library. 

Note that any contributed backends or extensions must include unit tests in ``backends/tests`` to demonstrate new Backend capability. 
We will not accept pull requests that are not tested.

.. figure:: images/BackendClassHierarchy.png
   :alt: Figure depicting the current backend class hierarchy.
   :class: with-shadow
   :scale: 100%
   :align: center

   Figure depicts the current DSI backend class hierarchy.

Filesystem Backends
~~~~~~~~~~~~~~~~~~~
Filesystem backends enable a user to ingest data into a local database file, and to query that file for metadata.
The database file is stored in the user's local directory and is persistent across user sessions.
DSI's Filesystem backends support POSIX-enforced file permissions, so users can control access to their data.

SQLite
------

.. automodule:: dsi.backends.sqlite
   :members: Sqlite
   :special-members: __init__

DuckDB
------

.. automodule:: dsi.backends.duckdb
   :members: DuckDB
   :special-members: __init__

.. SQLAlchemy
.. -----------

.. .. automodule:: dsi.backends.sqlalchemy
..    :members:
..    :special-members: __init__

GUFI
------

.. automodule:: dsi.backends.gufi
   :members:
   :special-members: __init__


Webserver Backends
~~~~~~~~~~~~~~~~~~
Webserver backends enable a user to connect to a remote data platform and interact with retrieved data in-memory.

NDP (Read-only)
---------------

.. automodule:: dsi.backends.ndp
   :members: NDP
   :special-members: __init__

Oceans11 (Read-only)
--------------------

.. automodule:: dsi.backends.oceans11
   :members: Oceans11
   :special-members: __init__

OSTI (Read-only)
----------------

.. automodule:: dsi.backends.osti
   :members: OSTI
   :special-members: __init__