.. _sync_page:

Sync
====

The DSI Sync middleware defines the `Sync` concepts.
An instantiated `Sync` supports data movement capabilities between local and remote locations and captures metadata documentation


The purpose of ``Sync`` is to provide file metadata documentation and data movement capabilities when moving data to/from local and remote locations.
The purpose of data documentation is to capture and archive metadata
(i.e. location of local file structure, their access permissions, file sizes, and creation/access/modification dates),
and track their movement to the remote location for future access.

The functions, ``index``, ``copy``, and ``move`` serve as mechanisms to index data for documentation, and copy/move data  by creating a DSI 
database using ``Terminal``.
The function ``get`` serves as a mechanism to retrieve data from remote locations by downloading an existing DSI database that contains the 
pointers to the target data.

.. autoclass:: dsi.sync.Sync
      :members:
      :special-members: __init__


.. Eventually make sync examples separate from dev core examples and list them here
