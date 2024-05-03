Core
====

The DSI Core middleware defines the Terminal concept. An instantiated Terminal is the human/machine DSI interface. The person setting up a Core Terminal only needs to know how they want to ask questions, and what metadata they want to ask questions about. If they don’t see an option to ask questions the way they like, or they don’t see the metadata they want to ask questions about, then they should ask a Backend Contributor or a Plugin Contributor, respectively.

A Core Terminal is a home for Plugins (Readers/Writers), and an interface for Backends. A Core Terminal is instantiated with a set of default Plugins and Backends, but they must be loaded before a user query is attempted. ``core.py`` contains examples of how you might work with DSI using an interactive Python interpreter for your data science workflows:

.. literalinclude:: ../examples/coreterminal.py


At this point, you might decide that you are ready to collect data for inspection. It is possible to utilize DSI Backends to load additional metadata to supplement your Plugin metadata, but you can also sample Plugin data and search it directly.


The process of transforming a set of Plugin writers and readers into a queryable format is called transloading. A DSI Core Terminal has a ``transload()`` method which may be called to execute all Plugins at once::

>>> a.transload()
>>> a.active_metadata
>>> # OrderedDict([('uid', [1000]), ('effective_gid', [1000]), ('moniker', ['qwofford'])...

Once a Core Terminal has been transloaded, no further Plugins may be added.

Core:Sync
---------

The DSI Core middleware also defines data management functionality in ``Sync``. The purpose of ``Sync`` is to provide file metadata documentation and data movement capabilities when moving data to/from local and remote locations. The purpose of data documentation is to capture and archive metadata (i.e. location of local file structure, their access permissions, file sizes, and creation/access/modification dates) and track their movement to the remote location for future access. The primary functions, ``Copy``, ``Move``, and ``Get`` serve as mechanisms to copy data, move data, or retrieve data from remote locations by creating a DSI database in the process, or retrieving an existing DSI database that contains the location(s) of the target data.

Core Modules and Functions
--------------------------

.. automodule:: dsi.core
      :members:
