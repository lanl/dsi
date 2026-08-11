Contributor Introduction
========================

The main Introduction page gave a brief description of the Readers/Writers, Backends, and DSI Core. 
This page will provide a more detailed explanation of them, with the following pages delving into how contributors can create their own compatible with DSI.

Similar to the Python User API which uses ``dsi.py``, contributors can view ``core.py`` and its documentation, :ref:`core_page`, to use the most-flexible version of DSI.

DSI Readers/Writers Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Readers/Writers transform an arbitrary data source into a format that is compatible with the DSI core. 
The parsed and queryable attributes of the data are called *metadata* -- data about the data. Metadata share the same security profile as the source data.

A simple data reader might parse an application's output file and place it into a core-compatible data structure such as Python built-ins and members of the popular Python ``collection`` module. 
A simple data writer might execute an application to supplement existing data and queryable metadata, e.g., adding locations of outputs data or plots after running an analysis workflow.

Readers/Writers are defined by a base abstract class, and support child abstract classes which inherit the properties of their ancestors.

A subset of DSI's Readers and Writers are:

..  figure:: images/PluginClassHierarchy.png
    :alt: Figure depicting the current Reader/Writer class hierarchy.
    :class: with-shadow
    :scale: 70%
    :align: center

    Figure depicting the current DSI Reader/Writer class hierarchy.

Backend Abstract Classes
~~~~~~~~~~~~~~~~~~~~~~~~

Backends are an interface between the core and a storage medium designed to support a user-needed functionality.  
Given the user metadata captured by a DSI frontend, a typical functionality needed by DSI users is to query that metadata. 
Because the files associated with the queryable metadata may be spread across filesystems and security domains, 
a supporting backend is required to assemble query results and present them to the DSI core for transformation and return.


DSI currently has two categories of backends:

Filesystem Backends - Create a permanent database file in the user's local directory:
  - SQLite: Python based SQL database and backend; the **default** DSI API backend. Supports POSIX-enforced file permissions.
  - DuckDB: In-process SQL database designed for fast queries on large data files. Supports POSIX-enforced file permissions.
  - GUFI: the `Grand Unified File Index system <https://github.com/mar-file-system/GUFI>`_ ; developed at LANL. 
    GUFI is a fast, secure metadata search across a filesystem accessible to both privileged and unprivileged users.

Webserver Backends - Create a connection to a remote data platform for users to retrieve data in-memory:
  - NDP (Read-only): Connection to data on the `National Data Platform <https://nationaldataplatform.org/>`_
  - Oceans11 (Read-only): Connection to data on DSI's `open data server: <https://oceans11.lanl.gov/>`_
  - OSTI (Read-only): Connection to data hosted by the Department of Energy's `Office of Scientific and Technical Information <https://www.osti.gov/>`_

DSI Core
~~~~~~~~

DSI basic functionality is contained within the middleware known as the *core*.  
The DSI core is focused on delivering user-queries on unified metadata which can be distributed across many files and security domains. 
DSI currently supports Linux, and is tested on RedHat- and Debian-based distributions. 
The DSI core is a home for DSI Readers/Writers and an interface for DSI Backends.

DSI Sync
~~~~~~~~

DSI data movement functionality is contained within the ``Sync`` class. 
This middleware enables users to index and copy/move metadata + data to a remote location, or to retrieve the metadata + data from several remote locations.
The currently supported copy/move tools are:

- Unix copy: effectively a "cp" command on a filesystem
- SCP: Secure Copy Protocol - tool that securely transfers data between a local machine and a remote server
- RSync: Remote Sync - more powerful tool than SCP that efficiently transfer data between a local machine and a remote server
- conduit: copy tool used by a certain HPC to move data between a staging area and a permanent store
- pfcp: copy tool used by a certain HPC to move data between a staging area and a permanent store