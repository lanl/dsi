Core
===================
The DSI Core middleware defines the Terminal concept. An instantiated Terminal is the human/machine DSI interface. The person setting up a Core Terminal only needs to know how they want to ask questions, and what metadata they want to ask questions about. If they don’t see an option to ask questions the way they like, or they don’t see the metadata they want to ask questions about, then they should ask a Driver Contributor or a Plugin Contributor, respectively.

A Core Terminal is a home for Plugins, and an interface for Drivers. A Core Terminal is instantiated with a set of default Plugins and Drivers, but they must be loaded before a user query is attempted::

>>> from dsi.core import Terminal
>>> a = Terminal()
>>> a.list_available_modules('plugin')
>>> # Loadable plugins: Hostname, SystemKernel, Bueno
>>> a.list_available_modules('driver')
>>> # Loadable drivers: Gufi, Sqlite
>>> a.load_module('plugin', 'Bueno')
>>> a.load_module('driver', 'Sqlite')

.. automodule:: dsi.core
      :members:
