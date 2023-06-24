Core
===================
The DSI Core middleware defines the Terminal concept. An instantiated Terminal is the human/machine DSI interface. The person setting up a Core Terminal only needs to know how they want to ask questions, and what metadata they want to ask questions about. If they don’t see an option to ask questions the way they like, or they don’t see the metadata they want to ask questions about, then they should ask a Driver Contributor or a Plugin Contributor, respectively.

A Core Terminal is a home for Plugins, and an interface for Drivers. A Core Terminal is instantiated with a set of default Plugins and Drivers, but they must be loaded before a user query is attempted::

>>> from dsi.core import Terminal
>>> a = Terminal()
>>> a.list_available_modules('plugin')
>>> # [ Hostname, SystemKernel, Bueno ]
>>> a.list_available_modules('driver')
>>> # [ Gufi, Sqlite ]
>>> a.load_module('plugin', 'Bueno','consumer')
>>> # Bueno plugin consumer loaded successfully.
>>> a.load_module('driver', 'Sqlite','front-end', filename='bogus') # Filename param requirement will be removed.
>>> # Sqlite driver front-end loaded successfully.
>>> a.load_module('driver', 'Sqlite','back-end',filename='yadda')
>>> # Sqlite driver back-end loaded successfully.
>>> a.list_loaded_modules()
>>> {
>>>  'producer': [], 
>>>  'consumer': [<dsi.plugins.env.Bueno object at 0x2b028dbde250>], 
>>>  'front-end': [<dsi.drivers.sqlite.Sqlite object at 0x2b028d8f7fd0>], 
>>>  'back-end': [<dsi.drivers.sqlite.Sqlite object at 0x2b028d914250>]
>>> }
>>> a.execute() # Not yet implemented

It is the Plugin contributor's responsibility to make sure consumer or producer functions succeed or report ``NotImplementedError``. It is the Driver contributor's responsiblity to make sure the front-end or back-end functions succeed or report ``NotImplementedError``. 

.. automodule:: dsi.core
      :members:
