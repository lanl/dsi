Core
===================
The DSI Core middleware defines the Terminal concept. An instantiated Terminal is the human/machine DSI interface. The person setting up a Core Terminal only needs to know how they want to ask questions, and what metadata they want to ask questions about. If they don’t see an option to ask questions the way they like, or they don’t see the metadata they want to ask questions about, then they should ask a Driver Contributor or a Plugin Contributor, respectively.

A Core Terminal is a home for Plugins, and an interface for Drivers. A Core Terminal is instantiated with a set of default Plugins and Drivers, but they must be loaded before a user query is attempted. Here's an example of how you might work with DSI using an interactive Python interpreter for your data science workflows::

>>> from dsi.core import Terminal
>>> a=Terminal()
>>> a.list_available_modules('plugin')
>>> # ['Bueno', 'Hostname', 'SystemKernel']
>>> a.load_module('plugin','Bueno','consumer',filename='./data/bueno.data')
>>> # Bueno plugin consumer loaded successfully.
>>> a.load_module('plugin','Hostname','producer')
>>> # Hostname plugin producer loaded successfully.
>>> a.list_loaded_modules()
>>> # {'producer': [<dsi.plugins.env.Hostname object at 0x7f21232474d0>], 
>>> #  'consumer': [<dsi.plugins.env.Bueno object at 0x7f2123247410>], 
>>> #  'front-end': [], 
>>> #   'back-end': []}


At this point, you might decide that you are ready to collect data for inspection. It is possible to utilize DSI Drivers to load additional metadata to supplement your Plugin metadata, but you can also sample Plugin data and search it directly. 


The process of transforming a set of Plugin producers and consumers into a querable format is called transloading. A DSI Core Terminal has a ``transload()`` method which may be called to execute all Plugins at once::

>>> a.transload()
>>> a.active_metadata
>>> # OrderedDict([('uid', [1000]), ('effective_gid', [1000]), ('moniker', ['qwofford'])...

Once a Core Terminal has been transloaded, no further Plugins may be added. However, the transload method can be used to samples of each plugin as many times as you like::

>>> a.transload()
>>> a.transload()
>>> a.transload()
>>> a.active_metadata
>>> # OrderedDict([('uid', [1000, 1000, 1000, 1000]), ('effective_gid', [1000, 1000, 1000...

If you perform data science tasks using Python, it is not necessary to create a DSI Core Terminal front-end because the data is already in a Python data structure. If your data science tasks can be completed in one session, it is not required to interact with DSI Drivers. However, if you do want to save your work, you can load a DSI Driver with a back-end function::

>>> a.list_available_modules('driver')
>>> # ['Gufi', 'Sqlite', 'Parquet']
>>> a.load_module('driver','Parquet','back-end',filename='parquet.data')
>>> # Parquet driver back-end loaded successfully.
>>> a.list_loaded_modules()
>>> # {'producer': [<dsi.plugins.env.Hostname object at 0x7f21232474d0>], 
>>> #  'consumer': [<dsi.plugins.env.Bueno object at 0x7f2123247410>], 
>>> #  'front-end': [], 
>>> #   'back-end': [<dsi.drivers.parquet.Parquet object at 0x7f212325a110>]}
>>> a.artifact_handler(interaction_type='put')

The contents of the active DSI Core Terminal metadata storage will be saved to a Parquet object at the path you provided at module loading time.

It is possible that you prefer to perform data science tasks using a higher level abstraction than Python itself. This is the purpose of the DSI Driver front-end functionality. Unlike Plugins, Drivers can be added after the initial ``transload()`` operation has been performed::

>>> a.load_module('driver','Parquet','front-end',filename='parquet.data')
>>> # Parquet driver front-end loaded successfully.
>>> a.list_loaded_modules()
>>> # {'producer': [<dsi.plugins.env.Hostname object at 0x7fce3c612b50>], 
>>> # 'consumer': [<dsi.plugins.env.Bueno object at 0x7fce3c622110>], 
>>> # 'front-end': [<dsi.drivers.parquet.Parquet object at 0x7fce3c622290>], 
>>> # 'back-end': [<dsi.drivers.parquet.Parquet object at 0x7fce3c622650>]}

Any front-end may be used, but in this case the Parquet driver has a front-end implementation which builds a jupyter notebook from scratch that loads your metadata collection into a Pandas Dataframe. The Parquet front-end will then launch the Jupyter Notebook to support an interactive data science workflow::

>>> a.artifact_handler(interaction_type='inspect')
>>> # Writing Jupyter notebook...
>>> # Opening Jupyter notebook...

.. image:: jupyter_frontend.png
        :scale: 33%

You can then close your Jupyter notebook, ``transload()`` additionally to increase your sample size, and use the interface to explore more data.

Although this demonstration only used one Plugin per Plugin functionality, any number of plugins can be added to collect an arbitrary amount of queriable metadata::

>>> a.load_module('plugin','SystemKernel','producer')
>>> # SystemKernel plugin producer loaded successfully
>>> a.list_loaded_modules()
>>> # {'producer': [<dsi.plugins.env.Hostname object at 0x7fce3c612b50>, <dsi.plugins.env.SystemKernel object at 0x7fce68519250>], 
>>> # 'consumer': [<dsi.plugins.env.Bueno object at 0x7fce3c622110>], 
>>> # 'front-end': [<dsi.drivers.parquet.Parquet object at 0x7fce3c622290>], 
>>> # 'back-end': [<dsi.drivers.parquet.Parquet object at 0x7fce3c622650>]}

.. automodule:: dsi.core
      :members:
