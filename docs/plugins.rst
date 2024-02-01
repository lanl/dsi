Plugins
===================
Plugins connect data-producing applications to DSI middleware. Plugins have "writers" or "readers" functions. A Plugin reader function deals with existing data files or input streams. A Plugin writer deals with generating new data. Plugins are modular to support user contribution. Plugin contributors are encouraged to offer custom Plugin abstract classes and Plugin implementations. A contributed Plugin abstract class may extend another plugin to inherit the properties of the parent. In order to be compatible with DSI middleware, Plugins should produce data in Python built-in data structures or data structures sourced from the Python ``collections`` library. Plugin extensions will be accepted conditional to the extention of ``plugins/tests`` to demonstrate the new Plugin capability. We can not accept pull requests that are not tested.

.. image:: PluginClassHierarchy.png

.. automodule:: dsi.plugins.plugin
   :members:

.. automodule:: dsi.plugins.metadata
   :members:

.. automodule:: dsi.plugins.env
   :members:

