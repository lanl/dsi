Plugins
=======
Plugins connect data-producing applications to DSI core functionalities. Plugins have *writers* or *readers* functions. A Plugin reader function deals with existing data files or input streams. A Plugin writer deals with generating new data. Plugins are modular to support user contribution.

Plugin contributors are encouraged to offer custom Plugin abstract classes and Plugin implementations. A contributed Plugin abstract class may extend another plugin to inherit the properties of the parent. In order to be compatible with DSI core, Plugins should produce data in Python built-in data structures or data structures sourced from the Python ``collections`` library.

Note that any contributed plugins or extension should include unit tests in  ``plugins/tests`` to demonstrate the new Plugin capability.

..  figure:: PluginClassHierarchy.png
    :alt: Figure depicting the current plugin class hierarchy.
    :class: with-shadow
    :scale: 100%

    Figure depicts the current DSI plugin class hierarchy.

.. automodule:: dsi.plugins.plugin
   :members:

.. automodule:: dsi.plugins.metadata
   :members:

.. automodule:: dsi.plugins.env
   :members:

Optional Plugin Type Enforcement
==================================

Plugins take data in an arbitrary format, and transform it into metadata which is queriable in DSI. Plugins may enforce types, but they are not required to enforce types. Plugin type enforcement can be static, like the Hostname default plugin. Plugin type enforcement can also be dynamic, like the Bueno default plugin.


.. automodule:: dsi.plugins.plugin_models
   :members:

