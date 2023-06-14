Plugins
===================

Plugins are an abstraction to connect a data source (ex- physics simulation) to the DSI core middleware. Plugins are responsible for transforming data source outputs into a DSI compatible middleware format. Currently we require that Plugins produce Python built-in data structures, or an ``OrderedDict`` from the ``collections`` Python library. Plugin inputs may be anything, however, and we encourage the open source community to act as Plugin Authors to produce Plugin contributions. Feel free to make Pull Requests into the project source with your Plugins. Please provide Unit tests for these contributions.



.. automodule:: dsi.plugins.structured_metadata_plugin
   :members:

.. automodule:: dsi.plugins.env
   :members:

