Backends
========================

Backends have front-end and back-end functions. Backends connect users to DSI Core middleware (front-end), and Backends allow DSI Middleware data structures to read and write to persistent external storage (back-end). Backends are modular to support user contribution. Backend contributors are encouraged to offer custom Backend abstract classes and Backend implementations. A contributed Backend abstract class may extend another Backend to inherit the properties of the parent. In order to be compatible with DSI Core middleware, Backends should create an interface to Python built-in data structures or data structures from the Python ``collections`` library. Backend extensions will be accepted conditional to the extention of ``backends/tests`` to demonstrate new Backend capability. We can not accept pull requests that are not tested.


.. image:: BackendClassHierarchy.png

.. automodule:: dsi.backends.filesystem
   :members:

.. automodule:: dsi.backends.sqlite
   :members:

.. automodule:: dsi.backends.gufi
   :members:

.. automodule:: dsi.backends.parquet
   :members:


