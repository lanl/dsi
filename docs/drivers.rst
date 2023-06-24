Drivers
========================

Drivers have front-end and back-end functions. Drivers connect users to DSI Core middleware (front-end), and Drivers allow DSI Middleware data structures to read and write to persistent external storage (back-end). Drivers are modular to support user contribution. Driver contributors are encouraged to offer custom Driver abstract classes and Driver implementations. A contributed Driver abstract class may extend another Driver to inherit the properties of the parent. In order to be compatible with DSI Core middleware, Drivers should create an interface to Python built-in data structures or data structures from the Python ``collections`` library. Driver extensions will be accepted conditional to the extention of ``drivers/tests`` to demonstrate new Driver capability. We can not accept pull requests that are not tested.


.. image:: DriverClassHierarchy.png

.. automodule:: dsi.drivers.filesystem
   :members:

.. automodule:: dsi.drivers.sqlite
   :members:

.. automodule:: dsi.drivers.gufi
   :members:

