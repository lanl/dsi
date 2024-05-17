DSI Development Plans
=====================

DSI v1.0 has a core set of capabilities with plans to add functionality in future releases.  This is user-driven so feel free to raise an issue on the `DSI GitHub repo <https://github.com/lanl/dsi>`_ for suggested capabilities.

Current capabilities include the DSI Core and associated DSI data services.  The DSI data services include the functionality to store and retrieve user metadata in DSI accessible storage.

DSI core functionalities
^^^^^^^^^^^^^^^^^^^^^^^^

* DSI software/API (v1.0) released via GitHub
* Can be used on its own or as part of a data service workflow
* Provides an API for users/services

.. list-table:: Current and Future DSI Capabilities
   :widths: 20 20 55 5
   :header-rows: 1

   * - Functionality
     - DSI module
     - Description
     - Release

   * - Ingest data
     - Plugins: Readers
     - Ability to read in data; users can create data-specific readers
     - DSI v1.0

   * - Write data
     - Plugins: Writers
     - Ability to write data; users can create data-specific writers
     - DSI v1.0

   * - Query/Find
     - Backends: Sqlite
     - Search across file store types and/or locations to retrieve data or files matching the query
     - DSI v1.0

   * - Move
     - Core:Sync
     - Move data between file store types
     - DSI v1.0

   * - Iterate/Process
     - Core
     - Action applied to collection returned from Query/Find
     - Future release

   * - Versioning
     - Core
     - Track and identify different versions of the same data
     - Future release
