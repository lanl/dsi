DSI Development Plans
=====================

DSI |version_num| has a core set of capabilities with plans to add functionality in future releases.  
This is user-driven so feel free to raise an issue on the `DSI GitHub repo <https://github.com/lanl/dsi>`_ for suggested capabilities.

Current capabilities include the DSI Core and associated DSI data services. 
The DSI data services include the functionality to store and retrieve user metadata in DSI accessible storage.

DSI Functionalities
^^^^^^^^^^^^^^^^^^^^^^^^

* DSI software/API (|version_num|) released via GitHub
* Can be used on its own or as part of a data service workflow
* Provides an API for users/services

.. list-table:: Current and Future DSI Capabilities
   :widths: 20 20 55 5
   :header-rows: 1

   * - Functionality
     - DSI module
     - Description
     - Release

   * - Read data
     - Plugins: Readers
     - Ability to read in data; users can create data-specific readers
     - DSI |version_num|

   * - Write data
     - Plugins: Writers
     - Ability to write data; users can create data-specific writers
     - DSI |version_num|

   * - Ingest data
     - Core: Terminal.artifact_handler ('ingest')
     - Store metadata and data from Readers into backends
     - DSI |version_num|
  
   * - Process data
     - Core: Terminal.artifact_handler ('process')
     - Store metadata and data from backends in DSI abstraction layer
     - DSI |version_num|
  
   * - Query data
     - Core: Terminal.artifact_handler ('query')
     - Query data from backends
     - DSI |version_num|

   * - Interact with data
     - Core: Terminal.artifact_handler ('notebook')
     - Generate Python notebook filled with data from a backend
     - DSI |version_num|

   * - Find
     - Core: Find, Find_Table, Find_Column, Find_Cell
     - Search across all parts of a backend to retrieve data matching a query
     - DSI |version_num|

   * - Move
     - Core:Sync
     - Move data between file store types
     - DSI |version_num|

   * - Versioning
     - Core
     - Track and identify different versions of the same data
     - Future release
