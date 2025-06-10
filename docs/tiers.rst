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
     - 1.0

   * - Write data
     - Plugins: Writers
     - Ability to write data; users can create data-specific writers
     - 1.0

   * - Ingest data
     - Core: Terminal.Ingest
     - Store metadata and data from Readers into backends
     - 1.0
  
   * - Query data
     - Core: Terminal.Query
     - Query data from backends
     - 1.0
     
   * - Process data
     - Core: Terminal.Process
     - Store metadata/data from backends in the DSI abstraction
     - 1.1

   * - Data Interaction
     - Core: Terminal.Notebook
     - Generate Python notebook filled with data from a backend
     - 1.1

   * - Find
     - Core: Terminal.Find()
     - Search across a backend to retrieve data matching a query
     - 1.1

   * - Move
     - Core: Sync.Move
     - Move data between file store types
     - 1.1

   * - Versioning
     - Core
     - Track and identify different versions of the same data
     - TBD
