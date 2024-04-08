DSI Development Tiers
=====================

The elements of the DSI project can be broken into Tiers.  Tier 1 is the DSI Core and associated DSI data services.  The DSI data services include the functionality to store and retrieve user metadata in DSI accessible storage. Tier 2 consists of DSI data workflows.  These are generally customized implementations of DSI for particular science applications.  Tier 2 workflows will often drive new capabilities within the DSI core.

DSI Tier 1
----------

DSI core functionalities
^^^^^^^^^^^^^^^^^^^^^^^^

* DSI software/API (v1.0) released via GitHub
* Can be used on its own or as part of a data service workflow
* Provides an API for users/services

.. list-table:: Current DSI Core Functionalities
   :widths: 25 25 50
   :header-rows: 1

   * - Functionality
     - DSI element
     - Description

   * - Ingest data
     - Plugins: Readers
     - Ability to read in data; users can create data-specific readers

   * - Write data
     - Plugins: Writers
     - Ability to write data; users can create data-specific writers

DSI data service
^^^^^^^^^^^^^^^^

* DSI software combined with storage resources
* Provides a generalized institutional data management service, e.g. readers, writers that can be used to store and update metadata on DSI storage resources
* DSI team is implementing the data service at LANL with targeted data storage; however, the storage could be set up anywhere


DSI Tier 2
----------

* Customized implementations of DSI for particular applications/workflows
* Higher level features/capabilities
* Can be implemented locally or connected to a DSI institutional service
* Typically developed by experienced DSI users/developers in consultation with an application team
* May incorporate other tools such as customized GUIs or Cinema
* Examples
  * Ensemble analysis such as the Wildfire data
  * AI/ML workflows
