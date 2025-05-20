
DSI Examples
============

PENNANT mini-app
----------------

`PENNANT` is an unstructured mesh physics mini-application developed at Los Alamos National Laboratory
for advanced architecture research.
It contains mesh data structures and a few
physics algorithms from radiation hydrodynamics and serves as an example of
typical memory access patterns for an HPC simulation code.

This DSI PENNANT example is used to show a common use case: create and query a set of metadata derived from an ensemble of simulation runs. 
The example GitHub directory includes 10 PENNANT runs using the PENNANT *Leblanc* test problem.

In the first step, a python script is used to parse the slurm output files and create a CSV (comma separated value) file with the output metadata.

.. code-block:: unixconfig

   python3 parse_slurm_output.py

In the second step, another python script,

.. code-block:: unixconfig

   python3 dsi_pennant.py

reads in the CSV file and creates a database:

.. literalinclude:: ../examples/pennant/dsi_pennant.py

Resulting in the output of the query:

..  figure:: images/example-pennant-output.png
    :alt: Screenshot of computer program output.
    :class: with-shadow

    The output of the PENNANT example.


Wildfire Dataset
----------------

This example highlights the use of the DSI framework with QUIC-Fire simulation data and resulting images. 
QUIC-Fire is a fire-atmosphere modeling framework for prescribed fire burn analysis. 
It is light-weight (able to run on a laptop), allowing scientists to generate ensembles of thousands of simulations in weeks. 
This QUIC-fire dataset is an ensemble of prescribed fire burns for the Wawona region of Yosemite National Park.

The original file, wildfire.csv, lists 1889 runs of a wildfire simulation. Each row is a unique run with input and output values and associated image url. 
The columns list the various parameters of interest. 
The input columns are: wild_speed, wdir (wind direction), smois (surface moisture), fuels, ignition, safe_unsafe_ignition_pattern, 
safe_unsafe_fire_behavior, does_fire_meet_objectives, and rationale_if_unsafe. 
The output of the simulation (and post-processing steps) include the burned_area and the url to the wildfire images stored on the San Diego Super Computer.

All paths in this example are defined from the main dsi repository folder, assumed to be ``~/<path-to-dsi-directory>/dsi``.

To run this example, load dsi and run:

.. code-block:: unixconfig

   python3 examples/wildfire/wildfire.py

.. literalinclude:: ../examples/wildfire/wildfire.py

.. This will generate a wildfire.cdb folder with downloaded images from the server and a data.csv file of numerical properties of interest. 
.. This cdb folder is called a `Cinema`_ database (CDB). 
.. Cinema is an ecosystem for management and analysis of high dimensional data artifacts that promotes flexible and interactive data exploration and analysis.  
.. A Cinema database is comprised of a CSV file where each row of the table is a data element (ex: run or ensemble member of a simulation) and each column is a property of the data element. 
.. Any column name that starts with 'FILE' is a path to a file associated with the data element.  
.. This could be an image, a plot, a simulation mesh or other data artifact.

.. Cinema databases can be visualized through various tools. We illustrate two options below:

.. To visualize the results using Jupyter Lab and Plotly, run:

.. .. code-block:: unixconfig

..    python3 -m pip install plotly
..    python3 -m pip install jupyterlab


.. Open Jupyter Lab with:

.. .. code-block:: unixconfig

..   jupyter lab --browser Firefox

.. and navigate to ``wildfire_plotly.ipynb``.  Run the cells to visualize the results of the DSI pipeline.

.. ..  figure:: images/example-wildfire-jupyter.png
..     :alt: User interface showing the visualization code to load the CSV file and resultant parallel coordinates plot.
..     :class: with-shadow
..     :scale: 50%

..     Screenshot of the JupyterLab workflow. 
..     The CSV file is loaded and used to generate a parallel coordinates plot showing the parameters of interest from the simulation.

.. Another option is to use `Pycinema`_, a QT-based GUI that supports visualization and analysis of Cinema databases. 
.. To open a pycinema viewer, first install pycinema and then run the example script.

.. .. code-block:: unixconfig

..    python3 -m pip install pycinema
..    cinema examples/wildfire/wildfire_pycinema.py


.. ..  figure:: images/example-wildfire-pycinema.png
..     :class: with-shadow
..     :scale: 40%

..     Screenshot of the Pycinema user interface showing the minimal set of components. 
..     Left: the nodeview showing the various pycinema components in the visualization pipeline; 
..     upper-right: the table-view; 
..     lower-right: the image view. 
..     Pycinema components are linked such that making a selection in one view will propagate to the other views.


.. .. _PENNANT: https://github.com/lanl/PENNANT
.. .. _Cinema: https://github.com/cinemascience
.. .. _PyCinema: https://github.com/cinemascience/pycinema

.. .. _user_schema_example_label:

.. Complex Schemas in DSI
.. ----------------------

.. This example details how to structure a JSON file for the DSI Schema Reader to store all table primary key - foreign key relations.

.. If we consider a workflow where a user reads in a complex schema for YAML data and generates an ER Diagram:

.. .. literalinclude:: ../examples/user/7.schema.py

.. where ``examples/data/example_schema.json`` is:

.. .. code-block:: json

..    {
..       "math": {
..          "primary_key": "specification",
..          "foreign_key": {
..                "b": ["address", "i"]
..          }
..       }, 
..       "address": {
..          "primary_key": "i",
..          "foreign_key": {
..                "h": ["physics", "n"]
..          }
..       }, 
..       "physics": {
..          "primary_key": "n"
..       }
..    }
   
.. the ER diagram looks like:

.. ..  figure:: images/schema_erd.png
..     :scale: 35%
..     :align: center

..     Entity Relationship Diagram of YAML data. 
..     Shows table relations between the math, address and physics tables.

.. NOTE: Schema JSON files do not need "comment" keys. They have only been included for clarity.

.. For futher clarity, each schema file must be structured as a dictionary where:

..    - Each table with a relation is a key whose value is a nested dictionary storing primary and foreign key information

..       - Ex from above: "math" : { ... }
..    - The nested dictionary has 2 keys: 'primary_key' and 'foreign_key' which must be spelled exactly the same to be processed:
..    - The value of 'primary_key' is this table's column that is a primary key

..       - Ex from above: "primary_key" : "specification"
..    - The value of 'foreign_key' is another inner dictionary, since a table can have multiple foreign keys:

..       - Each key in this dictionary is a column in this table that serves as a foreign key
..       - Each value is a list with 2 elements - the table storing the associated primary key, and the column in that table which is the primary key
..       - Ex: "foreign_key" : { "name" : ["table1", "table1_id"] , "age" : ["table2", "table2_id"] }
..    - If a table does not have a primary or foreign key, you do not have to include them in the table's nested dictionary

.. .. For example, if a user has a a table 'Payments' with a primary key 'id' 
.. .. and a foreign key 'user_name' that points to another table 'Users' with primary key 'name', the schema is: 

.. .. .. code-block:: json
   
.. ..    {
.. ..       "Payments": {
.. ..          "primary_key" : "id",
.. ..          "foreign_key" : {
.. ..             "user_name" : ["Users", "name"]
.. ..          }
.. ..       }
.. ..    }

.. For example, if we update ``examples/data/example_schema.json``, by adding a foreign key in 'math' pointing to 'n' in 'physics':

.. .. code-block:: json

..    {
..       "math": {
..          "primary_key": "specification",
..          "foreign_key": {
..                "b": ["address", "i"],
..                "c": ["physics", "n"]  // <--- new foreign key
..          }
..       }, 
..       "address": {
..          "primary_key": "i",
..          "foreign_key": {
..                "h": ["physics", "n"]
..          }
..       }, 
..       "physics": {
..          "primary_key": "n"
..       }
..    }

.. our new ER diagram would be:

.. ..  figure:: images/schema_erd_added.png
..     :scale: 35%
..     :align: center
   
..     ER Diagram of same data. However, there is now an additional foreign key from 'math' to 'physics'