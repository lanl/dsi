# QUIC-Fire Wildfire Simulations

This example highlights the use of the DSI Framework with QUIC-Fire fire-atmosphere simualation data and resulting images. The original file, wildfire.csv, lists 1889 runs of a wildfire simulation. Each row is a unique run with input and output values and associated image url. The columns list the various parameters of interest. The input columns are: wild_speed, wdir (wind direction), smois (surface moisture), fuels, ignition, safe_unsafe_ignition_pattern, safe_unsafe_fire_behavior, does_fire_meet_objectives, and rationale_if_unsafe. The output of the simulation (and post-processing steps) include the burned_area and the url to the wildfire images stored on the San Diego Super Computer.

All paths in this example are defined as from the main dsi repository folder, then follow the instructions.

To run this example, install dsi, move first into the root DSI directory, then:

    cd examples/wildfire/

USERS -- Run DSI's wildfire example:

    python3 wildfire.py

CONTRIBUTORS -- Run DSI's wildfire example:

    python3 wildfire_dev.py

This will generate a wildfire.cdb folder with downloaded images from the server and a data.csv file of numerical properties of interest. This *cdb* folder is called a Cinema Database (https://github.com/cinemascience). A cinema database is comprised of a *csv* file where each row of the table is a data element (a run or ensemble member of a simulation or experiment, for example) and each column is a property of a data element. Any column name that starts with 'FILE' is a path to a file corresponding to the data element. 

Cinema databases can be visualized through various tools (https://github.com/cinemascience). We show two options below:

To visualize the results using Jupyter Lab and Plotly, run:

    python3 -m pip install plotly

    python3 -m pip install jupyterlab

Open Jupyter Lab with 
    
    jupyter lab --browser Firefox 

and navigate to wildfire_plotly.ipynb. Run the cells to visualize the results of the DSI pipeline.

To visualize the results using pycinema (https://github.com/cinemascience/pycinema), run:

    python3 -m pip install pycinema

and 

    cinema examples/wildfire/wildfire_pycinema.py
