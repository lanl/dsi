# This is an example of a DSI framework use case with wildfire data. 

The original file, wildfire.csv, lists 1889 runs of a wildfire simulation. Each row is a unique run with input and output values and associated image url. The columns list the various parameters of interest. 

To run this example, load dsi. Then run:

    python3 wildfire.py

This will generate a wildfire.cdb folder with downloaded images from the server and a data.csv file of numerical properties of interest. 

To visualize the results, run:

    python3 -m pip install cinema

and 

    cinema wildfire_pycinema.py
