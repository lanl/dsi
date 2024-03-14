# QUIC-Fire Wildfire Simulations

This example highlights the use of the DSI Framework with QUIC-Fire fire-atmosphere simualation data and resulting images. The original file, wildfire.csv, lists 1889 runs of a wildfire simulation. Each row is a unique run with input and output values and associated image url. The columns list the various parameters of interest. The input columns are: wild_speed, wdir (wind direction), smois (surface moisture), fuels, ignition, safe_unsafe_ignition_pattern, safe_unsafe_fire_behavior, does_fire_meet_objectives, and rationale_if_unsafe. The output of the simulation (and post-processing steps) include the burned_area and the url to the wildfire image stored on the San Diego Super Computer.

All paths in this example are defined as from the main dsi repository folder.

To run this example, load dsi. Then run:

    python3 examples/wildfire/wildfire.py

This will generate a wildfire.cdb folder with downloaded images from the server and a data.csv file of numerical properties of interest. 

To visualize the results, run:

    python3 -m pip install cinema

and 

    cinema examples/wildfire/wildfire_pycinema.py
