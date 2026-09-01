# PENNANT DSI example
This is an example for creating and querying a DSI database from PENNANT output.
Output from 10 runs is included in the directory. If you wish to generate your own output, you can download PENNANT: from: [https://github.com/lanl/PENNANT](https://github.com/lanl/PENNANT) and compile with make after editing the Makefile. Runs are provided in this folder ready to ingest.

## Move into the PENNANT directory
Assuming the DSI library is installed, move first into the root DSI directory, then:
```
cd examples/pennant/
```

## Create a csv from the included outputs
Create a csv file from the outputs:
```
python3 parse_slurm_output.py
```

## FOR USERS -- Create a DSI db and query it
```
python3 dsi_pennant.py
```

## FOR CONTRIBUTORS -- Create a DSI db and query it
```
python3 dsi_pennant_dev.py
```
