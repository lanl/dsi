# PENNANT DSI example
This is an example for creating and querying a DSI database from PENNANT output.
Output from 10 runs is included in the directory. If you wish to generate your own output, you can download PENNANT: from: [https://github.com/lanl/PENNANT](https://github.com/lanl/PENNANT) and compile with make after editing the Makefile.

## Create a csv from the included outputs
Create a csv file from the outputs:
```
./parse_slurm_output.py --testname leblanc
```

## Create a DSI db and query it
```
./create_and_query_dsi_db.py --testname leblanc
```
