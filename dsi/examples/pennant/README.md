# PENNANT DSI example
This is an example for creating and querying a DSI database from PENNANT output.

## Getting and compiling PENNANT
- Download PENNANT from: [https://github.com/lanl/PENNANT](https://github.com/lanl/PENNANT).
- Compile pennant with make after editing the Makefile

## Running PENNANT
An example for running PENNANT on a cluster with slurm:

```
srun build/pennant test/leblanc/leblanc.pnt > run1.out
srun build/pennant test/leblanc/leblanc.pnt > run2.out
srun build/pennant test/leblanc/leblanc.pnt > run3.out
srun build/pennant test/leblanc/leblanc.pnt > run4.out
```

In this case, I would use "leblanc" as the test name for the following steps

## Create a csv from the outputs
After the runs completed, create a csv file from the outputs:
```
./parse_slurm_output.py --testname leblanc
```

## Create a DSI db and query it
```
./create_and_query_dsi_db.py --testname leblanc
```
