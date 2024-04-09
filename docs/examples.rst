
DSI Examples
============

PENNANT mini-app
----------------

`PENNANT`_ is an unstructured mesh physics mini-application developed at Los Alamos National Laboratory
for advanced architecture research.
It contains mesh data structures and a few
physics algorithms from radiation hydrodynamics and serves as an example of
typical memory access patterns for an HPC simulation code.

This DSI PENNANT example is used to show a common use case: create and query a set of metadata derived from an ensemble of simulation runs.  The example GitHub directory includes 10 PENNANT runs using the PENNANT *Leblanc* test problem.

In the first step, a python script is used to parse the slurm output files and create a CSV (comma separated value) file with the output metadata.

.. code-block:: unixconfig

   ./parse_slurm_output.py --testname leblanc


.. literalinclude:: ../examples/pennant/parse_slurm_output.py

A second python script,

.. code-block:: unixconfig

   ./create_and_query_dsi_db.py --testname leblanc


reads in the CSV file and creates a database:

.. code-block:: python

   """
   Creates the DSI db from the csv file
   """
   """
   This script reads in the csv file created from parse_slurm_output.py.
   Then it creates a DSI db from the csv file and performs a query.
   """

   import argparse
   import sys
   from dsi.backends.sqlite import Sqlite, DataType

   isVerbose = True

   def import_pennant_data(test_name):
      csvpath = 'pennant_' + test_name + '.csv'
      dbpath = 'pennant_' + test_name + '.db'
      store = Sqlite(dbpath)
      store.put_artifacts_csv(csvpath, "rundata", isVerbose=isVerbose)
      store.close()
      # No error implies success


Finally, the database is queried:

.. code-block:: python

   """
   Performs a sample query on the DSI db
   """
   def test_artifact_query(test_name):
      dbpath = "pennant_" + test_name + ".db"
      store = Sqlite(dbpath)
      _ = store.get_artifact_list(isVerbose=isVerbose)
      data_type = DataType()
      data_type.name = "rundata"
      query = "SELECT * FROM " + str(data_type.name) + \
        " where hydro_cycle_run_time > 0.006"
      print("Running Query", query)
      result = store.sqlquery(query)
      store.export_csv(result, "pennant_query.csv")
      store.close()

  if __name__ == "__main__":
      """ The testname argument is required """
      parser = argparse.ArgumentParser()
      parser.add_argument('--testname', help='the test name')
      args = parser.parse_args()
      test_name = args.testname
      if test_name is None:
          parser.print_help()
          sys.exit(0)

      import_pennant_data(test_name)
      test_artifact_query(test_name)

Resulting in the output of the query:

..  figure:: example-pennant-output.png
    :alt: Screenshot of computer program output.
    :class: with-shadow


    The output of the PENNANT example.



Wildfire Dataset
----------------


.. _PENNANT: https://github.com/lanl/PENNANT
