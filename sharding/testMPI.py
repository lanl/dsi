from mpi4py import MPI
from parallelDB import ParallelDB
from db_utils import *

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

print(f"Hello from process {rank} of {size}")

db_name = "test_par.db"
db_schema = "database/table_template.sql"

if rank == 0:
    create_sqlite_db(db_schema, db_name)
comm.Barrier()



db = ParallelDB(db_name, size)

col_cus = ['customer_id', 'name', 'email']
col_order = ['order_id', 'customer_id', 'order_date', 'amount']

count = 0
val = [rank*10 + count, 'peter Smith', 'peter@example.com']
db.database.insert_col_data("Customers",col_cus, val)

count = 1
val = [rank*10 + count, 'peter Smith', 'peter@example.com']
db.database.insert_col_data("Customers",col_cus, val)

count = 2
val = [rank*10 + count, 'peter Smith', 'peter@example.com']
db.database.insert_col_data("Customers",col_cus, val)

db.all_done()