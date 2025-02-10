from parallelDB import ParallelDB
from db_utils import *

db_name = "test.db"
db_schema = "database/table_template.sql"
create_sqlite_db(db_schema,db_name)

n = 4
db0 = ParallelDB(db_name, n)
db1 = ParallelDB(db_name, n)
db2 = ParallelDB(db_name, n)
db3 = ParallelDB(db_name, n)

col_cus = ['customer_id', 'name', 'email']
col_order = ['order_id', 'customer_id', 'order_date', 'amount']

val = [3, 'peter Smith', 'peter@example.com']
db1.database.insert_col_data("Customers",col_cus, val)

val = [4, 'jake Smith', 'jake@example.com']
db0.database.insert_col_data("Customers",col_cus, val)

val = [5, 3, '2025-02-03', 120.50]
db1.database.insert_col_data("Orders",col_order, val)

val = [6, 3, '2025-02-03', 120.50]
db3.database.insert_col_data("Orders",col_order, val)

db3.all_done()
db1.all_done()
db2.all_done()
db0.all_done()