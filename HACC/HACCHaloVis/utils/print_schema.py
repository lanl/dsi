import sys 
sys.path.append("./DBGenerator")
from database import Database

database = Database("./databases/hacc_halo.db")
schema = database.get_table_info('runs')
print(schema)

schema = database.get_table_info('files')
print(schema)
schema = database.get_table_info('halos')
print(schema)
