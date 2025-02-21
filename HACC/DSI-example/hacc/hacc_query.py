from dsi.backends.sqlite import Sqlite, DataType
import sqlite3

if __name__ == "__main__":
    filepath = "/Users/mhan/Desktop/dsi/hacc.db"
    ts = 0
    store = Sqlite(filepath)
    query = f"SELECT filepath FROM hacc_info_{ts}"
    result = store.sqlquery(query)
    print(result[0][0])
    
    
    