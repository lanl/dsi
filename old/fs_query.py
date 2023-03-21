import os
import stat
import sqlite3


#filepath = "H:/src/cosmocompare-master/CosmoCompare/"

# Open existing db
con = sqlite3.connect('fs.db')
cur = con.cursor()

res = cur.execute('SELECT * FROM world')
print(res.fetchall())
con.close()

# query file permissions

# assert file permissions

# print results

# Connect and open + query

