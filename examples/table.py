import sqlite3
from dsi.utils import utils
from dsi.sql.fs import fs


con = sqlite3.connect('wildfire.db')

def sql_fetch(con):

    cursorObj = con.cursor()

    cursorObj.execute('SELECT name from sqlite_master where type= "table"')

    print(cursorObj.fetchall())

sql_fetch(con)

