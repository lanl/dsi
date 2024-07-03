import os
import pandas as pd
import urllib.request 
from dsi.backends.sqlite import Sqlite, DataType
import shutil

isVerbose = True


"""
Query the db to identify columns of interest
"""
def extractDBColumns(path_to_db, columns_to_keep, dbName, path_to_csv_output):
    store = Sqlite(path_to_db)
    data_type = DataType()
    data_type.name = dbName

    # for name in result:
    #     filename = name[0].rsplit('/', 1)[1]
    #     filePath = "images/" + filename
    #     updateQuery = "UPDATE " + str(data_type.name) + " SET FILE = '" + filePath  + "' WHERE FILE = '" + name[0] + "';"
    #     store.sqlquery(updateQuery)

    #get column names
    query = "SELECT name FROM PRAGMA_TABLE_INFO('" + str(data_type.name) + "')"
    result = store.sqlquery(query)
    print(result)
    columnNames = list(map(result.__getitem__, columns_to_keep))
    names = ""
    for name in columnNames:
        name = name[0]
        names += name + ","
    names = names[:-1]

    # query the columns of interest
    query = "SELECT " + names + " FROM " + str(data_type.name) + ";"
    #query = "SELECT * FROM " + str(data_type.name) + " WHERE wind_speed > 5;"
    #result = store.sqlquery(query) + ";"
    store.export_csv_query(query, path_to_csv_output)
    store.close()

if __name__ == "__main__":
    # predefined paths
    dstFolder = "./"
    path_to_sqlite_db = dstFolder + 'git_test.db'
    dbName = "git_data"
    columns_to_keep = [0,1,2,9,10]

    # generate the SQLite database
    dbExist = os.path.exists(path_to_sqlite_db)
    if dbExist:
        os.remove(path_to_sqlite_db)

    generateWildfireDB(dstFolder, path_to_csv_input, path_to_sqlite_db, dbName)

    # moves the images to the Cinema Database folder
    isExist = os.path.exists(path_to_cinema_db)
    if not isExist:
        os.makedirs(path_to_cinema_db)
    imgDirExist = os.path.exists(path_to_cinema_images)
    if imgDirExist:
        shutil.rmtree(path_to_cinema_images)
    os.rename(imgDstFolder, path_to_cinema_images)

    # update the paths in the database to the local paths
    updateDBImagePaths(path_to_sqlite_db, dbName, imageFolderName)

    # extract columns of interest
    extractDBColumns(path_to_sqlite_db, columns_to_keep, dbName, path_to_cinema_csv)
    



