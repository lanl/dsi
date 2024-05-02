import os
import pandas as pd
import urllib.request 
from dsi.backends.sqlite import Sqlite, DataType
import shutil

isVerbose = True

"""
Read and download the images from the SDSC server
"""
def downloadImages(dstFolder, path_to_csv, imageFolder):
    df = pd.read_csv (path_to_csv)

    for index, row in df.iterrows():
        url = row['FILE'] 
        filename = url.rsplit('/', 1)[1]
        isExist = os.path.exists(imageFolder)
        if not isExist:
            os.makedirs(imageFolder)
        
        dst = imageFolder + filename
        urllib.request.urlretrieve(url, dst)

"""
Create the wildfire database from the csv file
"""
def generateWildfireDB(dstFolder, path_to_csv, path_to_db, dbName):
    store = Sqlite(path_to_db)
    store.put_artifacts_csv(path_to_csv, dbName, isVerbose=isVerbose)
    store.close()   

"""
Update the urls in the db to the local paths
"""
def updateDBImagePaths(path_to_db, dbName, imageFolderName):
    store = Sqlite(path_to_db)
    data_type = DataType()
    data_type.name = dbName

    query = "SELECT FILE FROM " + str(data_type.name) + ";"
    result = store.sqlquery(query)

    for name in result:
        filename = name[0].rsplit('/', 1)[1]
        filePath = imageFolderName + filename
        updateQuery = "UPDATE " + str(data_type.name) + " SET FILE = '" + filePath  + "' WHERE FILE = '" + name[0] + "';"
        store.sqlquery(updateQuery)    

    store.close()

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
    dstFolder = "examples/wildfire/"
    imageFolderName = "images/"
    imgDstFolder = dstFolder + imageFolderName
    path_to_csv_input = dstFolder + "wildfiredataSmall.csv"
    path_to_sqlite_db = dstFolder + 'wildfire.db'
    path_to_cinema_db = dstFolder + "wildfire.cdb/"
    path_to_cinema_images = path_to_cinema_db + imageFolderName
    path_to_cinema_csv = path_to_cinema_db + "data.csv"
    dbName = "wfdata"
    columns_to_keep = [0,1,2,9,10]

    # for each row, read the url in column 'FILE' and download the images
    downloadImages(dstFolder, path_to_csv_input, imgDstFolder)

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
    



