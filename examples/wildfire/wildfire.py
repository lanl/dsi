import os
import pandas as pd
import urllib.request 
from dsi.backends.sqlite import Sqlite, DataType
import shutil

from dsi.core import Terminal

isVerbose = True

"""
Read and download the images from the SDSC server
"""
def downloadImages(path_to_csv, imageFolder):
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
# def generateWildfireDB(dstFolder, path_to_csv, path_to_db, dbName):
#     store = Sqlite(path_to_db)
#     store.put_artifacts_csv(path_to_csv, dbName, isVerbose=isVerbose)
#     store.close()   

"""
Update the urls in the db to the local paths
"""
# def updateDBImagePaths(path_to_db, dbName, imageFolderName):
#     store = Sqlite(path_to_db)
#     data_type = DataType()
#     data_type.name = dbName

#     query = "SELECT FILE FROM " + str(data_type.name) + ";"
#     result = store.sqlquery(query)

#     for name in result:
#         filename = name[0].rsplit('/', 1)[1]
#         filePath = imageFolderName + filename
#         updateQuery = "UPDATE " + str(data_type.name) + " SET FILE = '" + filePath  + "' WHERE FILE = '" + name[0] + "';"
#         store.sqlquery(updateQuery)    

#     store.close()

"""
Query the db to identify columns of interest
"""
# def extractDBColumns(path_to_db, columns_to_keep, dbName, path_to_csv_output):
#     store = Sqlite(path_to_db)
#     data_type = DataType()
#     data_type.name = dbName

#     # for name in result:
#     #     filename = name[0].rsplit('/', 1)[1]
#     #     filePath = "images/" + filename
#     #     updateQuery = "UPDATE " + str(data_type.name) + " SET FILE = '" + filePath  + "' WHERE FILE = '" + name[0] + "';"
#     #     store.sqlquery(updateQuery)

#     #get column names
#     query = "SELECT name FROM PRAGMA_TABLE_INFO('" + str(data_type.name) + "')"
#     result = store.sqlquery(query)
#     print(result)
#     columnNames = list(map(result.__getitem__, columns_to_keep))
#     names = ""
#     for name in columnNames:
#         name = name[0]
#         names += name + ","
#     names = names[:-1]

#     # query the columns of interest
#     query = "SELECT " + names + " FROM " + str(data_type.name) + ";"
#     #query = "SELECT * FROM " + str(data_type.name) + " WHERE wind_speed > 5;"
#     #result = store.sqlquery(query) + ";"
#     store.export_csv_query(query, path_to_csv_output)
#     store.close()

if __name__ == "__main__":
    # predefined paths
    dstFolder = ""
    imageFolderName = "images/"
    imgDstFolder = dstFolder + imageFolderName
    path_to_csv_input = dstFolder + "wildfiredataSmall.csv"
    path_to_sqlite_db = dstFolder + 'wildfire.db'
    path_to_cinema_db = dstFolder + "wildfire.cdb/"
    path_to_cinema_images = path_to_cinema_db + imageFolderName
    path_to_cinema_csv = path_to_cinema_db + "data.csv"
    dbName = "wfdata"
    columns_to_keep = [0,1,2,9,10]

    #external work from DSI
    downloadImages(path_to_csv_input, imgDstFolder)

    # moves the images to the Cinema Database folder - external to DSI
    if not os.path.exists(path_to_cinema_db):
        os.makedirs(path_to_cinema_db)
    if os.path.exists(path_to_cinema_images):
        shutil.rmtree(path_to_cinema_images)
    os.rename(imgDstFolder, path_to_cinema_images)

    core = Terminal(run_table_flag=False)
    core.load_module('plugin', "Csv", "reader", filenames = path_to_csv_input, table_name = dbName)

    # update DSI abstraction directly
    updatedFilePaths = []
    wildfire_table = core.get_current_abstraction(table_name = dbName)
    for url_image in wildfire_table['FILE']:
        image_name = url_image.rsplit('/', 1)[1]
        filePath = imageFolderName + image_name
        updatedFilePaths.append(filePath)
    wildfire_table['FILE'] = updatedFilePaths
    core.update_abstraction(dbName, wildfire_table)

    # export data with revised filepaths to CSV
    core.load_module('plugin', "Csv_Writer", "writer", filename = path_to_cinema_csv, table_name = dbName, cols_to_export = columns_to_keep)
    core.transload()

    if os.path.exists(path_to_sqlite_db):
        os.remove(path_to_sqlite_db)

    #load data to a sqlite database
    core.load_module('backend','Sqlite','back-write', filename=path_to_sqlite_db)
    core.artifact_handler(interaction_type='put')


