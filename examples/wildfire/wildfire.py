import os
import pandas as pd
import urllib.request 
from dsi.backends.sqlite import Sqlite, DataType

isVerbose = False

"""
Read and download the images from the SDSC server
"""
def downloadImages(dstFolder, path_to_csv):
    df = pd.read_csv (path_to_csv)

    for index, row in df.iterrows():
        url = row['FILE'] 
        filename = url.rsplit('/', 1)[1]
        imgDstFolder = dstFolder + "images/"
        isExist = os.path.exists(imgDstFolder)
        if not isExist:
            os.makedirs(imgDstFolder)
        
        dst = imgDstFolder + filename
        urllib.request.urlretrieve(url, dst)

"""
Create the wildfire database from the csv file
"""
def generateWildfireDB(dstFolder, path_to_csv, path_to_db):
    store = Sqlite(path_to_db)
    store.put_artifacts_csv(path_to_csv, "wfdata", isVerbose=isVerbose)
    store.close()    

"""
Query the db to identify columns of interest
"""
def extractDBColumns(path_to_db, columns_to_keep):
    store = Sqlite(path_to_db)
    afList = store.get_artifact_list(isVerbose=isVerbose)
    data_type = DataType()
    data_type.name = "wfdata"

    query = "SELECT FILE FROM " + str(data_type.name) + ";"
    result = store.sqlquery(query)

    for name in result:
        filename = name[0].rsplit('/', 1)[1]
        filePath = "images/" + filename
        updateQuery = "UPDATE " + str(data_type.name) + " SET FILE = '" + filePath  + "' WHERE FILE = '" + name[0] + "';"
        store.sqlquery(updateQuery)

    # get column names
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
    store.export_csv_query(query, "examples/wildfire/data.csv")
    store.close()

if __name__ == "__main__":
    dstFolder = "examples/wildfire/"
    path_to_csv = "examples/wildfire/wildfiredataSmall.csv"
    path_to_db = dstFolder + 'wildfire.db'
    downloadImages(dstFolder, path_to_csv)
    generateWildfireDB(dstFolder, path_to_csv, path_to_db)
    columns_to_keep = [0,1,2,9,10]
    extractDBColumns(path_to_db, columns_to_keep)
    isExist = os.path.exists(dstFolder + "wildfire.cdb/")
    if not isExist:
        os.makedirs(dstFolder + "wildfire.cdb/")
    os.rename(dstFolder+"images/", dstFolder + "wildfire.cdb/" + "images")
    os.rename("examples/wildfire/data.csv", "examples/wildfire/wildfire.cdb/data.csv")        

    



