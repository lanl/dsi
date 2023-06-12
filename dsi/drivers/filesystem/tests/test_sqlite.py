import git
import os
import urllib.request
import sys

from dsi.drivers.filesystem.sqlite import SqlStore, DataType, Artifact

isVerbose=True

def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return(git_root)

def test_wildfire_data_sql_artifact():
    csvpath = '/'.join([get_git_root('.'),'dsi/data/wildfiredata.csv'])
    dbpath = "wildfire.db"
    store = SqlStore(dbpath)
    store.close()

    
def test_wildfire_data_csv_artifact():
    csvpath = '/'.join([get_git_root('.'),'dsi/data/wildfiredata.csv'])
    dbpath = "wildfire.db"
    store = SqlStore(dbpath)
    store.put_artifacts_csv(csvpath,"simulation", isVerbose=isVerbose)
    store.close()
    
def test_yosemite_data_csv_artifact():
    csvpath = '/'.join([get_git_root('.'),'dsi/data/yosemite5.csv'])
    dbpath = "yosemite.db"
    store = SqlStore(dbpath)
    store.put_artifacts_csv(csvpath,"vision", isVerbose=isVerbose)
    store.close()

def test_query():    
    #Query everything
    #result = store.sqlquery("SELECT * FROM " + str("vision"))
    #store.close()
    pass


def test_artifact_query():
    csvpath = '/'.join([get_git_root('.'),'dsi/data/wildfiredata.csv'])
    dbpath = "wildfire.db"
    store = SqlStore(dbpath)
    anames = store.get_artifact_list(isVerbose=isVerbose)
    for name in anames:
        print( name[0] )
    data_type = DataType()
    data_type.name = "simulation"
    result = store.sqlquery("SELECT *, MAX(wind_speed) AS max_windspeed FROM " + str(data_type.name) + " GROUP BY safe_unsafe_fire_behavior")
    store.export_csv(result, "query.csv")
    store.close()
    print("Done")
