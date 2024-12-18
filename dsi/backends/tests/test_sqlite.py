import git
from collections import OrderedDict

from dsi.backends.sqlite import Sqlite, DataType
import os

isVerbose = True


# def get_git_root(path):
#     git_repo = git.Repo(path, search_parent_directories=True)
#     git_root = git_repo.git.rev_parse("--show-toplevel")
#     return (git_root)

def test_sql_artifact():
    dbpath = "wildfire.db"
    store = Sqlite(dbpath)
    store.close()
    # No error implies success
    assert True

# def test_wildfire_data_csv_artifact():
#     csvpath = '/'.join([get_git_root('.'), 'examples/data/wildfiredata.csv'])
#     dbpath = "wildfire.db"
#     store = Sqlite(dbpath)
#     store.put_artifacts_csv(csvpath, "simulation", isVerbose=isVerbose)
#     store.close()
#     # No error implies success
#     assert True

def test_artifact_put():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.put_artifacts(valid_middleware_datastructure)
    store.close()
    # No error implies success
    assert True

def test_wildfiredata_artifact_put_t():
   valid_middleware_datastructure = OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})
   dbpath = 'test_wildfiredata_artifact.db'
   store = Sqlite(dbpath)
   store.put_artifacts_t(OrderedDict([("wildfire", valid_middleware_datastructure)]), tableName="Wildfire")
   store.close()
   # No error implies success
   assert True

def test_artifact_get():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.put_artifacts(valid_middleware_datastructure)
    query_data = store.get_artifacts(query = "SELECT * FROM wildfire;")
    store.close()
    correct_output = [(1, 3), (2, 2), (3, 1)]
    assert query_data == correct_output

def test_artifact_inspect():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.put_artifacts(valid_middleware_datastructure)
    store.inspect_artifacts()
    assert True

def test_artifact_read():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.put_artifacts(valid_middleware_datastructure)
    artifact = store.read_to_artifact()
    store.close()
    assert artifact == valid_middleware_datastructure

# #Data from: https://microsoftedge.github.io/Demos/json-dummy-data/64KB.json
# def test_jsondata_artifact_put():
#    jsonpath = '/'.join([get_git_root('.'), 'dsi/data/64KB.json'])
#    dbpath = "jsondata.db"
#    store = Sqlite(dbpath)
#    store.put_artifacts_json(jsonpath, tname="JSONData")
#    store.close()
#    # No error implies success
#    assert True

# def test_yosemite_data_csv_artifact():
#     csvpath = '/'.join([get_git_root('.'), 'examples/data/yosemite5.csv'])
#     dbpath = "yosemite.db"
#     store = Sqlite(dbpath)
#     store.put_artifacts_csv(csvpath, "vision", isVerbose=isVerbose)
#     store.close()
#     # No error implies success
#     assert True