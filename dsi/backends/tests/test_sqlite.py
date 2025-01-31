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
    store.write_artifacts(valid_middleware_datastructure)
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
    store.write_artifacts(valid_middleware_datastructure)
    query_data = store.process_artifacts(query = "SELECT * FROM wildfire;")
    store.close()
    correct_output = [(1, 3), (2, 2), (3, 1)]
    assert query_data == correct_output

def test_artifact_inspect():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.write_artifacts(valid_middleware_datastructure)
    store.inspect_artifacts()
    store.close()
    assert True

def test_artifact_read():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.write_artifacts(valid_middleware_datastructure)
    artifact = store.read_to_artifact()
    store.close()
    assert artifact == valid_middleware_datastructure

def test_find():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath, run_table=False)
    store.write_artifacts(valid_middleware_datastructure)
    table_data = store.find("wildfire")
    assert len(table_data) == 1
    assert len(table_data[0]) == 2
    assert len(table_data[0][1]) == 3
    
    col_list = store.find("wildfire", colFlag=True)
    assert len(col_list) == 1
    assert len(col_list[0]) == 2
    assert len(col_list[0][1]) == 2
    
    col_data = store.find("foo")
    assert len(col_data) == 1
    assert len(col_data[0]) == 2
    assert len(col_data[0][1]) == 3
    
    col_data = store.find("foo", data_range=True)
    assert len(col_data) == 1
    assert len(col_data[0]) == 3
    assert col_data[0][1] == "data range = (1, 3)"
    assert len(col_data[0][2]) == 3

    value_data = store.find(3)
    assert len(value_data) == 2
    assert len(value_data[0]) == 3
    store.close()