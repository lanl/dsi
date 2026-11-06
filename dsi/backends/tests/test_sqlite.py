from collections import OrderedDict

from dsi.backends.sqlite import Sqlite
import os

def test_sql_artifact():
    dbpath = "wildfire.db"
    store = Sqlite(dbpath)
    store.close()
    # No error implies success
    assert True

def test_artifact_ingest():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)
    store.close()
    # No error implies success
    assert True

def test_artifact_query():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)
    query_data = store.query_artifacts(query = "SELECT * FROM wildfire;")
    store.close()
    correct_output = [[1, 3], [2, 2], [3, 1]]
    assert query_data.values.tolist() == correct_output

def test_artifact_get_table():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)
    get_data = store.get_table("wildfire")
    query_data = store.query_artifacts(query = "SELECT * FROM wildfire;")
    store.close()
    correct_output = [[1, 3], [2, 2], [3, 1]]
    assert get_data.values.tolist() == correct_output == query_data.values.tolist()

def test_artifact_notebook():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)
    store.notebook()
    store.close()
    assert True

def test_artifact_process():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':[3,2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)
    artifact = store.process_artifacts()
    store.close()
    assert artifact == valid_middleware_datastructure

def test_find_cell():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':["f",2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)

    cell_data = store.find_cell("f")
    assert cell_data[0].t_name == "wildfire"
    assert cell_data[0].c_name == ['bar']
    assert cell_data[0].value == "f"
    assert cell_data[0].row_num == 1
    assert cell_data[0].type == 'cell'
    store.close()

def test_find_row():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':["f",2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)

    row_data = store.find_cell("f", row = True)
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[0].type == 'row'
    store.close()

def test_find_relation():
    valid_middleware_datastructure = OrderedDict({"wildfire": OrderedDict({'foo':[1,2,3],'bar':["f",2,1]})})
    dbpath = 'test_artifact.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    store = Sqlite(dbpath)
    store.ingest_artifacts(valid_middleware_datastructure)

    row_data = store.find_relation("foo", ">1")
    assert len(row_data) == 2
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [2,'2']
    assert row_data[0].row_num == 2
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "<2")
    assert len(row_data) == 1
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", ">=2")
    assert len(row_data) == 2
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [2,'2']
    assert row_data[0].row_num == 2
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "<=1")
    assert len(row_data) == 1
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "=3")
    assert len(row_data) == 1
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [3,'1']
    assert row_data[0].row_num == 3
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "==3")
    assert len(row_data) == 1
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [3,'1']
    assert row_data[0].row_num == 3
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "!=2")
    assert len(row_data) == 2
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[1].value == [3,'1']
    assert row_data[1].row_num == 3
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "(1,2)")
    assert len(row_data) == 2
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[1].value == [2,'2']
    assert row_data[1].row_num == 2
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("foo", "~ '1'")
    assert len(row_data) == 1
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[0].type == 'relation'

    row_data = store.find_relation("bar", "~~ 'f'")
    assert len(row_data) == 1
    assert row_data[0].t_name == "wildfire"
    assert row_data[0].c_name == ['foo', 'bar']
    assert row_data[0].value == [1,"f"]
    assert row_data[0].row_num == 1
    assert row_data[0].type == 'relation'

    store.close()