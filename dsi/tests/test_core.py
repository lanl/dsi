from dsi.core import Terminal
import os
from collections import OrderedDict
import io
from contextlib import redirect_stdout
import textwrap
import pandas as pd
import pytest

def test_terminal_module_getter():
    a = Terminal()
    plugins = a.list_available_modules('plugin')
    backends = a.list_available_modules('backend')
    assert len(plugins) > 0 and len(backends) > 0


def test_unload_module():
    a = Terminal()
    a.load_module('plugin', 'GitInfo', 'writer')
    assert len(a.list_loaded_modules()['writer']) == 1
    a.unload_module('plugin', 'GitInfo', 'writer')
    assert len(a.list_loaded_modules()['writer']) == 0

# SQLITE TESTS
def test_ingest_sqlite_backend():
    a = Terminal()
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])
    assert len(a.active_metadata) > 0

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','Sqlite','back-write', filename=dbpath)

    a.artifact_handler(interaction_type='ingest')
    assert os.path.getsize(dbpath) > 100
    a.close()

def test_process_sqlite_backend():
    test_ingest_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")

    assert len(a.active_metadata.keys()) == 4 # 4 tables - math, address, physics, dsi_units
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        assert all(len(colData) == numRows for colData in tableData.values())
    a.close()

def test_table_info_sqlite_backend():
    test_ingest_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.list()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: math
      - num of columns: 7
      - num of rows: 2

    Table: address
      - num of columns: 9
      - num of rows: 2

    Table: physics
      - num of columns: 7
      - num of rows: 2

    Table: dsi_units
      - num of columns: 3
      - num of rows: 5

    """)
    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.num_tables()
    num_t = num_f.getvalue()
    expected_num = "Database now has 4 tables\n"

    assert num_t == expected_num
    a.close()

def test_overwrite_sqlite_backend():
    test_ingest_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-write', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['n'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["n"] == [2000, 2000]
    a.close()

def test_ingest_schema_sqlite_backend():
    a = Terminal()
    a.load_module('plugin', 'Schema', 'reader', filename="examples/test/yaml1_schema.json")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])
    assert len(a.active_metadata) > 0

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','Sqlite','back-write', filename=dbpath)

    a.artifact_handler(interaction_type='ingest')
    assert os.path.getsize(dbpath) > 100
    a.close()

def test_process_schema_sqlite_backend():
    test_ingest_schema_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")

    assert len(a.active_metadata.keys()) == 5 # 4 tables - math, address, physics, dsi_units, dsi_relations
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        if name == "dsi_relations":
            assert "primary_key" in tableData.keys()
            assert "foreign_key" in tableData.keys()
            continue
        assert all(len(colData) == numRows for colData in tableData.values())
    a.close()

def test_overwrite_schema_sqlite_backend():
    test_ingest_schema_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-write', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['p'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["p"] == [2000, 2000]
    a.close()

def test_ingest_schema_run_table_sqlite_backend():
    a = Terminal(runTable=True)
    a.load_module('plugin', 'Schema', 'reader', filename="examples/test/yaml1_schema.json")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])
    assert len(a.active_metadata) > 0

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','Sqlite','back-write', filename=dbpath)

    a.artifact_handler(interaction_type='ingest')
    assert os.path.getsize(dbpath) > 100
    a.close()

def test_process_schema_run_table_sqlite_backend():
    test_ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")
    assert a.runTable

    assert len(a.active_metadata.keys()) == 6 # 4 tables - math, address, physics, dsi_units, dsi_relations, runTable
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        if name == "dsi_relations":
            assert "primary_key" in tableData.keys()
            assert "foreign_key" in tableData.keys()
            continue
        if name == "runTable":
            assert all(len(colData) == 1 for colData in tableData.values())
            continue
        assert all(len(colData) == numRows for colData in tableData.values())
    a.close()

def test_query_schema_run_table_sqlite_backend():
    test_ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM runTable;")
    assert query_data.columns.tolist() == ["run_id", "run_timestamp"]
    assert query_data["run_id"].tolist() == [1]
    a.close()

def test_table_info_run_table_sqlite_backend():
    test_ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.list()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: runTable
      - num of columns: 2
      - num of rows: 1

    Table: math
      - num of columns: 8
      - num of rows: 2

    Table: address
      - num of columns: 10
      - num of rows: 2

    Table: physics
      - num of columns: 8
      - num of rows: 2

    Table: dsi_units
      - num of columns: 3
      - num of rows: 5
    
    """)
    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.num_tables()
    num_t = num_f.getvalue()
    expected_num = "Database now has 5 tables\n"

    assert num_t == expected_num
    a.close()

def test_summary_run_table_sqlite_backend():
    test_ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.summary()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: runTable

    column        | type    | unique | min  | max  | avg  | std_dev
    ---------------------------------------------------------------
    run_id*       | INTEGER | 1      | 1    | 1    | 1.0  | 0      
    run_timestamp | TEXT    | 1      | None | None | None | None   

    Table: math

    column         | type    | unique | min    | max    | avg    | std_dev              
    ------------------------------------------------------------------------------------
    run_id         | INTEGER | 1      | 1      | 1      | 1.0    | 0.0                  
    specification* | VARCHAR | 2      | None   | None   | None   | None                 
    a              | INTEGER | 2      | 1      | 2      | 1.5    | 0.5                  
    b              | INTEGER | 2      | 2      | 3      | 2.5    | 0.5                  
    c              | FLOAT   | 1      | 45.98  | 45.98  | 45.98  | 0.0                  
    d              | INTEGER | 2      | 2      | 3      | 2.5    | 0.5                  
    e              | FLOAT   | 2      | 34.8   | 44.8   | 39.8   | 5.0                  
    f              | FLOAT   | 2      | 0.0089 | 0.0099 | 0.0094 | 0.0005000000000000004

    Table: address

    column        | type    | unique | min  | max  | avg   | std_dev
    ----------------------------------------------------------------
    run_id        | INTEGER | 1      | 1    | 1    | 1.0   | 0.0    
    specification | VARCHAR | 2      | None | None | None  | None   
    fileLoc       | VARCHAR | 1      | None | None | None  | None   
    g             | VARCHAR | 1      | None | None | None  | None   
    h             | FLOAT   | 2      | 9.8  | 91.8 | 50.8  | 41.0   
    i*            | INTEGER | 2      | 2    | 3    | 2.5   | 0.5    
    j             | INTEGER | 2      | 3    | 4    | 3.5   | 0.5    
    k             | INTEGER | 2      | 4    | 5    | 4.5   | 0.5    
    l             | FLOAT   | 2      | 1.0  | 11.0 | 6.0   | 5.0    
    m             | INTEGER | 2      | 99   | 999  | 549.0 | 450.0  

    Table: physics

    column        | type    | unique | min     | max     | avg     | std_dev              
    --------------------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1       | 1       | 1.0     | 0.0                  
    specification | VARCHAR | 2      | None    | None    | None    | None                 
    n*            | FLOAT   | 2      | 9.8     | 91.8    | 50.8    | 41.0                 
    o             | VARCHAR | 1      | None    | None    | None    | None                 
    p             | INTEGER | 2      | 23      | 233     | 128.0   | 105.0                
    q             | VARCHAR | 1      | None    | None    | None    | None                 
    r             | INTEGER | 2      | 1       | 12      | 6.5     | 5.5                  
    s             | FLOAT   | 2      | -0.0122 | -0.0012 | -0.0067 | 0.0055000000000000005

    Table: dsi_units

    column      | type | unique | min  | max  | avg  | std_dev
    ----------------------------------------------------------
    table_name  | TEXT | 3      | None | None | None | None   
    column_name | TEXT | 5      | None | None | None | None   
    unit        | TEXT | 4      | None | None | None | None   
    """)
    assert output == expected_output

    name_f = io.StringIO()
    with redirect_stdout(name_f):
        a.summary(table_name='physics')
    name_output = name_f.getvalue()

    name_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | unique | min     | max     | avg     | std_dev              
    --------------------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1       | 1       | 1.0     | 0.0                  
    specification | VARCHAR | 2      | None    | None    | None    | None                 
    n*            | FLOAT   | 2      | 9.8     | 91.8    | 50.8    | 41.0                 
    o             | VARCHAR | 1      | None    | None    | None    | None                 
    p             | INTEGER | 2      | 23      | 233     | 128.0   | 105.0                
    q             | VARCHAR | 1      | None    | None    | None    | None                 
    r             | INTEGER | 2      | 1       | 12      | 6.5     | 5.5                  
    s             | FLOAT   | 2      | -0.0122 | -0.0012 | -0.0067 | 0.0055000000000000005
    """)
    assert name_output == name_expected_output

    name_rows_f = io.StringIO()
    with redirect_stdout(name_rows_f):
        a.summary(table_name='physics')
    name_rows_output = name_rows_f.getvalue()

    name_rows_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | unique | min     | max     | avg     | std_dev              
    --------------------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1       | 1       | 1.0     | 0.0                  
    specification | VARCHAR | 2      | None    | None    | None    | None                 
    n*            | FLOAT   | 2      | 9.8     | 91.8    | 50.8    | 41.0                 
    o             | VARCHAR | 1      | None    | None    | None    | None                 
    p             | INTEGER | 2      | 23      | 233     | 128.0   | 105.0                
    q             | VARCHAR | 1      | None    | None    | None    | None                 
    r             | INTEGER | 2      | 1       | 12      | 6.5     | 5.5                  
    s             | FLOAT   | 2      | -0.0122 | -0.0012 | -0.0067 | 0.0055000000000000005
    """)
    assert name_rows_output == name_rows_expected_output
    a.close()

def test_display_run_table_sqlite_backend():
    test_ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.display("physics")
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: physics

    run_id | specification | n    | o       | p   | q       | r  | s      
    ----------------------------------------------------------------------
    1      | !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
    1      | !amy1         | 91.8 | gravity | 233 | home 23 | 12 | -0.0122

    """)

    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.display("physics", num_rows=1)
    num_output = num_f.getvalue()

    num_expected_output = textwrap.dedent("""
    Table: physics

    run_id | specification | n   | o       | p  | q       | r | s      
    -------------------------------------------------------------------
    1      | !amy          | 9.8 | gravity | 23 | home 23 | 1 | -0.0012
      ... showing 1 of 2 rows
    
    """)
    assert num_output == num_expected_output

    num_display_f = io.StringIO()
    with redirect_stdout(num_display_f):
        a.display("physics", num_rows=1, display_cols=["run_id", "n", "p", "s"])
    num_display_output = num_display_f.getvalue()

    num_display_expected_output = textwrap.dedent("""
    Table: physics

    run_id | n   | p  | s      
    ---------------------------
    1      | 9.8 | 23 | -0.0012
      ... showing 1 of 2 rows
    
    """)
    assert num_display_output == num_display_expected_output
    a.close()

def test_get_table_sqlite_backend():
    test_ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)

    run_query = a.artifact_handler(interaction_type="query", query = "SELECT * FROM runTable;")
    run_get = a.get_table(table_name="runTable")
    assert run_query.equals(run_get)

    physics_query = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    physics_get = a.get_table(table_name="physics")
    assert physics_query.equals(physics_get)
    a.close()

# DUCKDB
def test_ingest_duckdb_backend():
    a = Terminal()
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])
    assert len(a.active_metadata) > 0

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','DuckDB','back-write', filename=dbpath)

    a.artifact_handler(interaction_type='ingest')
    assert os.path.getsize(dbpath) > 100
    a.close()

def test_process_duckdb_backend():
    test_ingest_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")

    assert len(a.active_metadata.keys()) == 4 # 4 tables - math, address, physics, dsi_units
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        assert all(len(colData) == numRows for colData in tableData.values())
    a.close()

def test_table_info_duckdb_backend():
    test_ingest_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.list()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: address
      - num of columns: 9
      - num of rows: 2
                                      
    Table: dsi_units
      - num of columns: 3
      - num of rows: 5
    
    Table: math
      - num of columns: 7
      - num of rows: 2

    Table: physics
      - num of columns: 7
      - num of rows: 2

    """)
    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.num_tables()
    num_t = num_f.getvalue()
    expected_num = "Database now has 4 tables\n"

    assert num_t == expected_num
    a.close()

def test_overwrite_duckdb_backend():
    test_ingest_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-write', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['n'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["n"] == [2000, 2000]
    a.close()

def test_ingest_schema_duckdb_backend():
    a = Terminal()
    a.load_module('plugin', 'Schema', 'reader', filename="examples/test/yaml1_schema.json")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])
    assert len(a.active_metadata) > 0

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','DuckDB','back-write', filename=dbpath)

    a.artifact_handler(interaction_type='ingest')
    assert os.path.getsize(dbpath) > 100
    a.close()

def test_process_schema_duckdb_backend():
    test_ingest_schema_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")

    assert len(a.active_metadata.keys()) == 5 # 4 tables - math, address, physics, dsi_units, dsi_relations
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        if name == "dsi_relations":
            assert "primary_key" in tableData.keys()
            assert "foreign_key" in tableData.keys()
            continue
        assert all(len(colData) == numRows for colData in tableData.values())
    a.close()

def test_overwrite_schema_duckdb_backend():
    a = Terminal()
    a.load_module('plugin', 'Schema', 'reader', filename="examples/test/yaml1_schema.json")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','DuckDB','back-write', filename=dbpath)
    a.artifact_handler(interaction_type="ingest")

    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['p'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["p"] == [2000, 2000]
    a.close()

def test_ingest_schema_run_table_duckdb_backend():
    a = Terminal(runTable=True)
    a.load_module('plugin', 'Schema', 'reader', filename="examples/test/yaml1_schema.json")
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"])
    assert len(a.active_metadata) > 0

    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)
    a.load_module('backend','DuckDB','back-write', filename=dbpath)

    a.artifact_handler(interaction_type='ingest')
    assert os.path.getsize(dbpath) > 100
    a.close()

def test_process_schema_run_table_duckdb_backend():
    test_ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")
    assert a.runTable

    assert len(a.active_metadata.keys()) == 6 # 4 tables - math, address, physics, dsi_units, dsi_relations, runTable
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        if name == "dsi_relations":
            assert "primary_key" in tableData.keys()
            assert "foreign_key" in tableData.keys()
            continue
        if name == "runTable":
            assert all(len(colData) == 1 for colData in tableData.values())
            continue
        assert all(len(colData) == numRows for colData in tableData.values())
    a.close()

def test_query_schema_run_table_duckdb_backend():
    test_ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM runTable;")
    assert query_data.columns.tolist() == ["run_id", "run_timestamp"]
    assert query_data["run_id"].tolist() == [1]
    a.close()

def test_table_info_run_table_duckdb_backend():
    test_ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.list()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: address
      - num of columns: 10
      - num of rows: 2

    Table: dsi_units
      - num of columns: 3
      - num of rows: 5

    Table: math
      - num of columns: 8
      - num of rows: 2

    Table: physics
      - num of columns: 8
      - num of rows: 2

    Table: runTable
      - num of columns: 2
      - num of rows: 1
    
    """)
    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.num_tables()
    num_t = num_f.getvalue()
    expected_num = "Database now has 5 tables\n"

    assert num_t == expected_num
    a.close()

def test_summary_run_table_duckdb_backend():
    test_ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.summary()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: address

    column        | type    | unique | min  | max  | avg   | std_dev           
    ---------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1    | 1    | 1.0   | 0.0               
    specification | VARCHAR | 2      | None | None | None  | None              
    fileLoc       | VARCHAR | 1      | None | None | None  | None              
    g             | VARCHAR | 1      | None | None | None  | None              
    h             | DOUBLE  | 2      | 9.8  | 91.8 | 50.8  | 57.982756057296896
    i*            | INTEGER | 2      | 2    | 3    | 2.5   | 0.7071067811865476
    j             | INTEGER | 2      | 3    | 4    | 3.5   | 0.7071067811865476
    k             | INTEGER | 2      | 4    | 5    | 4.5   | 0.7071067811865476
    l             | DOUBLE  | 2      | 1.0  | 11.0 | 6.0   | 7.0710678118654755
    m             | INTEGER | 2      | 99   | 999  | 549.0 | 636.3961030678928 

    Table: dsi_units

    column      | type    | unique | min  | max  | avg  | std_dev
    -------------------------------------------------------------
    table_name  | VARCHAR | 3      | None | None | None | None   
    column_name | VARCHAR | 5      | None | None | None | None   
    unit        | VARCHAR | 4      | None | None | None | None   

    Table: math

    column         | type    | unique | min    | max    | avg    | std_dev              
    ------------------------------------------------------------------------------------
    run_id         | INTEGER | 1      | 1      | 1      | 1.0    | 0.0                  
    specification* | VARCHAR | 2      | None   | None   | None   | None                 
    a              | INTEGER | 2      | 1      | 2      | 1.5    | 0.7071067811865476   
    b              | INTEGER | 2      | 2      | 3      | 2.5    | 0.7071067811865476   
    c              | DOUBLE  | 1      | 45.98  | 45.98  | 45.98  | 0.0                  
    d              | INTEGER | 2      | 2      | 3      | 2.5    | 0.7071067811865476   
    e              | DOUBLE  | 2      | 34.8   | 44.8   | 39.8   | 7.0710678118654755   
    f              | DOUBLE  | 2      | 0.0089 | 0.0099 | 0.0094 | 0.0007071067811865482

    Table: physics

    column        | type    | unique | min     | max     | avg     | std_dev             
    -------------------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1       | 1       | 1.0     | 0.0                 
    specification | VARCHAR | 2      | None    | None    | None    | None                
    n*            | DOUBLE  | 2      | 9.8     | 91.8    | 50.8    | 57.982756057296896  
    o             | VARCHAR | 1      | None    | None    | None    | None                
    p             | INTEGER | 2      | 23      | 233     | 128.0   | 148.49242404917499  
    q             | VARCHAR | 1      | None    | None    | None    | None                
    r             | INTEGER | 2      | 1       | 12      | 6.5     | 7.7781745930520225  
    s             | DOUBLE  | 2      | -0.0122 | -0.0012 | -0.0067 | 0.007778174593052024

    Table: runTable

    column        | type    | unique | min  | max  | avg  | std_dev
    ---------------------------------------------------------------
    run_id*       | INTEGER | 1      | 1    | 1    | 1.0  | 0      
    run_timestamp | VARCHAR | 1      | None | None | None | None   
    """)
    assert output == expected_output

    name_f = io.StringIO()
    with redirect_stdout(name_f):
        a.summary(table_name='physics')
    name_output = name_f.getvalue()

    name_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | unique | min     | max     | avg     | std_dev             
    -------------------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1       | 1       | 1.0     | 0.0                 
    specification | VARCHAR | 2      | None    | None    | None    | None                
    n*            | DOUBLE  | 2      | 9.8     | 91.8    | 50.8    | 57.982756057296896  
    o             | VARCHAR | 1      | None    | None    | None    | None                
    p             | INTEGER | 2      | 23      | 233     | 128.0   | 148.49242404917499  
    q             | VARCHAR | 1      | None    | None    | None    | None                
    r             | INTEGER | 2      | 1       | 12      | 6.5     | 7.7781745930520225  
    s             | DOUBLE  | 2      | -0.0122 | -0.0012 | -0.0067 | 0.007778174593052024
    """)
    assert name_output == name_expected_output

    name_rows_f = io.StringIO()
    with redirect_stdout(name_rows_f):
        a.summary(table_name='physics')
    name_rows_output = name_rows_f.getvalue()

    name_rows_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | unique | min     | max     | avg     | std_dev             
    -------------------------------------------------------------------------------------
    run_id        | INTEGER | 1      | 1       | 1       | 1.0     | 0.0                 
    specification | VARCHAR | 2      | None    | None    | None    | None                
    n*            | DOUBLE  | 2      | 9.8     | 91.8    | 50.8    | 57.982756057296896  
    o             | VARCHAR | 1      | None    | None    | None    | None                
    p             | INTEGER | 2      | 23      | 233     | 128.0   | 148.49242404917499  
    q             | VARCHAR | 1      | None    | None    | None    | None                
    r             | INTEGER | 2      | 1       | 12      | 6.5     | 7.7781745930520225  
    s             | DOUBLE  | 2      | -0.0122 | -0.0012 | -0.0067 | 0.007778174593052024
    """)
    assert name_rows_output == name_rows_expected_output
    a.close()

def test_display_run_table_duckdb_backend():
    test_ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.display("physics")
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: physics

    run_id | specification | n    | o       | p   | q       | r  | s      
    ----------------------------------------------------------------------
    1      | !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
    1      | !amy1         | 91.8 | gravity | 233 | home 23 | 12 | -0.0122
    
    """)
    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.display("physics", num_rows=1)
    num_output = num_f.getvalue()

    num_expected_output = textwrap.dedent("""
    Table: physics

    run_id | specification | n   | o       | p  | q       | r | s      
    -------------------------------------------------------------------
    1      | !amy          | 9.8 | gravity | 23 | home 23 | 1 | -0.0012
      ... showing 1 of 2 rows

    """)
    assert num_output == num_expected_output

    num_display_f = io.StringIO()
    with redirect_stdout(num_display_f):
        a.display("physics", num_rows=1, display_cols=["run_id", "n", "p", "s"])
    num_display_output = num_display_f.getvalue()

    num_display_expected_output = textwrap.dedent("""
    Table: physics

    run_id | n   | p  | s      
    ---------------------------
    1      | 9.8 | 23 | -0.0012
      ... showing 1 of 2 rows
    
    """)
    assert num_display_output == num_display_expected_output
    a.close()

def test_get_table_duckdb_backend():
    test_ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    run_query = a.artifact_handler(interaction_type="query", query = "SELECT * FROM runTable;")
    run_get = a.get_table(table_name="runTable")
    assert run_query.equals(run_get)

    physics_query = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    physics_get = a.get_table(table_name="physics")
    assert physics_query.equals(physics_get)
    a.close()

def test_test_sanitize_input():
    my_dict = OrderedDict({'"2"': OrderedDict({'specification': ['!jack'], 'a': [1], 'b': [2], 'c': [45.98], 'd': [2], 'e': [34.8], 'f': [0.0089]}), 
                    'all': OrderedDict({'specification': ['!sam'], 'fileLoc': ['/home/sam/lib/data'], 'G': ['good memories'], 
                                        'all': [9.8], 'i': [2], 'j': [3], 'k': [4], 'l': [1.0], 'm': [99]}), 
                    'physics': OrderedDict({'specification': ['!amy', '!amy1'], 'n': [9.8, 91.8], 'o': ['gravity', 'gravity'], 
                                            'p': [23, 233], 'q': ['home 23', 'home 23'], 'r': [1, 12], 's': [-0.0012, -0.0122]}), 
                    '"math"': OrderedDict({'specification': [None, '!jack1'], 'a': [None, 2], 'b': [None, 3], 'c': [None, 45.98], 
                                         'd': [None, 3], 'e': [None, 44.8], 'f': [None, 0.0099]}), 
                    'address': OrderedDict({'specification': [None, '!sam1'], 'fileLoc': [None, '/home/sam/lib/data'], 'g': [None, 'good memories'], 
                                            'h': [None, 91.8], 'i': [None, 3], 'j': [None, 4], 'k': [None, 5], 'l': [None, 11.0], 'm': [None, 999]})})
    
    a = Terminal()
    a.load_module('plugin', 'Dictionary', 'reader', collection=my_dict)
    
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    a.load_module('backend','Sqlite','back-write', filename='data.db')
    a.artifact_handler(interaction_type='ingest')

    f = io.StringIO()
    with redirect_stdout(f):
        a.display('2')
    output = f.getvalue()

    expected_output = "\nTable: 2" + textwrap.dedent("""

    specification | a | b | c     | d | e    | f     
    -------------------------------------------------
    !jack         | 1 | 2 | 45.98 | 2 | 34.8 | 0.0089
    
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        a.display('"2"')
    output = f.getvalue()

    expected_output = '\nTable: "2"' + textwrap.dedent("""

    specification | a | b | c     | d | e    | f     
    -------------------------------------------------
    !jack         | 1 | 2 | 45.98 | 2 | 34.8 | 0.0089
    
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        a.display('all')
    output = f.getvalue()

    expected_output = '\nTable: all' + textwrap.dedent("""

    specification | fileLoc            | G             | all | i | j | k | l   | m 
    -------------------------------------------------------------------------------
    !sam          | /home/sam/lib/data | good memories | 9.8 | 2 | 3 | 4 | 1.0 | 99
    
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        a.display('"math"')
    output = f.getvalue()

    expected_output = '\nTable: "math"' + textwrap.dedent("""

    specification | a    | b    | c     | d    | e    | f     
    ----------------------------------------------------------
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 3.0  | 45.98 | 3.0  | 44.8 | 0.0099
    
    """)
    assert output == expected_output

    a.artifact_handler(interaction_type="process")
    a.artifact_handler(interaction_type='ingest')

    assert 'math' in a.active_metadata.keys()
    assert '"math"' not in a.active_metadata.keys()
    assert '"all"' in a.active_metadata.keys()
    assert 'all' not in a.active_metadata.keys()
    assert '"2"' in a.active_metadata.keys()
    assert '2' not in a.active_metadata.keys()

    f = io.StringIO()
    with redirect_stdout(f):
        a.display('"math"')
    output = f.getvalue()

    expected_output = '\nTable: "math"' + textwrap.dedent("""

    specification | a    | b    | c     | d    | e    | f     
    ----------------------------------------------------------
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 3.0  | 45.98 | 3.0  | 44.8 | 0.0099
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 3.0  | 45.98 | 3.0  | 44.8 | 0.0099
        
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        a.display('math')
    output = f.getvalue()

    expected_output = '\nTable: math' + textwrap.dedent("""

    specification | a    | b    | c     | d    | e    | f     
    ----------------------------------------------------------
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 3.0  | 45.98 | 3.0  | 44.8 | 0.0099
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 3.0  | 45.98 | 3.0  | 44.8 | 0.0099
        
    """)
    assert output == expected_output
    
# =============================================================================
# NDP BACKEND TESTS
# =============================================================================

NDP_BASE_PARAMS = {"keywords": "climate", "limit": 20}
NDP_FILTER_PARAMS = [
    {"organization": "USGS", "limit": 5},
    {
        "keywords": "temperature",
        "tags": ["climate"],
        "formats": ["CSV", "JSON"],
        "limit": 5,
    },
]


def _load_ndp_terminal(params):
    """Create a Terminal and load one NDP backend."""
    terminal = Terminal()
    try:
        terminal.load_module(
            "backend",
            "NDP",
            "back-read",
            params=params,
        )
    except Exception:
        terminal.close()
        raise
    return terminal


@pytest.fixture(scope="module")
def ndp_terminal():
    """
    Share one loaded and processed NDP Terminal across non-destructive tests.

    Terminal is not a context manager, so the yield fixture owns cleanup.
    The broad query is intentionally large enough for the find/relation tests.
    """
    terminal = _load_ndp_terminal(NDP_BASE_PARAMS)
    backend = terminal.active_modules["back-read"][0]

    try:
        # Processing copies the already-loaded backend cache into active_metadata;
        # it does not need another NDP server query.
        terminal.artifact_handler(interaction_type="process")
        yield terminal
    finally:
        terminal.close()
        # These teardown assertions retain coverage formerly provided by the
        # dedicated close test without another remote NDP instantiation.
        assert backend._loaded is False
        assert len(backend._cache) == 0


@pytest.fixture(scope="module")
def ndp_filtered_terminal():
    """
    Share one multi-query Terminal for filter parsing and deduplication tests.

    Two query dictionaries cover organization, tags, and formats while also
    exercising the multi-query path. NDP may make one request per dictionary.
    """
    terminal = _load_ndp_terminal(NDP_FILTER_PARAMS)
    try:
        terminal.artifact_handler(interaction_type="process")
        yield terminal
    finally:
        terminal.close()


@pytest.fixture
def ndp_unload_terminal():
    """Provide an isolated Terminal because unload mutates shared state."""
    terminal = _load_ndp_terminal({"keywords": "climate", "limit": 1})
    try:
        yield terminal
    finally:
        # Safe whether the test already unloaded the backend or failed earlier.
        terminal.close()


def test_terminal_load_ndp(ndp_terminal):
    """Test Terminal can load and initialize an NDP backend."""
    backend = ndp_terminal.active_modules["back-read"][0]

    assert backend is not None
    assert backend._loaded is True
    assert len(backend._cache) > 0


def test_terminal_unload_ndp(ndp_unload_terminal):
    """Test unloading NDP closes it and removes it from active modules."""
    backend = ndp_unload_terminal.active_modules["back-read"][0]

    ndp_unload_terminal.unload_module("backend", "NDP", "back-read")

    assert len(ndp_unload_terminal.active_modules["back-read"]) == 0
    assert backend._loaded is False
    assert len(backend._cache) == 0


def test_terminal_load_ndp_multiple_queries(ndp_filtered_terminal):
    """Test NDP multi-query results are deduplicated."""
    backend = ndp_filtered_terminal.active_modules["back-read"][0]
    assert backend._loaded is True

    datasets = ndp_filtered_terminal.active_metadata.get("datasets", {})
    if datasets and "id" in datasets:
        ids = datasets["id"]
        assert len(ids) == len(set(ids))


def test_terminal_process_ndp(ndp_terminal):
    """Test Terminal processed artifacts from the shared NDP backend."""
    assert len(ndp_terminal.active_metadata) > 0
    assert "datasets" in ndp_terminal.active_metadata
    assert isinstance(ndp_terminal.active_metadata["datasets"], OrderedDict)


def test_terminal_query_not_supported(ndp_terminal):
    """Test that SQL queries and write operations fail with NDP."""
    with pytest.raises(NotImplementedError):
        ndp_terminal.artifact_handler(
            interaction_type="query",
            query="SELECT * FROM datasets",
        )

    with pytest.raises(NotImplementedError):
        ndp_terminal.artifact_handler(interaction_type="ingest")


def test_terminal_get_table_ndp(ndp_terminal):
    """Test Terminal.get_table() with the shared NDP backend."""
    df = ndp_terminal.get_table("datasets", dict_return=False)
    assert isinstance(df, pd.DataFrame)

    dict_data = ndp_terminal.get_table("datasets", dict_return=True)
    assert isinstance(dict_data, OrderedDict)

# TO DO: Possible problematic malformed object on return summary(), May need refactor for CLI
def test_terminal_list_and_summary_ndp(ndp_terminal):
    """Test Terminal list, num_tables, and summary methods with NDP."""
    # Check if backend actually loaded data
    backend = ndp_terminal.active_modules["back-read"][0]
    if not backend._loaded or len(backend._cache) == 0:
        pytest.skip("NDP backend did not load data (network/API issue)")
    
    table_list = ndp_terminal.list(collection=True)
    assert "datasets" in table_list

    f = io.StringIO()
    with redirect_stdout(f):
        ndp_terminal.num_tables()
    assert "tables" in f.getvalue().lower()

    # Test summary with collection=True to get return value
    summary_list = ndp_terminal.summary(collection=True)
    assert isinstance(summary_list, list)
    assert len(summary_list) > 0
    # All elements should be DataFrames (table_names are stripped when collection=True)
    assert all(isinstance(df, pd.DataFrame) for df in summary_list)

    # Test summary with specific table name and collection=True
    summary_df = ndp_terminal.summary(table_name="datasets", collection=True)
    assert isinstance(summary_df, pd.DataFrame)


def test_terminal_display_and_schema_ndp(ndp_terminal):
    """Test Terminal display and schema methods with NDP."""
    f = io.StringIO()
    with redirect_stdout(f):
        ndp_terminal.display("datasets", num_rows=5)
    assert "datasets" in f.getvalue()

    schema = ndp_terminal.get_schema()
    assert isinstance(schema, str)
    assert "CREATE TABLE" in schema


def test_terminal_find_methods_ndp(ndp_terminal):
    """Test Terminal find methods with NDP."""
    assert isinstance(ndp_terminal.find("climate"), list)

    tables = ndp_terminal.find_table("datasets")
    assert isinstance(tables, list)
    assert len(tables) > 0

    assert isinstance(ndp_terminal.find_column("title"), list)
    assert isinstance(ndp_terminal.find_cell("climate"), list)


@pytest.mark.parametrize(
    "relation",
    [
        "num_resources > 0",
        "num_resources == 0",
        "num_resources (0, 5)",
    ],
)
def test_terminal_find_relation_ndp(ndp_terminal, relation):
    """Test Terminal.find_relation() operators without reloading NDP."""
    assert isinstance(ndp_terminal.find_relation(relation), list)


def test_terminal_ndp_filters(ndp_filtered_terminal):
    """Test a shared multi-query load accepts NDP filter combinations."""
    backend = ndp_filtered_terminal.active_modules["back-read"][0]

    assert backend._loaded is True
    assert "datasets" in backend._cache
