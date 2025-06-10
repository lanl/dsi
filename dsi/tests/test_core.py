from dsi.core import Terminal
import os
from collections import OrderedDict
import io
from contextlib import redirect_stdout
import textwrap


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
def ingest_sqlite_backend():
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

def process_sqlite_backend():
    ingest_sqlite_backend()

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

def table_info_sqlite_backend():
    ingest_sqlite_backend()

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

def overwrite_sqlite_backend():
    ingest_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-write', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['n'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["n"] == [2000, 2000]
    a.close()

def ingest_schema_sqlite_backend():
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

def process_schema_sqlite_backend():
    ingest_schema_sqlite_backend()

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

def overwrite_schema_sqlite_backend():
    ingest_schema_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-write', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['p'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["p"] == [2000, 2000]
    a.close()

def ingest_schema_run_table_sqlite_backend():
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

def process_schema_run_table_sqlite_backend():
    ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")
    assert a.runTable == True

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

def query_schema_run_table_sqlite_backend():
    ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM runTable;")
    assert query_data.columns.tolist() == ["run_id", "run_timestamp"]
    assert query_data["run_id"].tolist() == [1]
    a.close()

def table_info_run_table_sqlite_backend():
    ingest_schema_run_table_sqlite_backend()

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

def summary_run_table_sqlite_backend():
    ingest_schema_run_table_sqlite_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','Sqlite','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.summary()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: runTable

    column        | type    | min  | max  | avg  | std_dev
    ------------------------------------------------------
    run_id*       | INTEGER | 1    | 1    | 1.0  | None   
    run_timestamp | TEXT    | None | None | None | None   
      - num of rows: 1


    Table: math

    column         | type    | min    | max    | avg    | std_dev              
    ---------------------------------------------------------------------------
    run_id         | INTEGER | 1      | 1      | 1.0    | 0.0                  
    specification* | VARCHAR | None   | None   | None   | None                 
    a              | INTEGER | 1      | 2      | 1.5    | 0.5                  
    b              | INTEGER | 2      | 3      | 2.5    | 0.5                  
    c              | FLOAT   | 45.98  | 45.98  | 45.98  | 0.0                  
    d              | INTEGER | 2      | 3      | 2.5    | 0.5                  
    e              | FLOAT   | 34.8   | 44.8   | 39.8   | 5.0                  
    f              | FLOAT   | 0.0089 | 0.0099 | 0.0094 | 0.0005000000000000004
      - num of rows: 2


    Table: address

    column        | type    | min  | max  | avg   | std_dev
    -------------------------------------------------------
    run_id        | INTEGER | 1    | 1    | 1.0   | 0.0    
    specification | VARCHAR | None | None | None  | None   
    fileLoc       | VARCHAR | None | None | None  | None   
    g             | VARCHAR | None | None | None  | None   
    h             | FLOAT   | 9.8  | 91.8 | 50.8  | 41.0   
    i*            | INTEGER | 2    | 3    | 2.5   | 0.5    
    j             | INTEGER | 3    | 4    | 3.5   | 0.5    
    k             | INTEGER | 4    | 5    | 4.5   | 0.5    
    l             | FLOAT   | 1.0  | 11.0 | 6.0   | 5.0    
    m             | INTEGER | 99   | 999  | 549.0 | 450.0  
      - num of rows: 2


    Table: physics

    column        | type    | min     | max     | avg     | std_dev              
    -----------------------------------------------------------------------------
    run_id        | INTEGER | 1       | 1       | 1.0     | 0.0                  
    specification | VARCHAR | None    | None    | None    | None                 
    n*            | FLOAT   | 9.8     | 91.8    | 50.8    | 41.0                 
    o             | VARCHAR | None    | None    | None    | None                 
    p             | INTEGER | 23      | 233     | 128.0   | 105.0                
    q             | VARCHAR | None    | None    | None    | None                 
    r             | INTEGER | 1       | 12      | 6.5     | 5.5                  
    s             | FLOAT   | -0.0122 | -0.0012 | -0.0067 | 0.0055000000000000005
      - num of rows: 2


    Table: dsi_units

    column      | type | min  | max  | avg  | std_dev
    -------------------------------------------------
    table_name  | TEXT | None | None | None | None   
    column_name | TEXT | None | None | None | None   
    unit        | TEXT | None | None | None | None   
      - num of rows: 5
    
    """)
    assert output == expected_output

    name_f = io.StringIO()
    with redirect_stdout(name_f):
        a.summary(table_name='physics')
    name_output = name_f.getvalue()

    name_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | min     | max     | avg     | std_dev              
    -----------------------------------------------------------------------------
    run_id        | INTEGER | 1       | 1       | 1.0     | 0.0                  
    specification | VARCHAR | None    | None    | None    | None                 
    n*            | FLOAT   | 9.8     | 91.8    | 50.8    | 41.0                 
    o             | VARCHAR | None    | None    | None    | None                 
    p             | INTEGER | 23      | 233     | 128.0   | 105.0                
    q             | VARCHAR | None    | None    | None    | None                 
    r             | INTEGER | 1       | 12      | 6.5     | 5.5                  
    s             | FLOAT   | -0.0122 | -0.0012 | -0.0067 | 0.0055000000000000005
      - num of rows: 2
    
    """)
    assert name_output == name_expected_output

    name_rows_f = io.StringIO()
    with redirect_stdout(name_rows_f):
        a.summary(table_name='physics', num_rows = 3)
    name_rows_output = name_rows_f.getvalue()

    name_rows_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | min     | max     | avg     | std_dev              
    -----------------------------------------------------------------------------
    run_id        | INTEGER | 1       | 1       | 1.0     | 0.0                  
    specification | VARCHAR | None    | None    | None    | None                 
    n*            | FLOAT   | 9.8     | 91.8    | 50.8    | 41.0                 
    o             | VARCHAR | None    | None    | None    | None                 
    p             | INTEGER | 23      | 233     | 128.0   | 105.0                
    q             | VARCHAR | None    | None    | None    | None                 
    r             | INTEGER | 1       | 12      | 6.5     | 5.5                  
    s             | FLOAT   | -0.0122 | -0.0012 | -0.0067 | 0.0055000000000000005

    run_id | specification | n    | o       | p   | q       | r  | s      
    ----------------------------------------------------------------------
    1      | !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
    1      | !amy1         | 91.8 | gravity | 233 | home 23 | 12 | -0.0122
    
    """)
    assert name_rows_output == name_rows_expected_output
    a.close()

def display_run_table_sqlite_backend():
    ingest_schema_run_table_sqlite_backend()

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

    run_id | specification | n    | o       | p   | q       | r  | s      
    ----------------------------------------------------------------------
    1      | !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
      ... showing 1 of 2 rows
    
    """)
    assert num_output == num_expected_output

    num_display_f = io.StringIO()
    with redirect_stdout(num_display_f):
        a.display("physics", num_rows=1, display_cols=["run_id", "n", "p", "s"])
    num_display_output = num_display_f.getvalue()

    num_display_expected_output = textwrap.dedent("""
    Table: physics

    run_id | n    | p     | s      
    -------------------------------
    1.0    | 9.8  | 23.0  | -0.0012
      ... showing 1 of 2 rows
    
    """)
    assert num_display_output == num_display_expected_output
    a.close()

def get_table_sqlite_backend():
    ingest_schema_run_table_sqlite_backend()

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
def ingest_duckdb_backend():
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

def process_duckdb_backend():
    ingest_duckdb_backend()

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

def table_info_duckdb_backend():
    ingest_duckdb_backend()

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

def overwrite_duckdb_backend():
    ingest_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-write', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM physics;")
    query_data['n'] = 2000
    a.overwrite_table(table_name="physics", collection=query_data)
    a.artifact_handler(interaction_type="process")
    assert a.active_metadata["physics"]["n"] == [2000, 2000]
    a.close()

def ingest_schema_duckdb_backend():
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

def process_schema_duckdb_backend():
    ingest_schema_duckdb_backend()

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

def overwrite_schema_duckdb_backend():
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

def ingest_schema_run_table_duckdb_backend():
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

def process_schema_run_table_duckdb_backend():
    ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)
    a.artifact_handler(interaction_type="process")
    assert a.runTable == True

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

def query_schema_run_table_duckdb_backend():
    ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)
    query_data = a.artifact_handler(interaction_type="query", query = "SELECT * FROM runTable;")
    assert query_data.columns.tolist() == ["run_id", "run_timestamp"]
    assert query_data["run_id"].tolist() == [1]
    a.close()

def table_info_run_table_duckdb_backend():
    ingest_schema_run_table_duckdb_backend()

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

def summary_run_table_duckdb_backend():
    ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.summary()
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: address

    column        | type    | min               | max               | avg               | std_dev           
    --------------------------------------------------------------------------------------------------------
    run_id        | INTEGER | 1                 | 1                 | 1.0               | 0.0               
    specification | VARCHAR | None              | None              | None              | None              
    fileLoc       | VARCHAR | None              | None              | None              | None              
    g             | VARCHAR | None              | None              | None              | None              
    h             | FLOAT   | 9.800000190734863 | 91.80000305175781 | 50.80000162124634 | 57.982758080345626
    i*            | INTEGER | 2                 | 3                 | 2.5               | 0.7071067811865476
    j             | INTEGER | 3                 | 4                 | 3.5               | 0.7071067811865476
    k             | INTEGER | 4                 | 5                 | 4.5               | 0.7071067811865476
    l             | FLOAT   | 1.0               | 11.0              | 6.0               | 7.0710678118654755
    m             | INTEGER | 99                | 999               | 549.0             | 636.3961030678928 
      - num of rows: 2


    Table: dsi_units

    column      | type    | min  | max  | avg  | std_dev
    ----------------------------------------------------
    table_name  | VARCHAR | None | None | None | None   
    column_name | VARCHAR | None | None | None | None   
    unit        | VARCHAR | None | None | None | None   
      - num of rows: 5


    Table: math

    column         | type    | min                  | max                 | avg                  | std_dev              
    --------------------------------------------------------------------------------------------------------------------
    run_id         | INTEGER | 1                    | 1                   | 1.0                  | 0.0                  
    specification* | VARCHAR | None                 | None                | None                 | None                 
    a              | INTEGER | 1                    | 2                   | 1.5                  | 0.7071067811865476   
    b              | INTEGER | 2                    | 3                   | 2.5                  | 0.7071067811865476   
    c              | FLOAT   | 45.97999954223633    | 45.97999954223633   | 45.97999954223633    | 0.0                  
    d              | INTEGER | 2                    | 3                   | 2.5                  | 0.7071067811865476   
    e              | FLOAT   | 34.79999923706055    | 44.79999923706055   | 39.79999923706055    | 7.0710678118654755   
    f              | FLOAT   | 0.008899999782443047 | 0.00989999994635582 | 0.009399999864399433 | 0.0007071068970903809
      - num of rows: 2


    Table: physics

    column        | type    | min                   | max                    | avg                   | std_dev             
    -----------------------------------------------------------------------------------------------------------------------
    run_id        | INTEGER | 1                     | 1                      | 1.0                   | 0.0                 
    specification | VARCHAR | None                  | None                   | None                  | None                
    n*            | FLOAT   | 9.800000190734863     | 91.80000305175781      | 50.80000162124634     | 57.982758080345626  
    o             | VARCHAR | None                  | None                   | None                  | None                
    p             | INTEGER | 23                    | 233                    | 128.0                 | 148.49242404917499  
    q             | VARCHAR | None                  | None                   | None                  | None                
    r             | INTEGER | 1                     | 12                     | 6.5                   | 7.7781745930520225  
    s             | FLOAT   | -0.012199999764561653 | -0.0012000000569969416 | -0.006699999910779297 | 0.007778174386269048
      - num of rows: 2


    Table: runTable

    column        | type    | min  | max  | avg  | std_dev
    ------------------------------------------------------
    run_id*       | INTEGER | 1    | 1    | 1.0  | None   
    run_timestamp | VARCHAR | None | None | None | None   
      - num of rows: 1
    
    """)
    assert output == expected_output

    name_f = io.StringIO()
    with redirect_stdout(name_f):
        a.summary(table_name='physics')
    name_output = name_f.getvalue()

    name_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | min                   | max                    | avg                   | std_dev             
    -----------------------------------------------------------------------------------------------------------------------
    run_id        | INTEGER | 1                     | 1                      | 1.0                   | 0.0                 
    specification | VARCHAR | None                  | None                   | None                  | None                
    n*            | FLOAT   | 9.800000190734863     | 91.80000305175781      | 50.80000162124634     | 57.982758080345626  
    o             | VARCHAR | None                  | None                   | None                  | None                
    p             | INTEGER | 23                    | 233                    | 128.0                 | 148.49242404917499  
    q             | VARCHAR | None                  | None                   | None                  | None                
    r             | INTEGER | 1                     | 12                     | 6.5                   | 7.7781745930520225  
    s             | FLOAT   | -0.012199999764561653 | -0.0012000000569969416 | -0.006699999910779297 | 0.007778174386269048
      - num of rows: 2
    
    """)
    assert name_output == name_expected_output

    name_rows_f = io.StringIO()
    with redirect_stdout(name_rows_f):
        a.summary(table_name='physics', num_rows = 3)
    name_rows_output = name_rows_f.getvalue()

    name_rows_expected_output = textwrap.dedent("""
    Table: physics

    column        | type    | min                   | max                    | avg                   | std_dev             
    -----------------------------------------------------------------------------------------------------------------------
    run_id        | INTEGER | 1                     | 1                      | 1.0                   | 0.0                 
    specification | VARCHAR | None                  | None                   | None                  | None                
    n*            | FLOAT   | 9.800000190734863     | 91.80000305175781      | 50.80000162124634     | 57.982758080345626  
    o             | VARCHAR | None                  | None                   | None                  | None                
    p             | INTEGER | 23                    | 233                    | 128.0                 | 148.49242404917499  
    q             | VARCHAR | None                  | None                   | None                  | None                
    r             | INTEGER | 1                     | 12                     | 6.5                   | 7.7781745930520225  
    s             | FLOAT   | -0.012199999764561653 | -0.0012000000569969416 | -0.006699999910779297 | 0.007778174386269048

    run_id | specification | n                 | o       | p   | q       | r  | s                     
    --------------------------------------------------------------------------------------------------
    1      | !amy          | 9.800000190734863 | gravity | 23  | home 23 | 1  | -0.0012000000569969416
    1      | !amy1         | 91.80000305175781 | gravity | 233 | home 23 | 12 | -0.012199999764561653 
    
    """)
    assert name_rows_output == name_rows_expected_output
    a.close()

def display_run_table_duckdb_backend():
    ingest_schema_run_table_duckdb_backend()

    a = Terminal()
    dbpath = 'data.db'
    a.load_module('backend','DuckDB','back-read', filename=dbpath)

    f = io.StringIO()
    with redirect_stdout(f):
        a.display("physics")
    output = f.getvalue()

    expected_output = textwrap.dedent("""
    Table: physics

    run_id | specification | n                 | o       | p   | q       | r  | s                     
    --------------------------------------------------------------------------------------------------
    1      | !amy          | 9.800000190734863 | gravity | 23  | home 23 | 1  | -0.0012000000569969416
    1      | !amy1         | 91.80000305175781 | gravity | 233 | home 23 | 12 | -0.012199999764561653 
    
    """)
    assert output == expected_output

    num_f = io.StringIO()
    with redirect_stdout(num_f):
        a.display("physics", num_rows=1)
    num_output = num_f.getvalue()

    num_expected_output = textwrap.dedent("""
    Table: physics

    run_id | specification | n                 | o       | p   | q       | r  | s                     
    --------------------------------------------------------------------------------------------------
    1      | !amy          | 9.800000190734863 | gravity | 23  | home 23 | 1  | -0.0012000000569969416
      ... showing 1 of 2 rows
    
    """)
    assert num_output == num_expected_output

    num_display_f = io.StringIO()
    with redirect_stdout(num_display_f):
        a.display("physics", num_rows=1, display_cols=["run_id", "n", "p", "s"])
    num_display_output = num_display_f.getvalue()

    num_display_expected_output = textwrap.dedent("""
    Table: physics

    run_id | n                 | p     | s                     
    -----------------------------------------------------------
    1.0    | 9.800000190734863 | 23.0  | -0.0012000000569969416
      ... showing 1 of 2 rows
    
    """)
    assert num_display_output == num_display_expected_output
    a.close()

def get_table_duckdb_backend():
    ingest_schema_run_table_duckdb_backend()

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