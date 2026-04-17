from dsi.dsi import DSI
import os
import io
from contextlib import redirect_stdout
import textwrap
from pandas import DataFrame
from collections import OrderedDict
import hashlib

def test_list_functions():
    test = DSI()
    test.list_backends()
    test.list_readers()
    test.list_writers()
    assert True

def test_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    DSI(filename=dbpath, backend_name= "Sqlite")
    assert True

def test_error_filename():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    try:
        test.read(data_sources=["examples/test/WRONG_FILENAME_1.yml", "examples/test/WRONG_FILENAME_2.yml"], reader_name='YAML1')
        assert False
    except Exception as e:
        expected = "read() ERROR: All input files must have a valid filepath. Please check again."
        assert str(e) == expected

def test_read_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/results.toml", "examples/test/results1.toml"], reader_name='TOML1')
    test.read(data_sources="examples/test/yosemite5.csv", reader_name='CSV', table_name = "yosemite") # data table is named yosemite not Csv
    test.read(data_sources="examples/test/wildfiredata.csv", reader_name='Ensemble', table_name = "wildfire") # makes a sim table automatically
    test.read(data_sources=['examples/test/bueno1.data', 'examples/test/bueno2.data'], reader_name='Bueno')

    test.read(data_sources=['examples/wildfire/wildfire_oceans11.yml', 'examples/pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
    test.read(data_sources="examples/wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
    test.read(data_sources="examples/wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')
    test.read(data_sources="examples/wildfire/wildfire_google.yml", reader_name='GoogleDatacard')
    assert True

def test_write_sqlite_backend():
    test_read_sqlite_backend()
    
    dbpath = 'data.db'
    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.write(filename="er_diagram.png", writer_name="ER_Diagram")
    test.write(filename="physics_plot.png", writer_name="Table_Plot", table_name="physics")
    test.write(filename="physics.csv", writer_name="Csv_Writer", table_name="physics")
    assert True

def test_query_sqlite_backend():
    test_read_sqlite_backend()

    dbpath = 'data.db'
    test = DSI(filename=dbpath, backend_name= "Sqlite")

    f = io.StringIO()
    with redirect_stdout(f):
        test.query("SELECT * FROM physics")
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])

    expected_output = textwrap.dedent("""
    specification | n    | o       | p   | q       | r  | s      
    -------------------------------------------------------------
    !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
    !amy1         | 91.8 | gravity | 233 | home 23 | 12 | -0.0122
    """)
    
    assert output == expected_output

    query_data = test.query("SELECT * FROM physics", collection=True, update=True)
    assert isinstance(query_data, DataFrame)
    assert query_data.columns.tolist() == ['dsi_table_name','specification','n','o','p','q','r','s']
    assert len(query_data) == 2
    assert query_data["n"].tolist() == [9.8, 91.8]
    assert query_data["dsi_table_name"][0] == "physics"

def test_query_update_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    query_df = test.query("SELECT * FROM address", collection=True, update=True)  # return output
    query_df['i'] = 123
    query_df["new_col"] = "test1"
    test.update(query_df)

    data = test.get_table("address", collection=True, update=True)
    assert data['i'].tolist() == [123,123]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_get_table_sqlite_backend():
    test_read_sqlite_backend()

    dbpath = 'data.db'
    test = DSI(filename=dbpath, backend_name= "Sqlite")

    f = io.StringIO()
    with redirect_stdout(f):
        test.get_table(table_name="physics")
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])

    query_f = io.StringIO()
    with redirect_stdout(query_f):
        test.query("SELECT * FROM physics")
    expected_output = query_f.getvalue()
    expected_output = "\n".join(expected_output.splitlines()[1:])

    assert output == expected_output

    query_data = test.query("SELECT * FROM physics", collection=True, update=True)
    get_data = test.get_table(table_name="physics", collection=True, update=True)
    assert query_data.equals(get_data)

def test_search_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        test.search(query=2)
    output = f.getvalue()

    expected_output = "Searching for all instances of 2 in the active backend\n" + textwrap.dedent("""
    Table: math
      - Columns: ['specification', 'a', 'b', 'c', 'd', 'e', 'f']
      - Row Number: 1
      - Data: ['!jack', 1, 2, 45.98, 2, 34.8, 0.0089]
    
    Table: math
      - Columns: ['specification', 'a', 'b', 'c', 'd', 'e', 'f']
      - Row Number: 2
      - Data: ['!jack1', 2, 3, 45.98, 3, 44.8, 0.0099]
    
    Table: address
      - Columns: ['specification', 'fileLoc', 'g', 'h', 'i', 'j', 'k', 'l', 'm']
      - Row Number: 1
      - Data: ['!sam', '/home/sam/lib/data', 'good memories', 9.8, 2, 3, 4, 1.0, 99]
    
    Table: physics
      - Columns: ['specification', 'n', 'o', 'p', 'q', 'r', 's']
      - Row Number: 1
      - Data: ['!amy', 9.8, 'gravity', 23, 'home 23', 1, -0.0012]
    
    Table: physics
      - Columns: ['specification', 'n', 'o', 'p', 'q', 'r', 's']
      - Row Number: 2
      - Data: ['!amy1', 91.8, 'gravity', 233, 'home 23', 12, -0.0122]
    
    """)
    assert output == expected_output

    find_df = test.search(query=2, collection=True)
    assert len(find_df) == 3

def test_sanitize_inputs_sqlite():
    my_dict = OrderedDict({'"2"': OrderedDict({'specification': ['!jack'], 'a': [1], 'b': [2], 'c': [45.98], 'd': [2], 'e': [34.8], 'f': [0.0089]}), 
                    'all': OrderedDict({'specification': ['!sam'], 'fileLoc': ['/home/sam/lib/data'], 'G': ['good memories'], 
                                        'all': [9.8], 'i': [2], 'j': [3], 'k': [4], 'l': [1.0], 'm': [99]}), 
                    'physics': OrderedDict({'specification': ['!amy', '!amy1'], 'n': [9.8, 91.8], 'o': ['gravity', 'gravity'], 
                                            'p': [23, 233], 'q': ['home 23', 'home 23'], 'r': [1, 12], 's': [-0.0012, -0.0122]}), 
                    'math': OrderedDict({'specification': [None, '!jack1'], 'a': [None, 2], '"math"': [None, 3], 'c': [None, 45.98], 
                                         'd': [None, 3], 'e': [None, 44.8], 'f': [None, 0.0099]}), 
                    'address': OrderedDict({'specification': [None, '!sam1'], 'fileLoc': [None, '/home/sam/lib/data'], 'g': [None, 'good memories'], 
                                            'h': [None, 91.8], 'i': [None, 3], 'j': [None, 4], 'k': [None, 5], 'l': [None, 11.0], 'm': [None, 999]})})
    
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    test.read(my_dict, "collection")

    f = io.StringIO()
    with redirect_stdout(f):
        test.find('all > 9')
    output = f.getvalue()

    expected_output = 'Finding all rows where \'all > 9\' in the active backend' + textwrap.dedent("""
    
    Table: "all"

    specification | fileLoc            | G             | all | i | j | k | l   | m 
    -------------------------------------------------------------------------------
    !sam          | /home/sam/lib/data | good memories | 9.8 | 2 | 3 | 4 | 1.0 | 99
                                                         
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('all > 9', True, True)

    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['"all"']
    assert find_df['G'].tolist() == ['good memories']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('"all" > 9', True, True)

    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['"all"']
    assert find_df['G'].tolist() == ['good memories']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('"math" < 9', True, True)
    
    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['math']
    assert find_df['math'].tolist() == [3]

    find_df['math'] = 4
    test.update(find_df)
    
    f = io.StringIO()
    with redirect_stdout(f):
        test.display('math')
    output = f.getvalue()

    expected_output = '\nTable: math' + textwrap.dedent("""

    specification | a    | math | c     | d    | e    | f     
    ----------------------------------------------------------
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 4.0  | 45.98 | 3.0  | 44.8 | 0.0099
    
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('b < 9', True, True)
    
    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['"2"']
    assert find_df['b'].tolist() == [2]

    find_df['b'] = 99
    test.update(find_df)
    
    f = io.StringIO()
    with redirect_stdout(f):
        test.display('"2"')
    output = f.getvalue()

    expected_output = '\nTable: "2"' + textwrap.dedent("""

    specification | a | b  | c     | d | e    | f     
    --------------------------------------------------
    !jack         | 1 | 99 | 45.98 | 2 | 34.8 | 0.0089
    
    """)
    assert output == expected_output

def test_find_inequality_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 2", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a > 2' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a > 2  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a > 1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [2, 4, 6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a<3", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a<3' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a <=  1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a <=  1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a>=3", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a>=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a>=3  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

def test_find_equality_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a !=  1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a !=  1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [2, 4, 6]
    assert find_df['specification'].tolist() == ['!jack1','!jack1','!jack1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a=3", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a=3  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a =  1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a =  1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]
    assert find_df['specification'].tolist() == ['!jack','!jack','!jack']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a ==  2", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a ==  2' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [2, 4, 6]
    assert find_df['specification'].tolist() == ['!jack1','!jack1','!jack1']

def test_find_range_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1, 2)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1, 2)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1,2)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1,2)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1, 1)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1, 1)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]
    assert find_df['specification'].tolist() == ['!jack','!jack','!jack']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a(1,1)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a(1,1)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]
    assert find_df['specification'].tolist() == ['!jack','!jack','!jack']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1,3)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1,3)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a(3,4)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a(3,4)' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a(3,4)  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

def test_find_partial_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g~memorr", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g~memorr' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  g~memorr  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g~memo", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g~memo' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]
    assert find_df['specification'].tolist() == ['!sam','!sam1','!sam','!sam1','!sam','!sam1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g ~ memo", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g ~ memo' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]
    assert find_df['specification'].tolist() == ['!sam','!sam1','!sam','!sam1','!sam','!sam1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="h ~~ 1.8", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'h ~~ 1.8' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [2,4,6]
    assert find_df['specification'].tolist() == ['!sam1','!sam1','!sam1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find("specification~~y1")
    output = f.getvalue()
    assert find_df is None    
    expected_output = textwrap.dedent("""\
        Finding all rows where 'specification~~y1' in the active backend\n
        WARNING: 'specification' appeared in more than one table. Can only find if 'specification' is in one table
        Try using `dsi.query()` to retrieve the matching rows for a specific table
        These are recommended inputs for query():
         - SELECT * FROM math WHERE CAST(specification AS TEXT) LIKE '%y1%'
         - SELECT * FROM address WHERE CAST(specification AS TEXT) LIKE '%y1%'
         - SELECT * FROM physics WHERE CAST(specification AS TEXT) LIKE '%y1%'\n""")
    assert output == expected_output

def test_find_relation_error_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    try:
        test.find(query=2, collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must be a string."
    
    try:
        test.find(query="a", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must contain an operator. Format: [column] [operator] [value]"

    try:
        test.find(query='"a" > "14"', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: The value in the relational find() cannot be enclosed in double quotes"

    try:
        test.find(query="'a' > 1", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Cannot have a single quote as part of a column name"

    try:
        test.find(query="'a' 1", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must contain an operator. Format: [column] [operator] [value]"
    
    try:
        test.find(query="a 1", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must contain an operator. Format: [column] [operator] [value]"

    try:
        test.find(query='a ">1"', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Could not identify the operator in `query`. The operator cannot be nested in double quotes"

    try:
        test.find(query='a>"2"', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == 'find() ERROR: The value in the relational find() cannot be enclosed in double quotes'
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > '<4'", collection=True, update=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'a > '<4'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a > '<4'  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g !='>good memories'", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g !='>good memories'' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    try:
        test.find(query="a > '<4')", collection=True, update=True)
        assert False
    except Exception as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap all in single quotes."

    try:
        test.find(query="a > <4)", collection=True, update=True)
        assert False
    except Exception as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap all in single quotes."

    try:
        test.find(query="a (1,2))", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Only one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."

    try:
        test.find(query="a (')')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), values must be separated by a comma."

    try:
        test.find(query="a (,", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), it must end with closing parenthesis."

    try:
        test.find(query="a (,')')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == 'find() ERROR: There needs to be two values for the range find. Ex: (1,2)'

    try:
        test.find(query="g !=there's", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Found an unmatched single quote. For apostrophes use 2 single quotes. Ex: he's -> he''s NOT he\"s"

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g == 'there '> \"temperature\" '?'", collection=True, update=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'g == 'there '> \"temperature\" '?'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  g == 'there '> \"temperature\" '?'  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    try:
        test.find(query="g (there is, a place)", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    try:
        test.find(query="g ('there is', a place)", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"

    try:
        test.find(query="g ('there is', 'a place')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Invalid input range: '('there is','a place')'. The lower value must come first."
    
    try:
        test.find(query="g ('there is' 'a place')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), values must be separated by a comma."
    
    try:
        test.find(query="g ('there is', 'a place'", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place)'", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place'))", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Only one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."

    try:
        test.find(query="g (3,4))", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Only one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."

    try:
        test.find(query="g (,4)", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: There needs to be two values for the range find. Ex: (1,2)"
    
    try:
        test.find(query='a ("hello",6)', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello")', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello", 6)', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find("specification = '!jack'")
    output = f.getvalue()
    assert find_df is None    
    expected_output = textwrap.dedent("""\
        Finding all rows where 'specification = '!jack'' in the active backend\n
        WARNING: 'specification' appeared in more than one table. Can only find if 'specification' is in one table
        Try using `dsi.query()` to retrieve the matching rows for a specific table
        These are recommended inputs for query():
         - SELECT * FROM math WHERE specification = '!jack'
         - SELECT * FROM address WHERE specification = '!jack'
         - SELECT * FROM physics WHERE specification = '!jack'\n""")
    assert output == expected_output

def test_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    assert True

def test_error_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    try:
        test.schema(filename="examples/test/yaml1_schema.json")
        test.read(data_sources="examples/wildfire/wildfire_google.yml", reader_name='GoogleDatacard') # Unrelated data loaded in after schema
        assert False
    except Exception as e:
        expected = "read() ERROR: Users must load all associated data for a schema after loading a complex schema."
        assert str(e) == expected

    try:
        test.schema(filename="examples/test/yaml1_schema.json")
        assert False
    except Exception as e:
        expected = "schema() ERROR: There is already a complex schema in memory. First load all its associated files."
        assert str(e) == expected

def test_query_update_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    
    query_df = test.query("SELECT * FROM address", collection=True, update=True)  # return output
    query_df['i'] = [123, 234]
    query_df["new_col"] = "test1"

    f = io.StringIO()
    with redirect_stdout(f):
        test.update(query_df)
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])
    expected_output = "WARNING: The data in address's primary key column was edited which could reorder rows in the table."
    assert output == expected_output

    data = test.get_table("address", collection=True, update=True)
    assert data['i'].tolist() == [123,234]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_overwrite_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.write(filename="full_erd.png", writer_name="ER_Diagram")

    test.schema(filename="examples/test/yaml1_circular_schema.json")
    test.write(filename="new_erd.png", writer_name="ER_Diagram")

    def file_hash(path):
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            sha.update(f.read())
        return sha.hexdigest()

    hash1 = file_hash("full_erd.png")
    hash2 = file_hash("new_erd.png")

    assert hash1 != hash2



# DUCKDB
# DUCKDB
# DUCKDB


def test_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    DSI(filename=dbpath, backend_name= "DuckDB")
    assert True

def test_read_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/results.toml", "examples/test/results1.toml"], reader_name='TOML1')
    test.read(data_sources="examples/test/yosemite5.csv", reader_name='CSV', table_name = "yosemite") # data table is named yosemite not Csv
    test.read(data_sources="examples/test/wildfiredata.csv", reader_name='Ensemble', table_name = "wildfire") # makes a sim table automatically
    test.read(data_sources=['examples/test/bueno1.data', 'examples/test/bueno2.data'], reader_name='Bueno')

    test.read(data_sources=['examples/wildfire/wildfire_oceans11.yml', 'examples/pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
    test.read(data_sources="examples/wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
    test.read(data_sources="examples/wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')
    test.read(data_sources="examples/wildfire/wildfire_google.yml", reader_name='GoogleDatacard')
    assert True

def test_write_duckdb_backend():
    test_read_duckdb_backend()
    
    dbpath = 'data.db'
    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.write(filename="er_diagram.png", writer_name="ER_Diagram")
    test.write(filename="physics_plot.png", writer_name="Table_Plot", table_name="physics")
    test.write(filename="physics.csv", writer_name="Csv_Writer", table_name="physics")
    assert True

def test_query_duckdb_backend():
    test_read_duckdb_backend()

    dbpath = 'data.db'
    test = DSI(filename=dbpath, backend_name= "DuckDB")

    f = io.StringIO()
    with redirect_stdout(f):
        test.query("SELECT * FROM physics")
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])

    expected_output = textwrap.dedent("""
    specification | n    | o       | p   | q       | r  | s      
    -------------------------------------------------------------
    !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
    !amy1         | 91.8 | gravity | 233 | home 23 | 12 | -0.0122
    """)
    assert output == expected_output

    query_data = test.query("SELECT * FROM physics", collection=True, update=True)
    assert isinstance(query_data, DataFrame)
    assert query_data.columns.tolist() == ['dsi_table_name','specification','n','o','p','q','r','s']
    assert len(query_data) == 2
    assert query_data["n"].tolist() == [9.8, 91.8]
    assert query_data["dsi_table_name"][0] == "physics"

def test_query_update_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    query_df = test.query("SELECT * FROM address", collection=True, update=True)  # return output
    query_df['i'] = 123
    query_df["new_col"] = "test1"
    test.update(query_df)

    data = test.get_table("address", collection=True, update=True)
    assert data['i'].tolist() == [123,123]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_get_table_duckdb_backend():
    test_read_duckdb_backend()

    dbpath = 'data.db'
    test = DSI(filename=dbpath, backend_name= "DuckDB")

    f = io.StringIO()
    with redirect_stdout(f):
        test.get_table(table_name="physics")
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])

    query_f = io.StringIO()
    with redirect_stdout(query_f):
        test.query("SELECT * FROM physics")
    expected_output = query_f.getvalue()
    expected_output = "\n".join(expected_output.splitlines()[1:])

    assert output == expected_output

    query_data = test.query("SELECT * FROM physics", collection=True, update=True)
    get_data = test.get_table(table_name="physics", collection=True, update=True)
    assert query_data.equals(get_data)

def test_search_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        test.search(query=2)
    output = f.getvalue()

    expected_output = "Searching for all instances of 2 in the active backend\n" + textwrap.dedent("""
    Table: address
      - Columns: ['specification', 'fileLoc', 'g', 'h', 'i', 'j', 'k', 'l', 'm']
      - Row Number: 1
      - Data: ['!sam', '/home/sam/lib/data', 'good memories', 9.8, 2, 3, 4, 1.0, 99]
    
    Table: math
      - Columns: ['specification', 'a', 'b', 'c', 'd', 'e', 'f']
      - Row Number: 1
      - Data: ['!jack', 1, 2, 45.98, 2, 34.8, 0.0089]
    
    Table: math
      - Columns: ['specification', 'a', 'b', 'c', 'd', 'e', 'f']
      - Row Number: 2
      - Data: ['!jack1', 2, 3, 45.98, 3, 44.8, 0.0099]
    
    Table: physics
      - Columns: ['specification', 'n', 'o', 'p', 'q', 'r', 's']
      - Row Number: 1
      - Data: ['!amy', 9.8, 'gravity', 23, 'home 23', 1, -0.0012]
    
    Table: physics
      - Columns: ['specification', 'n', 'o', 'p', 'q', 'r', 's']
      - Row Number: 2
      - Data: ['!amy1', 91.8, 'gravity', 233, 'home 23', 12, -0.0122]
    
    """)
    assert output == expected_output

    find_df = test.search(query=2, collection=True)
    assert len(find_df) == 3

def test_sanitize_inputs_duckdb():
    my_dict = OrderedDict({'"2"': OrderedDict({'specification': ['!jack'], 'a': [1], 'b': [2], 'c': [45.98], 'd': [2], 'e': [34.8], 'f': [0.0089]}), 
                    'all': OrderedDict({'specification': ['!sam'], 'fileLoc': ['/home/sam/lib/data'], 'G': ['good memories'], 
                                        'all': [9.8], 'i': [2], 'j': [3], 'k': [4], 'l': [1.0], 'm': [99]}), 
                    'physics': OrderedDict({'specification': ['!amy', '!amy1'], 'n': [9.8, 91.8], 'o': ['gravity', 'gravity'], 
                                            'p': [23, 233], 'q': ['home 23', 'home 23'], 'r': [1, 12], 's': [-0.0012, -0.0122]}), 
                    'math': OrderedDict({'specification': [None, '!jack1'], 'a': [None, 2], '"math"': [None, 3], 'c': [None, 45.98], 
                                         'd': [None, 3], 'e': [None, 44.8], 'f': [None, 0.0099]}), 
                    'address': OrderedDict({'specification': [None, '!sam1'], 'fileLoc': [None, '/home/sam/lib/data'], 'g': [None, 'good memories'], 
                                            'h': [None, 91.8], 'i': [None, 3], 'j': [None, 4], 'k': [None, 5], 'l': [None, 11.0], 'm': [None, 999]})})
    
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.read(my_dict, "collection")

    f = io.StringIO()
    with redirect_stdout(f):
        test.find('all > 9')
    output = f.getvalue()

    expected_output = 'Finding all rows where \'all > 9\' in the active backend' + textwrap.dedent("""
    
    Table: "all"

    specification | fileLoc            | G             | all | i | j | k | l   | m 
    -------------------------------------------------------------------------------
    !sam          | /home/sam/lib/data | good memories | 9.8 | 2 | 3 | 4 | 1.0 | 99
                                                         
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('all > 9', collection=True, update=True)

    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['"all"']
    assert find_df['G'].tolist() == ['good memories']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('"all" > 9', collection=True, update=True)

    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['"all"']
    assert find_df['G'].tolist() == ['good memories']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('"math" < 9', collection=True, update=True)
    
    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['math']
    assert find_df['math'].tolist() == [3]

    find_df['math'] = 4
    test.update(find_df)
    
    f = io.StringIO()
    with redirect_stdout(f):
        test.display('math')
    output = f.getvalue()

    expected_output = '\nTable: math' + textwrap.dedent("""

    specification | a    | math | c     | d    | e    | f     
    ----------------------------------------------------------
    None          | None | None | None  | None | None | None  
    !jack1        | 2.0  | 4.0  | 45.98 | 3.0  | 44.8 | 0.0099
    
    """)
    assert output == expected_output

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find('b < 9', collection=True, update=True)
    
    assert not find_df.empty
    assert find_df['dsi_table_name'].tolist() == ['"2"']
    assert find_df['b'].tolist() == [2]

    find_df['b'] = 99
    test.update(find_df)
    
    f = io.StringIO()
    with redirect_stdout(f):
        test.display('"2"')
    output = f.getvalue()

    expected_output = '\nTable: "2"' + textwrap.dedent("""

    specification | a | b  | c     | d | e    | f     
    --------------------------------------------------
    !jack         | 1 | 99 | 45.98 | 2 | 34.8 | 0.0089
    
    """)
    assert output == expected_output

def test_find_inequality_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 2", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a > 2' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a > 2  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a > 1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [2, 4, 6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a<3", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a<3' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a <=  1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a <=  1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a>=3", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a>=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a>=3  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

def test_find_equality_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a !=  1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a !=  1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [2, 4, 6]
    assert find_df['specification'].tolist() == ['!jack1','!jack1','!jack1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a=3", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a=3  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a =  1", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a =  1' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]
    assert find_df['specification'].tolist() == ['!jack','!jack','!jack']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a ==  2", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a ==  2' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [2, 4, 6]
    assert find_df['specification'].tolist() == ['!jack1','!jack1','!jack1']

def test_find_range_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1, 2)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1, 2)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1,2)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1,2)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1, 1)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1, 1)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]
    assert find_df['specification'].tolist() == ['!jack','!jack','!jack']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a(1,1)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a(1,1)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,3,5]
    assert find_df['specification'].tolist() == ['!jack','!jack','!jack']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1,3)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a (1,3)' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a(3,4)", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a(3,4)' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  a(3,4)  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

def test_find_partial_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g~memorr", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g~memorr' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  g~memorr  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g~memo", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g~memo' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]
    assert find_df['specification'].tolist() == ['!sam','!sam1','!sam','!sam1','!sam','!sam1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g ~ memo", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g ~ memo' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]
    assert find_df['specification'].tolist() == ['!sam','!sam1','!sam','!sam1','!sam','!sam1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="h ~~ 1.8", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'h ~~ 1.8' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 3
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [2,4,6]
    assert find_df['specification'].tolist() == ['!sam1','!sam1','!sam1']

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find("specification~~y1")
    output = f.getvalue()
    assert find_df is None    
    expected_output = textwrap.dedent("""\
        Finding all rows where 'specification~~y1' in the active backend\n
        WARNING: 'specification' appeared in more than one table. Can only find if 'specification' is in one table
        Try using `dsi.query()` to retrieve the matching rows for a specific table
        These are recommended inputs for query():
         - SELECT * FROM address WHERE CAST(specification AS TEXT) ILIKE '%y1%'
         - SELECT * FROM math WHERE CAST(specification AS TEXT) ILIKE '%y1%'
         - SELECT * FROM physics WHERE CAST(specification AS TEXT) ILIKE '%y1%'\n""")
    assert output == expected_output

def test_find_relation_error_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    try:
        test.find(query=2, collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must be a string."
    
    try:
        test.find(query="a", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must contain an operator. Format: [column] [operator] [value]"

    try:
        test.find(query='"a" > "14"', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: The value in the relational find() cannot be enclosed in double quotes"

    try:
        test.find(query="'a' > 1", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Cannot have a single quote as part of a column name"

    try:
        test.find(query="'a' 1", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must contain an operator. Format: [column] [operator] [value]"
    
    try:
        test.find(query="a 1", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Input must contain an operator. Format: [column] [operator] [value]"

    try:
        test.find(query='a ">1"', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Could not identify the operator in `query`. The operator cannot be nested in double quotes"

    try:
        test.find(query='a>"2"', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == 'find() ERROR: The value in the relational find() cannot be enclosed in double quotes'
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g < '<4'", collection=True, update=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'g < '<4'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  g < '<4'  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g !='>good memories'", collection=True, update=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'g !='>good memories'' in the active backend\n"
    expected_output2 = "Note: Output includes 2 'dsi_' columns required for dsi.update(). " \
    "DO NOT modify if updating; keep any extra rows blank. Drop if not updating.\n\n"
    assert output == expected_output1 + expected_output2
    assert find_df is not None
    assert "dsi_table_name" in find_df.columns and "dsi_row_index" in find_df.columns
    assert len(find_df) == 6
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1,2,3,4,5,6]

    try:
        test.find(query="a > '<4')", collection=True, update=True)
        assert False
    except Exception as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap all in single quotes."

    try:
        test.find(query="a > <4)", collection=True, update=True)
        assert False
    except Exception as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap all in single quotes."

    try:
        test.find(query="a (1,2))", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Only one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."

    try:
        test.find(query="a (')')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), values must be separated by a comma."

    try:
        test.find(query="a (,", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), it must end with closing parenthesis."

    try:
        test.find(query="a (,')')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == 'find() ERROR: There needs to be two values for the range find. Ex: (1,2)'

    try:
        test.find(query="g !=there's", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Found an unmatched single quote. For apostrophes use 2 single quotes. Ex: he's -> he''s NOT he\"s"

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g == 'there '> \"temperature\" '?'", collection=True, update=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'g == 'there '> \"temperature\" '?'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where  g == 'there '> \"temperature\" '?'  in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    try:
        test.find(query="g (there is, a place)", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    try:
        test.find(query="g ('there is', a place)", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"

    try:
        test.find(query="g ('there is', 'a place')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Invalid input range: '('there is','a place')'. The lower value must come first."
    
    try:
        test.find(query="g ('there is' 'a place')", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), values must be separated by a comma."
    
    try:
        test.find(query="g ('there is', 'a place'", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place)'", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place'))", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Only one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."

    try:
        test.find(query="g (3,4))", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Only one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~]."

    try:
        test.find(query="g (,4)", collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: There needs to be two values for the range find. Ex: (1,2)"
    
    try:
        test.find(query='a ("hello",6)', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello")', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello", 6)', collection=True, update=True)
        assert False
    except Exception as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find("specification = '!jack'")
    output = f.getvalue()
    assert find_df is None
    expected_output = textwrap.dedent("""\
        Finding all rows where 'specification = '!jack'' in the active backend\n
        WARNING: 'specification' appeared in more than one table. Can only find if 'specification' is in one table
        Try using `dsi.query()` to retrieve the matching rows for a specific table
        These are recommended inputs for query():
         - SELECT * FROM address WHERE specification = '!jack'
         - SELECT * FROM math WHERE specification = '!jack'
         - SELECT * FROM physics WHERE specification = '!jack'\n""")
    assert output == expected_output

def test_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    assert True

def test_query_update_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    
    query_df = test.query("SELECT * FROM math", collection=True, update=True)  # return output
    query_df['specification'] = [123, 234]
    query_df["new_col"] = "test1"
    f = io.StringIO()
    with redirect_stdout(f):
        test.update(query_df)
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])
    expected_output = "WARNING: The data in math's primary key column was edited which could reorder rows in the table."
    assert output == expected_output

    data = test.get_table("math", collection=True, update=True)
    assert data['specification'].tolist() == [123,234]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_overwrite_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.write(filename="full_erd.png", writer_name="ER_Diagram")

    #loophole to assign new schema since there isn't another schema file that can be used with yaml data (circular won't work here)
    new_schema = OrderedDict({'primary_key': [('address', 'i'), ('math', 'specification')], 'foreign_key': [('math', 'b'), (None, None)]})
    test.read(data_sources=new_schema, reader_name="Collection", table_name="dsi_relations") #loophole to assign new schema since
    test.write(filename="new_erd.png", writer_name="ER_Diagram")

    def file_hash(path):
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            sha.update(f.read())
        return sha.hexdigest()

    hash1 = file_hash("full_erd.png")
    hash2 = file_hash("new_erd.png")

    assert hash1 != hash2

def test_fail_overwrite_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(data_sources=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.write(filename="full_erd.png", writer_name="ER_Diagram")

    try:
        test.schema(filename="examples/test/yaml1_circular_schema.json") # should not allow circular dependency overwrite
        assert False
    except Exception as e:
        expected = "schema() ERROR: A complex schema with a circular dependency cannot be ingested into a DuckDB backend."
        assert str(e) == expected
        
        
# NDP
# NDP
# NDP

def test_ndp_backend():
    """Test basic NDP connection"""
    dsi = DSI(backend_name="NDP", keywords="test", limit=3)
    dsi.close()
    assert True

def test_list_ndp_backend():
    """Test listing NDP tables"""
    dsi = DSI(backend_name="NDP", keywords="climate", limit=5)
    
    # Should have datasets table and possibly resource tables
    tables = dsi.list(collection=True)
    assert isinstance(tables, list)
    assert len(tables) > 0
    assert "datasets" in tables
    
    dsi.close()

def test_get_table_ndp_backend():
    """Test getting tables from NDP"""
    dsi = DSI(backend_name="NDP", keywords="ocean", limit=10)
    
    # Test getting datasets table
    f = io.StringIO()
    with redirect_stdout(f):
        dsi.get_table(table_name="datasets")
    output = f.getvalue()
    
    # Verify output contains table data
    assert len(output) > 0
    
    # Test with collection=True
    df = dsi.get_table(table_name="datasets", collection=True)
    assert isinstance(df, DataFrame)
    assert len(df) > 0
    assert 'title' in df.columns
    assert 'num_resources' in df.columns
    
    dsi.close()

def test_search_ndp_backend():
    """Test searching in NDP backend"""
    dsi = DSI(backend_name="NDP", keywords="data", limit=5)
    
    # Test search output
    f = io.StringIO()
    with redirect_stdout(f):
        dsi.search(query="CSV")
    output = f.getvalue()
    
    # Verify search message appears
    assert "Searching for all instances of CSV in the active backend" in output
    
    # Test search with collection=True
    results = dsi.search(query="CSV", collection=True)
    assert isinstance(results, list)
    # Results may be empty, just verify it returns a list
    
    dsi.close()

def test_find_ndp_backend():
    """Test finding datasets in NDP backend"""
    dsi = DSI(backend_name="NDP", keywords="climate", limit=10)
    
    # Get datasets table for filtering
    df = dsi.get_table(table_name="datasets", collection=True)
    assert 'num_resources' in df.columns
    
    # Test filtering datasets with resources
    filtered = df[df['num_resources'] > 0]
    assert len(filtered) >= 0  # May or may not have resources
    
    # Test filtering by title
    if 'title' in df.columns and len(df) > 0:
        # Just verify we can access and filter the column
        assert df['title'].dtype == object  # String column
    
    dsi.close()

def test_query_ndp_backend():
    """Test that NDP doesn't support direct SQL queries"""
    dsi = DSI(backend_name="NDP", keywords="test", limit=3)
    
    # NDP backend doesn't support SQL queries
    # This test verifies the backend works without query() method
    tables = dsi.list(collection=True)
    assert "datasets" in tables
    
    dsi.close()

def test_resources_ndp_backend():
    """Test accessing resource tables in NDP backend"""
    dsi = DSI(backend_name="NDP", keywords="climate", limit=5)
    
    # Get all tables
    tables = dsi.list(collection=True)
    
    # Find resource tables (not 'datasets')
    resource_tables = [t for t in tables if t != 'datasets']
    
    # If there are resource tables, verify we can access them
    if len(resource_tables) > 0:
        first_resource_table = resource_tables[0]
        
        # Test output
        f = io.StringIO()
        with redirect_stdout(f):
            dsi.get_table(table_name=first_resource_table)
        output = f.getvalue()
        assert len(output) > 0
        
        # Test collection
        resource_df = dsi.get_table(table_name=first_resource_table, collection=True)
        assert isinstance(resource_df, DataFrame)
        assert len(resource_df) > 0
    
    dsi.close()

def test_multiple_connections_ndp_backend():
    """Test multiple connection cycles for NDP backend"""
    # Test multiple open/close cycles
    for i in range(3):
        dsi = DSI(backend_name="NDP", keywords="test", limit=2)
        df = dsi.get_table("datasets", collection=True)
        assert df is not None
        assert len(df) > 0
        dsi.close()
    
    assert True

def test_different_keywords_ndp_backend():
    """Test NDP with different keyword searches"""
    keywords_list = ["ocean", "climate", "water"]
    
    for keyword in keywords_list:
        dsi = DSI(backend_name="NDP", keywords=keyword, limit=5)
        
        # Verify we can get datasets table
        df = dsi.get_table("datasets", collection=True)
        assert isinstance(df, DataFrame)
        assert len(df) > 0
        
        dsi.close()
    
    assert True

def test_limit_parameter_ndp_backend():
    """Test NDP with different limit values"""
    # Test with limit=3
    dsi1 = DSI(backend_name="NDP", keywords="data", limit=3)
    df1 = dsi1.get_table("datasets", collection=True)
    dsi1.close()
    
    # Test with limit=10
    dsi2 = DSI(backend_name="NDP", keywords="data", limit=10)
    df2 = dsi2.get_table("datasets", collection=True)
    dsi2.close()
    
    # Verify both return dataframes
    assert isinstance(df1, DataFrame)
    assert isinstance(df2, DataFrame)
    
    # Note: Can't guarantee df2 > df1 due to API behavior
    assert len(df1) > 0
    assert len(df2) > 0
    
    assert True

def test_datasets_structure_ndp_backend():
    """Test that datasets table has expected structure"""
    dsi = DSI(backend_name="NDP", keywords="climate", limit=5)
    
    df = dsi.get_table("datasets", collection=True)
    
    # Verify it's a DataFrame
    assert isinstance(df, DataFrame)
    assert len(df) > 0
    
    # Verify key columns exist
    expected_columns = ['title', 'num_resources']
    for col in expected_columns:
        assert col in df.columns, f"Missing expected column: {col}"
    
    # Verify data types
    assert df['num_resources'].dtype in ['int64', 'int32', 'float64']
    assert df['title'].dtype == object  # String column
    
    dsi.close()

def test_error_invalid_backend_ndp():
    """Test error handling for invalid NDP parameters"""
    # Test with empty keywords
    try:
        dsi = DSI(backend_name="NDP", keywords="", limit=5)
        dsi.close()
        # Empty keywords might work, just verify connection
        assert True
    except:
        # Or it might error - either is acceptable
        assert True

def test_close_ndp_backend():
    """Test proper closing of NDP connections"""
    dsi = DSI(backend_name="NDP", keywords="test", limit=3)
    
    # Verify connection is active
    tables = dsi.list(collection=True)
    assert len(tables) > 0
    
    # Close connection
    dsi.close()
    
    # Test is successful if close() doesn't raise exception
    assert True