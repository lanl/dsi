from dsi.dsi import DSI
import os
import io
from contextlib import redirect_stdout
import textwrap
from pandas import DataFrame


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

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    assert True

def test_error_filename():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    try:
        test.read(filenames=["examples/test/WRONG_FILENAME_1.yml", "examples/test/WRONG_FILENAME_2.yml"], reader_name='YAML1')
        assert False
    except SystemExit as e:
        expected = "read() ERROR: All input files must have a valid filepath. Please check again."
        assert str(e) == expected

def test_read_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/results.toml", "examples/test/results1.toml"], reader_name='TOML1')
    test.read(filenames="examples/test/yosemite5.csv", reader_name='CSV', table_name = "yosemite") # data table is named yosemite not Csv
    test.read(filenames="examples/test/wildfiredata.csv", reader_name='Ensemble', table_name = "wildfire") # makes a sim table automatically
    test.read(filenames=['examples/test/bueno1.data', 'examples/test/bueno2.data'], reader_name='Bueno')

    test.read(filenames=['examples/wildfire/wildfire_oceans11.yml', 'examples/pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
    test.read(filenames="examples/wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
    test.read(filenames="examples/wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')
    test.read(filenames="examples/wildfire/wildfire_google.yml", reader_name='GoogleDatacard')
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

    excepted_output = textwrap.dedent("""
    specification | n    | o       | p   | q       | r  | s      
    -------------------------------------------------------------
    !amy          | 9.8  | gravity | 23  | home 23 | 1  | -0.0012
    !amy1         | 91.8 | gravity | 233 | home 23 | 12 | -0.0122
    """)
    
    assert output == excepted_output

    query_data = test.query("SELECT * FROM physics", collection=True)
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

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    query_df = test.query("SELECT * FROM address", collection=True)  # return output
    query_df['i'] = 123
    query_df["new_col"] = "test1"
    test.update(query_df)

    data = test.get_table("address", collection=True)
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
    excepted_output = query_f.getvalue()
    excepted_output = "\n".join(excepted_output.splitlines()[1:])

    assert output == excepted_output

    query_data = test.query("SELECT * FROM physics", collection=True)
    get_data = test.get_table(table_name="physics", collection=True)
    assert query_data.equals(get_data)

def test_find_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        test.find(query=2)
    output = f.getvalue()

    expected_output = "Finding all instances of 2 in the active backend\n" + textwrap.dedent("""
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

    find_df = test.find(query=2, collection=True)
    assert find_df.columns.tolist()[0] == "dsi_table_name"
    assert find_df["dsi_table_name"][0] == "math"
    assert find_df["dsi_row_index"].tolist() == [1,2]

def test_find_update_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    find_df = test.find(query=2, collection=True)   # return output

    find_df['i'] = list(range(2000, 2000 + len(find_df)))
    find_df['b'] = list(range(2000, 2000 + len(find_df)))
    find_df["new_col"] = "test1"
    test.update(find_df)

    data = test.get_table("math", collection=True)
    assert data['b'].tolist() == [2000,2001]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_find_inequality_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 2", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a > 2' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a > 2\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 1", collection=True)
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
        find_df = test.find(query="a<3", collection=True)
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
        find_df = test.find(query="a <=  1", collection=True)
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
        find_df = test.find(query="a>=3", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a>=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a>=3\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

def test_find_equality_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a !=  1", collection=True)
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
        find_df = test.find(query="a=3", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a=3\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a =  1", collection=True)
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
        find_df = test.find(query="a ==  2", collection=True)
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

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1, 2)", collection=True)
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
        find_df = test.find(query="a (1,2)", collection=True)
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
        find_df = test.find(query="a (1, 1)", collection=True)
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
        find_df = test.find(query="a(1,1)", collection=True)
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
        find_df = test.find(query="a (1,3)", collection=True)
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
        find_df = test.find(query="a(3,4)", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a(3,4)' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a(3,4)\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2


def test_find_relation_error_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    try:
        test.find(query='"a" > "14"', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: The value in the relational find() cannot be enclosed in double quotes"

    try:
        test.find(query="'a' > 1", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Cannot have a single quote as part of a column name"

    try:
        test.find(query="'a' 1", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == 'find() ERROR: near "a": syntax error'
    
    f = io.StringIO()
    with redirect_stdout(f):
        test.find(query="a 1", collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all instances of 'a 1' in the active backend\n"
    expected_output2 = "WARNING: 'a 1' was not found in this backend\n\n"
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        test.find(query='a ">1"', collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all instances of 'a \">1\"' in the active backend\n"
    expected_output2 = "WARNING: 'a \">1\"' was not found in this backend\n\n"
    assert output == expected_output1 + expected_output2

    try:
        test.find(query='a>"2"', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == 'find() ERROR: The value in the relational find() cannot be enclosed in double quotes'
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > '<4'", collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'a > '<4'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a > '<4'\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g !='>good memories'", collection=True)
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
        test.find(query="a > '<4')", collection=True)
        assert False
    except SystemExit as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap in single quotes."

    try:
        test.find(query="a > <4)", collection=True)
        assert False
    except SystemExit as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap in single quotes."

    try:
        test.find(query="a (1,2))", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]"

    try:
        test.find(query="a (')')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), values must be separated by a comma."

    try:
        test.find(query="a (,", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), it must end with closing parenthesis."

    try:
        test.find(query="a (,')')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == 'find() ERROR: There needs to be two values for the range find. Ex: (1,2)'

    try:
        test.find(query="g !=there's", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Found an unmatched single quote. For apostrophes use 2 single quotes. Ex: he's -> he''s NOT he\"s"

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g == 'there '> \"temperature\" '?'", collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'g == 'there '> \"temperature\" '?'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"g == 'there '> \"temperature\" '?'\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    try:
        test.find(query="g (there is, a place)", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    try:
        test.find(query="g ('there is', a place)", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"

    try:
        test.find(query="g ('there is', 'a place')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Invalid input range: '('there is','a place')'. The lower value must come first."
    
    try:
        test.find(query="g ('there is' 'a place')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), values must be separated by a comma."
    
    try:
        test.find(query="g ('there is', 'a place'", collection=True)
        assert False
    except SystemExit as output:
        print(output)
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place)'", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place'))", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]"

    try:
        test.find(query="g (3,4))", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]"

    try:
        test.find(query="g (,4)", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: There needs to be two values for the range find. Ex: (1,2)"
    
    try:
        test.find(query='a ("hello",6)', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello")', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello", 6)', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find("specification = '!jack'")
    output = f.getvalue()
    assert find_df is None    
    expected_output = textwrap.dedent("""\
        Finding all rows where 'specification = '!jack'' in the active backend\n
        WARNING: 'specification' appeared in more than one table. Can only do a conditional find if 'specification' is in one table
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
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    assert True

def test_error_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    try:
        test.schema(filename="examples/test/yaml1_schema.json")
        test.read(filenames="examples/wildfire/wildfire_google.yml", reader_name='GoogleDatacard') # Unrelated data loaded in after schema
        assert False
    except SystemExit as e:
        expected = "read() ERROR: Users must load all associated data for a schema after loading a complex schema."
        assert str(e) == expected

    try:
        test.schema(filename="examples/test/yaml1_schema.json")
        test.query("SELECT * FROM math") # Querying data but need to load in associated data after loading in schema
        assert False
    except SystemExit as e:
        expected = "ERROR: Cannot query() on an empty backend. Please ensure there is data in it."
        assert str(e) == expected

def test_query_update_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    
    query_df = test.query("SELECT * FROM address", collection=True)  # return output
    query_df['i'] = [123, 234]
    query_df["new_col"] = "test1"

    f = io.StringIO()
    with redirect_stdout(f):
        test.update(query_df)
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])
    expected_output = "WARNING: The data in address's primary key column was edited which could reorder rows in the table."
    assert output == expected_output

    data = test.get_table("address", collection=True)
    assert data['i'].tolist() == [123,234]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_find_update_schema_sqlite_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "Sqlite")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    find_df = test.find(query=2, collection=True)   # return output

    find_df['i'] = list(range(2000, 2000 + len(find_df)))
    find_df['specification'] = list(range(2000, 2000 + len(find_df)))
    find_df["new_col"] = "test1"

    f = io.StringIO()
    with redirect_stdout(f):
        test.update(find_df)
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])
    expected_output = "WARNING: The data in math's primary key column was edited which could reorder rows in the table."
    assert output == expected_output

    data = test.get_table("math", collection=True)
    assert data['specification'].tolist() == [2000,2001]
    assert data['new_col'].tolist() == ["test1", "test1"]

# DUCKDB
# DUCKDB
# DUCKDB

def test_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    assert True

def test_read_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/results.toml", "examples/test/results1.toml"], reader_name='TOML1')
    test.read(filenames="examples/test/yosemite5.csv", reader_name='CSV', table_name = "yosemite") # data table is named yosemite not Csv
    test.read(filenames="examples/test/wildfiredata.csv", reader_name='Ensemble', table_name = "wildfire") # makes a sim table automatically
    test.read(filenames=['examples/test/bueno1.data', 'examples/test/bueno2.data'], reader_name='Bueno')

    test.read(filenames=['examples/wildfire/wildfire_oceans11.yml', 'examples/pennant/pennant_oceans11.yml'], reader_name='Oceans11Datacard')
    test.read(filenames="examples/wildfire/wildfire_dublin_core.xml", reader_name='DublinCoreDatacard')
    test.read(filenames="examples/wildfire/wildfire_schema_org.json", reader_name='SchemaOrgDatacard')
    test.read(filenames="examples/wildfire/wildfire_google.yml", reader_name='GoogleDatacard')
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

    excepted_output = textwrap.dedent("""
    specification | n                 | o       | p   | q       | r  | s                     
    -----------------------------------------------------------------------------------------
    !amy          | 9.800000190734863 | gravity | 23  | home 23 | 1  | -0.0012000000569969416
    !amy1         | 91.80000305175781 | gravity | 233 | home 23 | 12 | -0.012199999764561653 
    """)
    assert output == excepted_output

    query_data = test.query("SELECT * FROM physics", collection=True)
    assert isinstance(query_data, DataFrame)
    assert query_data.columns.tolist() == ['dsi_table_name','specification','n','o','p','q','r','s']
    assert len(query_data) == 2
    assert query_data["n"].tolist() == [9.800000190734863, 91.80000305175781]
    assert query_data["dsi_table_name"][0] == "physics"

def test_query_update_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    query_df = test.query("SELECT * FROM address", collection=True)  # return output
    query_df['i'] = 123
    query_df["new_col"] = "test1"
    test.update(query_df)

    data = test.get_table("address", collection=True)
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
    excepted_output = query_f.getvalue()
    excepted_output = "\n".join(excepted_output.splitlines()[1:])

    assert output == excepted_output

    query_data = test.query("SELECT * FROM physics", collection=True)
    get_data = test.get_table(table_name="physics", collection=True)
    assert query_data.equals(get_data)

def test_find_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        test.find(query=2)
    output = f.getvalue()

    expected_output = "Finding all instances of 2 in the active backend\n" + textwrap.dedent("""
    Table: address
      - Columns: ['specification', 'fileLoc', 'g', 'h', 'i', 'j', 'k', 'l', 'm']
      - Row Number: 1
      - Data: ['!sam', '/home/sam/lib/data', 'good memories', 9.800000190734863, 2, 3, 4, 1.0, 99]
    Table: math
      - Columns: ['specification', 'a', 'b', 'c', 'd', 'e', 'f']
      - Row Number: 1
      - Data: ['!jack', 1, 2, 45.97999954223633, 2, 34.79999923706055, 0.008899999782443047]
    Table: math
      - Columns: ['specification', 'a', 'b', 'c', 'd', 'e', 'f']
      - Row Number: 2
      - Data: ['!jack1', 2, 3, 45.97999954223633, 3, 44.79999923706055, 0.00989999994635582]
    Table: physics
      - Columns: ['specification', 'n', 'o', 'p', 'q', 'r', 's']
      - Row Number: 1
      - Data: ['!amy', 9.800000190734863, 'gravity', 23, 'home 23', 1, -0.0012000000569969416]
    Table: physics
      - Columns: ['specification', 'n', 'o', 'p', 'q', 'r', 's']
      - Row Number: 2
      - Data: ['!amy1', 91.80000305175781, 'gravity', 233, 'home 23', 12, -0.012199999764561653]
    
    """)
    assert output == expected_output

    find_df = test.find(query=2, collection=True)
    assert find_df.columns.tolist()[0] == "dsi_table_name"
    assert find_df["dsi_table_name"][0] == "address"
    assert find_df["dsi_row_index"].tolist() == [1]

def test_find_inequality_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 2", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a > 2' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a > 2\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a > 1", collection=True)
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
        find_df = test.find(query="a<3", collection=True)
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
        find_df = test.find(query="a <=  1", collection=True)
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
        find_df = test.find(query="a>=3", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a>=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a>=3\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

def test_find_equality_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a !=  1", collection=True)
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
        find_df = test.find(query="a=3", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a=3' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a=3\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a =  1", collection=True)
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
        find_df = test.find(query="a ==  2", collection=True)
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

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="a (1, 2)", collection=True)
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
        find_df = test.find(query="a (1,2)", collection=True)
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
        find_df = test.find(query="a (1, 1)", collection=True)
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
        find_df = test.find(query="a(1,1)", collection=True)
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
        find_df = test.find(query="a (1,3)", collection=True)
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
        find_df = test.find(query="a(3,4)", collection=True)
    output = f.getvalue()

    expected_output1 = "Finding all rows where 'a(3,4)' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"a(3,4)\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2


def test_find_relation_error_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    try:
        test.find(query='"a" > "14"', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: The value in the relational find() cannot be enclosed in double quotes"

    try:
        test.find(query="'a' > 1", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Cannot have a single quote as part of a column name"

    try:
        test.find(query="'a' 1", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == 'find() ERROR: Parser Error: syntax error at or near "a"'
    
    f = io.StringIO()
    with redirect_stdout(f):
        test.find(query="a 1", collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all instances of 'a 1' in the active backend\n"
    expected_output2 = "WARNING: 'a 1' was not found in this backend\n\n"
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        test.find(query='a ">1"', collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all instances of 'a \">1\"' in the active backend\n"
    expected_output2 = "WARNING: 'a \">1\"' was not found in this backend\n\n"
    assert output == expected_output1 + expected_output2

    try:
        test.find(query='a>"2"', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == 'find() ERROR: The value in the relational find() cannot be enclosed in double quotes'
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g < '<4'", collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'g < '<4'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"g < '<4'\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g !='>good memories'", collection=True)
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
        test.find(query="a > '<4')", collection=True)
        assert False
    except SystemExit as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap in single quotes."

    try:
        test.find(query="a > <4)", collection=True)
        assert False
    except SystemExit as output:
        first = "find() ERROR: Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]."
        assert str(output) ==  first + " If matching value has an operator in it, make sure to wrap in single quotes."

    try:
        test.find(query="a (1,2))", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]"

    try:
        test.find(query="a (')')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), values must be separated by a comma."

    try:
        test.find(query="a (,", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'a' using (), it must end with closing parenthesis."

    try:
        test.find(query="a (,')')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == 'find() ERROR: There needs to be two values for the range find. Ex: (1,2)'

    try:
        test.find(query="g !=there's", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Found an unmatched single quote. For apostrophes use 2 single quotes. Ex: he's -> he''s NOT he\"s"

    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find(query="g == 'there '> \"temperature\" '?'", collection=True)
    output = f.getvalue()
    expected_output1 = "Finding all rows where 'g == 'there '> \"temperature\" '?'' in the active backend\n\n"
    expected_output2 = "WARNING: Could not find any rows where \"g == 'there '> \"temperature\" '?'\" in this backend.\n\n"
    assert find_df is None
    assert output == expected_output1 + expected_output2

    try:
        test.find(query="g (there is, a place)", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    try:
        test.find(query="g ('there is', a place)", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"

    try:
        test.find(query="g ('there is', 'a place')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Invalid input range: '('there is','a place')'. The lower value must come first."
    
    try:
        test.find(query="g ('there is' 'a place')", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), values must be separated by a comma."
    
    try:
        test.find(query="g ('there is', 'a place'", collection=True)
        assert False
    except SystemExit as output:
        print(output)
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place)'", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: When applying a range-based find on 'g' using (), it must end with closing parenthesis."
    
    try:
        test.find(query="g ('there is', 'a place'))", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]"

    try:
        test.find(query="g (3,4))", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Can only apply one operation per find. Inequality [<,>,<=,>=,!=], equality [=,==], or range [()]"

    try:
        test.find(query="g (,4)", collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: There needs to be two values for the range find. Ex: (1,2)"
    
    try:
        test.find(query='a ("hello",6)', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello")', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Neither value in the range-based find can be enclosed in double quotes. Only single quotes"

    try:
        test.find(query='a (6, "hello", 6)', collection=True)
        assert False
    except SystemExit as output:
        assert str(output) == "find() ERROR: Range-based finds require multi-word values to be enclosed in single quotes"
    
    f = io.StringIO()
    with redirect_stdout(f):
        find_df = test.find("specification = '!jack'")
    output = f.getvalue()
    assert find_df is None
    expected_output = textwrap.dedent("""\
        Finding all rows where 'specification = '!jack'' in the active backend\n
        WARNING: 'specification' appeared in more than one table. Can only do a conditional find if 'specification' is in one table
        Try using `dsi.query()` to retrieve the matching rows for a specific table
        These are recommended inputs for query():
         - SELECT * FROM address WHERE specification = '!jack'
         - SELECT * FROM math WHERE specification = '!jack'
         - SELECT * FROM physics WHERE specification = '!jack'\n""")
    assert output == expected_output

def test_find_update_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")

    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    find_df = test.find(query=2, collection=True)   # return output

    find_df['i'] = list(range(2000, 2000 + len(find_df)))
    find_df['b'] = list(range(2000, 2000 + len(find_df)))
    find_df["new_col"] = "test1"
    test.update(find_df)

    data = test.get_table("address", collection=True)
    assert data['i'].tolist() == [2000,3]
    assert data['new_col'].tolist() == ["test1", None]

def test_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    assert True

def test_query_update_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')
    
    query_df = test.query("SELECT * FROM math", collection=True)  # return output
    query_df['specification'] = [123, 234]
    query_df["new_col"] = "test1"
    f = io.StringIO()
    with redirect_stdout(f):
        test.update(query_df)
    output = f.getvalue()
    output = "\n".join(output.splitlines()[1:])
    expected_output = "WARNING: The data in math's primary key column was edited which could reorder rows in the table."
    assert output == expected_output

    data = test.get_table("math", collection=True)
    assert data['specification'].tolist() == [123,234]
    assert data['new_col'].tolist() == ["test1", "test1"]

def test_find_update_schema_duckdb_backend():
    dbpath = 'data.db'
    if os.path.exists(dbpath):
        os.remove(dbpath)

    test = DSI(filename=dbpath, backend_name= "DuckDB")
    test.schema(filename="examples/test/yaml1_schema.json")
    test.read(filenames=["examples/test/student_test1.yml", "examples/test/student_test2.yml"], reader_name='YAML1')

    find_df = test.find(query=2, collection=True)   # return output

    find_df['i'] = list(range(2000, 2000 + len(find_df)))
    find_df['b'] = list(range(2000, 2000 + len(find_df)))
    find_df["new_col"] = "test1"
    
    try:
        test.update(find_df)
    except SystemExit as e:
        output = str(e)
    expected_output1 = "update() ERROR: Data in 'b', the foreign key of 'math', must match 'i', the primary key of 'address'. "
    expected_output2 = "Please ensure that all rows in 'math' are updated"
    assert output == expected_output1+expected_output2
    
    data = test.get_table("address", collection=True)
    assert data['i'].tolist() == [2,3]