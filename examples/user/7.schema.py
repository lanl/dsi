# examples/user/7.schema.py
from dsi.dsi import DSI

schema_dsi = DSI()

# loads a complex schema into DSI to apply to a database
schema_dsi.read(filenames="../data/example_schema.json", reader_name="Schema") # view comments in dsi/data/example_schema.json to learn how to structure it
schema_dsi.read(filenames="../data/student_test1.yml", reader_name='YAML1')

schema_dsi.write(filename="schema_er_diagram.png", writer_name="ER_Diagram")

schema_dsi.close()

# DSI without a complex Schema
basic_dsi = DSI()

basic_dsi.read(filenames="../data/student_test1.yml", reader_name='YAML1')

basic_dsi.write(filename="normal_er_diagram.png", writer_name="ER_Diagram") # schema_er_diagram.png will be different due to complex schema

basic_dsi.close()