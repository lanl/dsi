# examples/user/7.schema.py
from dsi.dsi import DSI

schema_dsi = DSI()

# loads a complex schema into DSI to apply to a database
#dsi.read(filename, reader)
schema_dsi.schema("../data/example_schema.json") # view comments in dsi/data/example_schema.json on how to structure it
schema_dsi.read("../data/student_test1.yml", 'YAML1')

#dsi.write(filename, writer)
schema_dsi.write("schema_er_diagram.png", "ER_Diagram")

schema_dsi.close()

# DSI without a complex Schema
basic_dsi = DSI()

#dsi.read(filename, reader)
basic_dsi.read("../data/student_test1.yml", 'YAML1')

#dsi.write(filename, writer)
basic_dsi.write("normal_er_diagram.png", "ER_Diagram") # schema_er_diagram.png will be different due to complex schema

basic_dsi.close()