# examples/user/7.schema.py
from dsi.dsi import DSI

schema_dsi = DSI()

# dsi.schema(filename)
schema_dsi.schema("../test/example_schema.json")

#dsi.read(filename, reader)
schema_dsi.read("../test/student_test1.yml", 'YAML1')

#dsi.write(filename, writer)
schema_dsi.write("schema_er_diagram.png", "ER_Diagram")

schema_dsi.close()