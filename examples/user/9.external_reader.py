# examples/user/9.external_reader.py
from dsi.dsi import DSI

external_dsi = DSI("external_data.db")

#dsi.read(filename, path/to/custom/dsi/reader.py)
external_dsi.read("../test/test.txt", "../test/text_file_reader.py")

#dsi.display(table_name)
external_dsi.display("people")

external_dsi.close()