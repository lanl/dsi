# examples/developer/6.find.py
from dsi.core import Terminal

terminal_find = Terminal()

# Run 3.schema.py so schema_data.db is not empty
terminal_find.load_module('backend','Sqlite','back-write', filename='schema_data.db')

## TABLE match                      - return matching table data
data = terminal_find.find_table("input")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## COLUMN match                     - return matching column data
data = terminal_find.find_column("density")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## RANGE match (range = True)       - return [min, max] of matching cols
data = terminal_find.find_column("density", range = True)
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## CELL match                       - return the cells which match the search term
data = terminal_find.find_cell("rectangle")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## ROW match (row_return = True)    - return the rows where cells match the search term
data = terminal_find.find_cell("rectangle", row = True)
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## ALL match                        - find all instances where the search term is found: table, column, cell
data = terminal_find.find("Jun")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)