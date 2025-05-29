# examples/developer/find.py
from dsi.core import Terminal

terminal_find = Terminal()

# Run Example 2 so data.db is not empty and data can then be found here
terminal_find.load_module('backend','Sqlite','back-write', filename='data.db')

## TABLE match                      - return matching table data
data = terminal_find.find_table("people")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## COLUMN match                     - return matching column data
data = terminal_find.find_column("a")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## RANGE match (range = True)       - return [min, max] of matching cols
data = terminal_find.find_column("avg", range = True)
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## CELL match                       - return the cells which match the search term
data = terminal_find.find_cell(5.5)
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## ROW match (row_return = True)    - return the rows where cells match the search term
data = terminal_find.find_cell(5.9, row = True)
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)

## ALL match                        - find all instances where the search term is found: table, column, cell
data = terminal_find.find("a")
for val in data:
    print(val.t_name, val.c_name, val.value, val.row_num, val.type)