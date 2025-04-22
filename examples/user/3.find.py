from dsi.core import DSI

a = DSI()
a.open('data.db')
data = a.findt('people')
for val in data:
   print(val.t_name, val.c_name, val.value, val.row_num, val.type)

data = a.findc('std_gravity')
for val in data:
   print(val.t_name, val.c_name, val.value, val.row_num, val.type)

data = a.find('5.5')
for val in data:
   print(val.t_name, val.c_name, val.value, val.row_num, val.type)

