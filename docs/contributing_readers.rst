====================
Contributing Readers
====================

DSI readers are the primary way to transform outside data to metadata that DSI can ingest. 

Readers are Python classes that must include a few methods, namely `__init__`, `pack_header`, and `add_row`.

`__init__(self) -> None: # may include other parameters`
--------------------------------------------------------
`__init__` is where you can include all of your initialization logic, just make sure to initialize your superclass.

Example `__init__`: ::

  def __init__(self) -> None:
    super().__init__()  # see "plugins" to determine which superclass your reader should extend

`pack_header(self) -> None`
----------------------------

`pack_header` is responsible for setting a schema, registering which columns 
will be populated by the reader. The `set_schema(self, column_names: list, validation_model=None) -> None` method 
is available to subclasses of `StructuredMetadata`, which allows one to simply give a list of column names to register. 
`validation_model` is an pydantic model that can help you enforce types, but is completely optional.

Example `pack_header` ::

  def pack_header(self) -> None:
    column_names = ["foo", "bar", "baz"]
    self.set_schema(column_names)

`add_row(self) -> None`
------------------------

`add_row` is responsible for appending to the internal metadata buffer. 
Whatever data is being ingested, it's done here. The `add_to_output(self, row: list) -> None` method is available to subclasses 
of `StructuredMetadata`, which takes a list of data that matches the schema and appends it to the internal metadata buffer.

Note: `pack_header` must be called before metadata is appended in `add_row`. Another helper method of 
`StructuredMetadata` is `schema_is_set`, which provides a way to tell if this restriction is met.

Example `add_row` ::

  def add_row(self) -> None:
    if not self.schema_is_set():
      self.pack_header()

    # data parsing can go here (or abstracted to other functions)
    my_data = [1, 2, 3]

    self.add_to_output(my_data)

Implemented Examples
---------------------
If you want to see some full reader examples in-code, some can be found in 
`dsi/dsi/plugins/env.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/env.py>`_.
`Hostname` is an especially simple example to go off of. 
