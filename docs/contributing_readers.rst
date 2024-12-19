====================
Contributing Readers
====================

DSI readers are the primary way to transform outside data to metadata that DSI can ingest. 

Readers are Python classes that must include a few methods, `pack_header` and `add_row`.


  * `pack_header(self) -> None`: `pack_header` is responsible for setting a schema, registering which columns 
    will be populated by the reader. The `set_schema` method is available to classes that extend `StructuredMetadata`, 
    which allows one to simply give a list of column names (`list[str]`) to register. 
  * `add_row(self) -> None`: `add_row` is responsible for appending to the internal metadata buffer. 
    Whatever data is being ingested, it's done here. The `add_to_output` method is available to classes 
    that extend `StructuredMetadata`, which takes a list of data that matches the schema (`list[any]`) 
    and appends it to the internal metadata buffer.
  * Note: `pack_header` must be called before metadata is appended in `add_row`. Another helper method of 
    `StructuredMetadata` is `schema_is_set`, which provides a way to tell if this restriction is met.
    The following can be added to `add_row` to easily satisfy this: `if not self.schema_is_set(): self.pack_header()`.

Examples
---------
Some example readers can be found in `dsi/dsi/plugins/env.py <https://github.com/lanl/dsi/blob/main/dsi/plugins/env.py>`_.
`Hostname` is an especially simple example to go off of. 
