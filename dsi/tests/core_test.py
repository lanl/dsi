from dsi import core

## Use fluentd as inspiration
#core.add_plugin('my_plugin.py')
#core.activate_plugin('my_plugin')
#core.list_available_plugins()
#core.activate_plugin('default1')
#core.list_loaded_plugins()
#data1 = core.post_process['my_plugin'](foo='bar')
## Not in backend, loaded into python native data structures.
#
#core.add_driver('my_driver.py')
## Unimplemented back-end gives error
#core.store['my_driver'](foo='bar')
## Output: Error, not implemented.
#query1 = core.build_query('SELECT * FROM collection WHERE foo>1')
## Hidden methods of core intialize front-end driver, perform linting, return DSI query object
## DSI query object may be a SQLAlchemy object wrapper.
#core.query(data1, query1)
