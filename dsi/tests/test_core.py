from dsi.core import Terminal


def test_terminal_module_getter():
    a = Terminal()
    plugins = a.list_available_modules('plugin')
    drivers = a.list_available_modules('driver')
    assert len(plugins) > 0 and len(drivers) > 0


def test_unload_module():
    a = Terminal()
    a.load_module('plugin', 'GitInfo', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 1
    a.unload_module('plugin', 'GitInfo', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 0
