from dsi.core import Terminal


def test_terminal_module_getter():
    a = Terminal()
    plugins = a.list_available_modules('plugin')
    backends = a.list_available_modules('backend')
    assert len(plugins) > 0 and len(backends) > 0


def test_unload_module():
    a = Terminal()
    a.load_module('plugin', 'GitInfo', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 1
    a.unload_module('plugin', 'GitInfo', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 0


def test_unload_after_transload_fails():
    a = Terminal()
    a.load_module('plugin', 'Hostname', 'producer')
    a.transload()
    a.unload_module('plugin', 'Hostname', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 1
