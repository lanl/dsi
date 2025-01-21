from dsi.core import Terminal


def test_terminal_module_getter():
    a = Terminal()
    plugins = a.list_available_modules('plugin')
    backends = a.list_available_modules('backend')
    assert len(plugins) > 0 and len(backends) > 0


def test_unload_module():
    a = Terminal()
    a.load_module('plugin', 'GitInfo', 'writer')
    assert len(a.list_loaded_modules()['writer']) == 1
    a.unload_module('plugin', 'GitInfo', 'writer')
    assert len(a.list_loaded_modules()['writer']) == 0


def test_unload_after_transload_fails():
    a = Terminal()
    a.load_module('plugin', 'Hostname', 'reader')
    # a.transload()
    # a.unload_module('plugin', 'Hostname', 'reader')
    assert len(a.active_metadata) > 0