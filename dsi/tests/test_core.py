from dsi.core import Terminal


def test_terminal_module_getter():
    a = Terminal()
    plugins = a.list_available_modules('plugin')
    drivers = a.list_available_modules('driver')
    assert len(plugins) > 0 and len(drivers) > 0
