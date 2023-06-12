import collections

from dsi.plugins.env import HostnamePlugin

def test_hostnameplugin_type():
    a = HostnamePlugin()
    a.parse()
    a.add_row()
    a.add_row()
    assert type(a.output_collector)==collections.OrderedDict

def test_hostname_plugin_col_shape():
    a = HostnamePlugin()
    a.parse()
    a.add_row()
    a.add_row()
    assert len(a.output_collector.keys())==len(a.output_collector.values())
