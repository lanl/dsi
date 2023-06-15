import collections

from dsi.plugins.env import HostnamePlugin

def test_hostname_plugin_type():
    a = HostnamePlugin()
    a.add_row()
    a.add_row()
    assert type(a.output_collector)==collections.OrderedDict

def test_hostname_plugin_col_shape():
    a = HostnamePlugin()
    a.add_row()
    a.add_row()
    assert len(a.output_collector.keys())==len(a.output_collector.values())

def test_hostname_plugin_row_shape():
    for row_cnt in range(1,10):
        a = HostnamePlugin()
        for _ in range(row_cnt):
            a.add_row()
        column_values = list(a.output_collector.values())
        row_shape = len(column_values[0])
        for col in column_values[1:]:
            assert len(col) == row_shape == row_cnt
