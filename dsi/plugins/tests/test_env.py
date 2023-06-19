import collections

from dsi.plugins.env import Hostname, SystemKernel

def test_hostname_plugin_type():
    a = Hostname()
    a.add_row()
    a.add_row()
    assert type(a.output_collector)==collections.OrderedDict

def test_hostname_plugin_col_shape():
    a = Hostname()
    a.add_row()
    a.add_row()
    assert len(a.output_collector.keys())==len(a.output_collector.values())

def test_hostname_plugin_row_shape():
    for row_cnt in range(1,10):
        a = Hostname()
        for _ in range(row_cnt):
            a.add_row()
        column_values = list(a.output_collector.values())
        row_shape = len(column_values[0])
        for col in column_values[1:]:
            assert len(col) == row_shape == row_cnt

def test_envprov_plugin_type():
    plug = SystemKernel()
    assert type(plug.output_collector) == collections.OrderedDict

def test_envprov_plugin_adds_rows():
    plug = SystemKernel()
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    assert len(plug.output_collector.keys()) > 100 # should def have more than 100 columns

