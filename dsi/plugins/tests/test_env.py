import collections

from dsi.plugins.env import HostnamePlugin, EnvProvPlugin

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

def test_envprov_plugin_type():
    plug = EnvProvPlugin()
    plug.add_row()

    #for key in plug.output_collector:
    #    print(key, ":", plug.output_collector[key])
    #print(len(plug.output_collector))

    assert type(plug.output_collector) == collections.OrderedDict

def test_envprov_plugin_kver_type():
    plug = EnvProvPlugin()
    plug.add_row()

    k_ver = plug.output_collector["kernel version"][0]
    assert type(k_ver) == str

def test_envprov_plugin_adds_rows():
    plug = EnvProvPlugin()
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

def test_envprov_plugin_has_many_columns():
    plug = EnvProvPlugin()
    plug.add_row()

    assert len(plug.output_collector.keys()) > 100 # should def have more than 100
