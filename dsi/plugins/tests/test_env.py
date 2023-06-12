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
    plug.parse()
    assert type(plug.output_collector) == collections.OrderedDict

def test_envprov_plugin_kver_type():
    plug = EnvProvPlugin()
    plug.parse()
    k_ver = plug.output_collector["kernel version"]
    assert type(k_ver) == str
