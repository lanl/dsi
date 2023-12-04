from dsi.core import Terminal
from collections import OrderedDict
import git

from dsi.plugins.file_consumer import Bueno, Csv


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_bueno_plugin_type():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno1.data'])
    plug = Bueno(filenames=path)
    plug.add_rows()
    assert type(plug.output_collector) == OrderedDict


def test_bueno_plugin_adds_rows():
    path1 = '/'.join([get_git_root('.'), 'dsi/data', 'bueno1.data'])
    path2 = '/'.join([get_git_root('.'), 'dsi/data', 'bueno2.data'])
    plug = Bueno(filenames=[path1, path2])
    plug.add_rows()
    plug.add_rows()

    for key, val in plug.output_collector.items():
        assert len(val) == 4  # two lists of length 4

    # 4 Bueno cols
    assert len(plug.output_collector.keys()) == 4


def test_csv_plugin_type():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])
    plug = Csv(filenames=path)
    plug.add_rows()
    assert type(plug.output_collector) == OrderedDict


def test_csv_plugin_adds_rows():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])
    plug = Csv(filenames=path)
    plug.add_rows()

    for key, val in plug.output_collector.items():
        assert len(val) == 4

    # 11 Csv cols + 1 inherited FileConsumer cols
    assert len(plug.output_collector.keys()) == 12


def test_csv_plugin_adds_rows_multiple_files():
    path1 = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])
    path2 = '/'.join([get_git_root('.'), 'dsi/data', 'yosemite5.csv'])

    plug = Csv(filenames=[path1, path2])
    plug.add_rows()

    for key, val in plug.output_collector.items():
        assert len(val) == 8

    # 13 Csv cols + 2 inherited FileConsumer cols
    assert len(plug.output_collector.keys()) == 15
    
    
def test_csv_plugin_adds_rows_multiple_files_strict_mode():
    path1 = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])
    path2 = '/'.join([get_git_root('.'), 'dsi/data', 'yosemite5.csv'])

    plug = Csv(filenames=[path1, path2], strict_mode = True)
    plug.add_rows()

    assert len(plug.output_collector.keys()) == 0


def test_csv_plugin_leaves_active_metadata_wellformed():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])

    term = Terminal()
    term.load_module('plugin', 'Csv', 'consumer', filenames=[path])
    term.load_module('plugin', 'Hostname', 'producer')
    term.transload()

    columns = list(term.active_metadata.values())
    assert all([len(columns[0]) == len(col)
               for col in columns])  # all same length
