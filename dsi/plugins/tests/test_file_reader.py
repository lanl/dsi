from dsi.core import Terminal
from collections import OrderedDict
import git

from dsi.plugins.file_reader import JSON, Bueno, Csv


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_bueno_plugin_type():
    path = '/'.join([get_git_root('.'), 'examples/data', 'bueno1.data'])
    plug = Bueno(filenames=path)
    plug.add_rows()
    assert type(plug.output_collector) == OrderedDict


def test_bueno_plugin_adds_rows():
    path1 = '/'.join([get_git_root('.'), 'examples/data', 'bueno1.data'])
    path2 = '/'.join([get_git_root('.'), 'examples/data', 'bueno2.data'])
    plug = Bueno(filenames=[path1, path2])
    plug.add_rows()
    plug.add_rows()

    for key, val in plug.output_collector["Bueno"].items():
        assert len(val) == 4  # two lists of length 4

    # 4 Bueno cols
    assert len(plug.output_collector["Bueno"].keys()) == 4

def test_json_plugin_adds_rows():
    path1 = '/'.join([get_git_root('.'), 'examples/data', 'bueno1.data'])
    path2 = '/'.join([get_git_root('.'), 'examples/data', 'bueno2.data'])
    plug = JSON(filenames=[path1, path2])
    plug.add_rows()
    for key, val in plug.output_collector["JSON"].items():
        assert len(val) == 2  # two lists of length 4

    # 4 Bueno cols
    assert len(plug.output_collector["JSON"].keys()) == 4

def test_csv_plugin_type():
    path = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])
    plug = Csv(filenames=path)
    plug.add_rows()
    assert type(plug.output_collector) == OrderedDict

def test_csv_plugin_adds_rows():
    path = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])
    plug = Csv(filenames=path)
    plug.add_rows()

    for key, val in plug.output_collector["Csv"].items():
        assert len(val) == 4

    # 11 Csv cols + 1 inherited FileReader cols
    assert len(plug.output_collector["Csv"].keys()) == 12

def test_csv_plugin_adds_rows_multiple_files():
    path1 = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])
    path2 = '/'.join([get_git_root('.'), 'examples/data', 'yosemite5.csv'])

    plug = Csv(filenames=[path1, path2])
    plug.add_rows()

    for key, val in plug.output_collector["Csv"].items():
        assert len(val) == 8

    # 13 Csv cols + 2 inherited FileReader cols
    assert len(plug.output_collector["Csv"].keys()) == 15

def test_csv_plugin_adds_rows_multiple_files_strict_mode():
    path1 = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])
    path2 = '/'.join([get_git_root('.'), 'examples/data', 'yosemite5.csv'])

    plug = Csv(filenames=[path1, path2], strict_mode=True)
    try:
        plug.add_rows()
    except TypeError:
        # Strict mode will throw TypeError if enabled and csv headers don't match
        assert True

def test_csv_plugin_leaves_active_metadata_wellformed():
    path = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])

    term = Terminal()
    term.load_module('plugin', 'Csv', 'reader', filenames=[path])
    #term.load_module('plugin', 'Hostname', 'writer')
    term.transload()

    columns = list(term.active_metadata["Csv"].values())
    assert all([len(columns[0]) == len(col)
               for col in columns])  # all same length