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

    # 11 Csv cols
    assert len(plug.output_collector["Csv"].keys()) == 11

def test_csv_plugin_adds_rows_multiple_files():
    path1 = '/'.join([get_git_root('.'), 'examples/data', 'wildfiredata.csv'])
    path2 = '/'.join([get_git_root('.'), 'examples/data', 'yosemite5.csv'])

    plug = Csv(filenames=[path1, path2])
    plug.add_rows()

    for key, val in plug.output_collector["Csv"].items():
        assert len(val) == 8

    # 13 Csv cols
    assert len(plug.output_collector["Csv"].keys()) == 13

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
    
def test_yaml_reader():
    a=Terminal()
    a.load_module('plugin', 'YAML1', 'reader', filenames=["examples/data/student_test1.yml", "examples/data/student_test2.yml"], target_table_prefix = "student")
    a.transload()

    assert len(a.active_metadata.keys()) == 4 # 4 tables - math, address, physics, dsi_units
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        numRows = 2
        if name == "dsi_units":
            continue
        assert all(len(colData) == numRows for colData in tableData.values())

def test_toml_reader():
    a=Terminal()
    a.load_module('plugin', 'TOML1', 'reader', filenames="examples/data/results.toml", target_table_prefix = "results")
    a.transload()

    assert len(a.active_metadata.keys()) == 2 # 2 tables - people and dsi_units
    for name, tableData in a.active_metadata.items():
        assert isinstance(tableData, OrderedDict)
        if name == "dsi_units":
            continue
        numRows = 1
        assert all(len(colData) == numRows for colData in tableData.values())

def test_schema_reader():
    a=Terminal()
    a.load_module('plugin', 'Schema', 'reader', filename="examples/data/example_schema.json" , target_table_prefix = "student")
    a.transload()

    assert len(a.active_metadata.keys()) == 1
    assert "dsi_relations" in a.active_metadata.keys()
    for tableData in a.active_metadata.values():
        assert isinstance(tableData, OrderedDict)
        assert len(tableData["primary_key"]) == len(tableData["foreign_key"])