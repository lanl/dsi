from collections import OrderedDict
import git

from dsi.plugins.file_consumer import Bueno, CSV


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_bueno_plugin_type():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    plug = Bueno(filename=path)
    plug.add_row()
    assert type(plug.output_collector) == OrderedDict


def test_bueno_plugin_adds_rows():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    plug = Bueno(filename=path)
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    # 3 Bueno cols + 2 inherited FileConsumer cols
    assert len(plug.output_collector.keys()) == 5


def test_csv_plugin_type():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])
    plug = CSV(filename=path)
    plug.add_row()
    assert type(plug.output_collector) == OrderedDict


def test_csv_plugin_adds_rows():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'wildfiredata.csv'])
    plug = CSV(filename=path)
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 4

    # 11 CSV cols + 2 inherited FileConsumer cols
    assert len(plug.output_collector.keys()) == 13
