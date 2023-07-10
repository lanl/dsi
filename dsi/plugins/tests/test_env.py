import collections

from dsi.plugins.env import Hostname, SystemKernel, Bueno, GitInfo
import git


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_hostname_plugin_type():
    a = Hostname()
    a.add_row()
    a.add_row()
    assert type(a.output_collector) == collections.OrderedDict


def test_hostname_plugin_col_shape():
    a = Hostname()
    a.add_row()
    a.add_row()
    assert len(a.output_collector.keys()) == len(a.output_collector.values())


def test_hostname_plugin_row_shape():
    for row_cnt in range(1, 10):
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

    # should def have more than 100 columns
    assert len(plug.output_collector.keys()) > 100


def test_bueno_plugin_type():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    plug = Bueno(filename=path)
    plug.add_row()
    assert type(plug.output_collector) == collections.OrderedDict


def test_bueno_plugin_adds_rows():
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    plug = Bueno(filename=path)
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    # 3 Bueno cols + 4 inherited Env cols
    assert len(plug.output_collector.keys()) == 7


def test_git_plugin_type():
    root = get_git_root('.')
    plug = GitInfo(git_repo_path=root)
    plug.add_row()
    assert type(plug.output_collector) == collections.OrderedDict


def test_git_plugin_adds_rows():
    root = get_git_root('.')
    plug = GitInfo(git_repo_path=root)
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    # 2 Git cols + 4 inherited Env cols
    assert len(plug.output_collector.keys()) == 6


def test_git_plugin_infos_are_str():
    root = get_git_root('.')
    plug = GitInfo(git_repo_path=root)
    plug.add_row()

    assert type(plug.output_collector["git_remote"][0]) == str
    assert type(plug.output_collector["git_commit"][0]) == str
