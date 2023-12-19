import collections

from dsi.plugins.env import Hostname, SystemKernel, Bueno, GitInfo
from dsi.permissions.permissions import PermissionsManager
import git
from json import loads


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_hostname_plugin_type():
    mock_pm = PermissionsManager()
    a = Hostname(perms_manager=mock_pm)
    a.add_row()
    a.add_row()
    assert type(a.output_collector) == collections.OrderedDict


def test_hostname_plugin_col_shape():
    mock_pm = PermissionsManager()
    a = Hostname(perms_manager=mock_pm)
    a.add_row()
    a.add_row()
    assert len(a.output_collector.keys()) == len(a.output_collector.values())


def test_hostname_plugin_row_shape():
    for row_cnt in range(1, 10):
        mock_pm = PermissionsManager()
        a = Hostname(perms_manager=mock_pm)
        for _ in range(row_cnt):
            a.add_row()
        column_values = list(a.output_collector.values())
        row_shape = len(column_values[0])
        for col in column_values[1:]:
            assert len(col) == row_shape == row_cnt


def test_envprov_plugin_type():
    mock_pm = PermissionsManager()
    plug = SystemKernel(perms_manager=mock_pm)
    assert type(plug.output_collector) == collections.OrderedDict


def test_envprov_plugin_adds_rows():
    mock_pm = PermissionsManager()
    plug = SystemKernel(perms_manager=mock_pm)
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    # 1 SystemKernel column + 4 inherited Env cols
    assert len(plug.output_collector.keys()) == 5


def test_systemkernel_plugin_blob_is_big():
    plug = SystemKernel()
    plug.add_row()

    blob = plug.output_collector["kernel_info"][0]
    info_dict = loads(blob)

    # dict should have more than 1000 (~7000) keys
    assert len(info_dict.keys()) > 1000


def test_bueno_plugin_type():
    mock_pm = PermissionsManager()
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    plug = Bueno(filename=path, perms_manager=mock_pm)
    plug.add_row()
    assert type(plug.output_collector) == collections.OrderedDict


def test_bueno_plugin_adds_rows():
    mock_pm = PermissionsManager()
    path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    plug = Bueno(filename=path, perms_manager=mock_pm)
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    # 3 Bueno cols + 4 inherited Env cols
    assert len(plug.output_collector.keys()) == 7


def test_git_plugin_type():
    mock_pm = PermissionsManager()
    root = get_git_root('.')
    plug = GitInfo(git_repo_path=root, perms_manager=mock_pm)
    plug.add_row()
    assert type(plug.output_collector) == collections.OrderedDict


def test_git_plugin_adds_rows():
    mock_pm = PermissionsManager()
    root = get_git_root('.')
    plug = GitInfo(git_repo_path=root, perms_manager=mock_pm)
    plug.add_row()
    plug.add_row()

    for key, val in plug.output_collector.items():
        assert len(val) == 2

    # 2 Git cols + 4 inherited Env cols
    assert len(plug.output_collector.keys()) == 6


def test_git_plugin_infos_are_str():
    mock_pm = PermissionsManager()
    root = get_git_root('.')
    plug = GitInfo(git_repo_path=root, perms_manager=mock_pm)
    plug.add_row()

    assert type(plug.output_collector["git_remote"][0]) == str
    assert type(plug.output_collector["git_commit"][0]) == str
