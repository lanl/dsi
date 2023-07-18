import git
from dsi.core import Terminal


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_terminal_module_getter():
    a = Terminal()
    plugins = a.list_available_modules('plugin')
    drivers = a.list_available_modules('driver')
    assert len(plugins) > 0 and len(drivers) > 0


def test_unload_module():
    a = Terminal()
    a.load_module('plugin', 'GitInfo', 'producer',
                  git_repo_path=get_git_root('.'))
    assert len(a.list_loaded_modules()['producer']) == 1
    a.unload_module('plugin', 'GitInfo', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 0


def test_unload_after_transload_fails():
    a = Terminal()
    a.load_module('plugin', 'Hostname', 'producer')
    a.transload()
    a.unload_module('plugin', 'Hostname', 'producer')
    assert len(a.list_loaded_modules()['producer']) == 1
