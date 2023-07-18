import git
import os

from dsi.core import Terminal


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_multiple_files_different_perms():
    term = Terminal()
    bueno_path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    pq_path = '/'.join([get_git_root('.'), 'dsi/data', 'dummy_data.pq'])
    os.chmod(bueno_path, 0o660)
    os.chmod(pq_path, 0o664)

    term.load_module('plugin', 'Bueno', 'consumer', filename=bueno_path)
    term.load_module('driver', 'Parquet', 'back-end', filename=pq_path)
    term.artifact_handler(interaction_type='get')
    term.transload()

    for env_col in ('uid', 'effective_gid', 'moniker', 'gid_list'):
        assert tuple(term.perms_manager.column_perms[env_col]) == \
            (None, None, None)

    for bueno_col in ('foo', 'bar', 'baz'):
        uid, gid, settings = tuple(term.perms_manager.column_perms[bueno_col])
        assert type(uid) == type(gid) == int
        assert settings == '0o660'

    for pq_col in ('one', 'two', 'three'):
        uid, gid, settings = tuple(term.perms_manager.column_perms[pq_col])
        assert type(uid) == type(gid) == int
        assert settings == '0o664'
