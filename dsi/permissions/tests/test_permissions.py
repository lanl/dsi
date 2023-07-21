import git
import os
from glob import glob

from dsi.core import Terminal
from dsi.permissions.permissions import PermissionsManager


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return (git_root)


def test_multiple_files_different_perms():
    term = Terminal()
    bueno_path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    os.chmod(bueno_path, 0o664)
    term.load_module('plugin', 'Bueno', 'consumer', filename=bueno_path)

    term.transload()

    for env_col in ('uid', 'effective_gid', 'moniker', 'gid_list'):
        uid, gid, settings = tuple(term.perms_manager.column_perms[env_col])
        assert type(uid) == type(gid) == int
        assert settings == '0o660'

    for bueno_col in ('foo', 'bar', 'baz'):
        uid, gid, settings = tuple(term.perms_manager.column_perms[bueno_col])
        assert type(uid) == type(gid) == int
        assert settings == '0o664'


def test_output_file_mapping():
    term = Terminal()
    bueno_path = '/'.join([get_git_root('.'), 'dsi/data', 'bueno.data'])
    os.chmod(bueno_path, 0o664)

    term.load_module('plugin', 'Bueno', 'consumer', filename=bueno_path)
    term.transload()

    pq_path = '/'.join([get_git_root('.'), 'dsi/data', 'dummy_data.pq'])
    term.load_module('driver', 'Parquet', 'back-end', filename=pq_path)

    term.artifact_handler(interaction_type='put')

    pm = PermissionsManager()
    written_paths = glob(
        "/".join([get_git_root('.'), 'dsi/data']) + "/dummy_data*.pq")
    for path in written_paths:
        uid, gid, settings = pm.get_file_permissions(path)
        assert path.find(str(uid)) != -1 and \
            path.find(str(gid)) != -1 and \
            path.find(settings) != -1
