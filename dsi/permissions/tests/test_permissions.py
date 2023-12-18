import git
import os
from glob import glob

from dsi.core import Terminal
from dsi.permissions.permissions import PermissionsManager


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return git_root


def print_and_y(s):
    print(s)
    return "y"


def test_multiple_perms_fails_by_default(monkeypatch):
    monkeypatch.setattr("builtins.input", print_and_y)  # mock input
    term = Terminal()
    bueno_path = "/".join([get_git_root("."), "dsi/data", "bueno1.data"])
    os.chmod(bueno_path, 0o664)
    term.load_module("plugin", "Bueno", "consumer", filenames=bueno_path)
    term.load_module("plugin", "Hostname", "consumer")
    term.transload()
    pq_path = "/".join([get_git_root("."), "dsi/data", "dummy_data.pq"])
    term.load_module("driver", "Parquet", "back-end", filename=pq_path)
    assert not term.artifact_handler(interaction_type="put")


def test_multiple_permissions_register_correctly(monkeypatch):
    monkeypatch.setattr("builtins.input", print_and_y)  # mock input
    term = Terminal(allow_multiple_permissions=True)
    bueno_path = "/".join([get_git_root("."), "dsi/data", "bueno1.data"])
    os.chmod(bueno_path, 0o664)
    term.load_module("plugin", "Bueno", "consumer", filenames=bueno_path)
    term.load_module("plugin", "Hostname", "consumer")

    term.transload()

    for env_col in ("uid", "effective_gid", "moniker", "gid_list"):
        uid, gid, settings = tuple(term.perms_manager.column_perms[env_col])
        assert isinstance(uid, int)
        assert isinstance(gid, int)
        assert settings == "0o660"

    for bueno_col in ("foo", "bar", "baz"):
        uid, gid, settings = tuple(term.perms_manager.column_perms[bueno_col])
        assert isinstance(uid, int)
        assert isinstance(gid, int)
        assert settings == "0o664"

    pq_path = "/".join([get_git_root("."), "dsi/data", "dummy_data.pq"])
    term.load_module("driver", "Parquet", "back-end", filename=pq_path)
    assert term.artifact_handler(interaction_type="put")


def test_squash_permissions_register_correctly(monkeypatch):
    monkeypatch.setattr("builtins.input", print_and_y)  # mock input
    term = Terminal(squash_permissions=True)
    bueno_path = "/".join([get_git_root("."), "dsi/data", "bueno1.data"])
    os.chmod(bueno_path, 0o664)
    term.load_module("plugin", "Bueno", "consumer", filenames=bueno_path)
    term.load_module("plugin", "Hostname", "consumer")

    term.transload()

    for env_col in ("uid", "effective_gid", "moniker", "gid_list"):
        uid, gid, settings = tuple(term.perms_manager.column_perms[env_col])
        assert isinstance(uid, int)
        assert isinstance(gid, int)
        assert settings == "0o660"

    for bueno_col in ("foo", "bar", "baz"):
        uid, gid, settings = tuple(term.perms_manager.column_perms[bueno_col])
        assert isinstance(uid, int)
        assert isinstance(gid, int)
        assert settings == "0o660"

    pq_path = "/".join([get_git_root("."), "dsi/data", "dummy_data.pq"])
    term.load_module("driver", "Parquet", "back-end", filename=pq_path)
    assert term.artifact_handler(interaction_type="put")


def test_permissions_output_correctly(monkeypatch):
    monkeypatch.setattr("builtins.input", print_and_y)  # mock input
    term = Terminal(allow_multiple_permissions=True)
    bueno_path = "/".join([get_git_root("."), "dsi/data", "bueno1.data"])
    os.chmod(bueno_path, 0o664)

    term.load_module("plugin", "Bueno", "consumer", filenames=bueno_path)
    term.load_module("plugin", "Hostname", "consumer")
    term.transload()

    pq_path = "/".join([get_git_root("."), "dsi/data", "dummy_data.pq"])
    term.load_module("driver", "Parquet", "back-end", filename=pq_path)

    assert term.artifact_handler(interaction_type="put")

    pm = PermissionsManager()
    written_paths = glob("/".join([get_git_root("."), "dsi/data"]) + "/dummy_data*.pq")
    for path in written_paths:
        uid, gid, settings = pm.get_file_permissions(path)
        if settings == "0o664":  # the bueno file
            old_uid, old_gid, old_settings = pm.get_file_permissions(bueno_path)
            assert uid == old_uid
            assert gid == old_gid
            assert settings == old_settings
        assert (
            path.find(str(uid)) != -1
            and path.find(str(gid)) != -1
            and path.find(settings) != -1
        )
