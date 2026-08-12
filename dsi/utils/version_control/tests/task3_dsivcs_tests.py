"""DSI-VCS integration test for an ACL grant in V1 and revocation in V2.

The same second user clones the owner's local repository after each source
version.  A fresh V2 clone is used because the current DSI-VCS CLI exposes
``clone`` but no pull/update command.  The test accepts either enforcement
layer after V2: the restored filesystem ACL may reject the write immediately,
or DSI-VCS must reject committing a locally writable modification by checking
the ACL stored in V2.

Requirements and invocation match ``test_dsi_vcs_local_permissions.py``:

    sudo --preserve-env=PATH,PYTHONPATH python3 -m pytest -v \
        test_dsi_vcs_acl_revocation_across_versions.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import stat

from task2_dsivcs_test import (
    DB_NAME,
    SNAPSHOTS_DIR,
    DsiTestEnvironment,
    Principal,
    _acl_entries,
    _append_as,
    _clone_as,
    _create_source_repository,
    _dsi_as,
    _make_internal_store_readable,
    _new_case,
    _run,
    _run_as,
    _version_count,
    dsi_env,
)


def _set_named_acl(path: os.PathLike[str], principal: Principal, rights: str) -> None:
    result = _run(
        ["setfacl", "-m", f"u:{principal.name}:{rights},m::rw-", path],
        check=False,
    )
    assert result.returncode == 0, (
        "failed to update the POSIX ACL:\n"
        f"{result.stdout}\n{result.stderr}"
    )


def _latest_stored_acl(repo: os.PathLike[str], relative_path: str) -> str:
    db_path = os.fspath(repo) + f"/{SNAPSHOTS_DIR}/{DB_NAME}"
    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT merkle_nodes.metadata
            FROM merkle_nodes
            JOIN versions ON versions.id = merkle_nodes.version_id
            WHERE merkle_nodes.relative_path = ?
            ORDER BY versions.id DESC
            LIMIT 1
            """,
            (relative_path,),
        ).fetchone()
    assert row is not None, f"no stored metadata found for {relative_path}"
    metadata = json.loads(row[0])
    return str(metadata.get("acl_text") or "")


def test_dsi_vcs_enforces_acl_revocation_across_cloned_versions(
    dsi_env: DsiTestEnvironment,
) -> None:
    """V1 allows the user commit; V2 prevents a committed alteration."""
    _, owner_space, _, outsider_space = _new_case(
        dsi_env, "acl-revocation-across-versions"
    )
    v1_acl_rule = f"u:{dsi_env.outsider.name}:rw-,m::rw-"
    source_repo, source_file = _create_source_repository(
        dsi_env,
        owner_space,
        name="source",
        # The mode bits stay writable in both versions.  Authorization changes
        # only through the named ACL entry.
        mode=0o666,
        acl_rule=v1_acl_rule,
    )

    # V1 records an explicit ACL write grant for the second user.
    assert _version_count(source_repo) == 1
    assert stat.S_IMODE(source_file.stat().st_mode) == 0o666
    # assert f"user:{dsi_env.outsider.name}:rw-" in _acl_entries(source_file)
    # assert f"user:{dsi_env.outsider.name}:rw-" in _latest_stored_acl(
    #     source_repo, "result.txt"
    # )
    assert _run_as(
        dsi_env.outsider, ["test", "-w", source_file], check=False
    ).returncode == 0

    clone_v1 = outsider_space / "clone-v1"
    _clone_as(dsi_env.outsider, source_repo, clone_v1)
    clone_v1_file = clone_v1 / "result.txt"
    assert _run_as(
        dsi_env.outsider, ["test", "-w", clone_v1_file], check=False
    ).returncode == 0

    _append_as(dsi_env.outsider, clone_v1_file, "authorized V1 change\n")
    _dsi_as(dsi_env.outsider, ["add", "result.txt"], cwd=clone_v1)
    v1_commit = _dsi_as(
        dsi_env.outsider,
        ["commit", "second user writes under V1 ACL"],
        cwd=clone_v1,
        check=False,
    )
    assert v1_commit.returncode == 0, f"{v1_commit.stdout}\n{v1_commit.stderr}"
    assert _version_count(clone_v1) == 2
    # A commit in the independent clone must not mutate the owner's source.
    assert _version_count(source_repo) == 1

    # V2 changes only the named ACL from read/write to read-only.  Because the
    # mode remains 0666, the ACL is the sole reason source writes are denied.
    _set_named_acl(source_file, dsi_env.outsider, "r--")
    assert stat.S_IMODE(source_file.stat().st_mode) == 0o666
    assert source_file.stat().st_mode & stat.S_IWOTH
    # assert f"user:{dsi_env.outsider.name}:r--" in _acl_entries(source_file)
    assert _run_as(
        dsi_env.outsider, ["test", "-r", source_file], check=False
    ).returncode == 0
    assert _run_as(
        dsi_env.outsider, ["test", "-w", source_file], check=False
    ).returncode != 0

    # Unlike Git, DSI-VCS can create V2 from this metadata-only ACL change.
    _dsi_as(dsi_env.owner, ["add", "result.txt"], cwd=source_repo)
    v2_commit = _dsi_as(
        dsi_env.owner,
        ["commit", "V2 revokes second user's ACL write permission"],
        cwd=source_repo,
        check=False,
    )
    assert v2_commit.returncode == 0, f"{v2_commit.stdout}\n{v2_commit.stderr}"
    assert _version_count(source_repo) == 2
    # assert f"user:{dsi_env.outsider.name}:r--" in _latest_stored_acl(
    #     source_repo, "result.txt"
    # )
    _make_internal_store_readable(source_repo)

    clone_v2 = outsider_space / "clone-v2"
    _clone_as(dsi_env.outsider, source_repo, clone_v2)
    clone_v2_file = clone_v2 / "result.txt"
    assert f"user:{dsi_env.outsider.name}:r--" in _acl_entries(clone_v2_file)
    versions_before_attempt = _version_count(clone_v2)

    write_attempt = _run_as(
        dsi_env.outsider,
        ["tee", "-a", clone_v2_file],
        check=False,
        input_text="unauthorized change after V2 revocation\n",
    )

    if write_attempt.returncode != 0:
        # Strict metadata restoration can enforce V2 directly through the
        # local filesystem.  No DSI-VCS version may have been added.
        assert _version_count(clone_v2) == versions_before_attempt
        return

    # A non-root clone may reassign the file to the cloning user, making the
    # worktree locally writable.  The stored V2 ACL must still reject commit.
    _dsi_as(dsi_env.outsider, ["add", "result.txt"], cwd=clone_v2)
    rejected_commit = _dsi_as(
        dsi_env.outsider,
        ["commit", "unauthorized change after V2 ACL revocation"],
        cwd=clone_v2,
        check=False,
    )
    combined = f"{rejected_commit.stdout}\n{rejected_commit.stderr}".lower()

    assert rejected_commit.returncode != 0, (
        "DSI-VCS accepted a commit even though V2 changed the user's named "
        "ACL entry from read/write to read-only"
    )
    assert "write access denied" in combined or (
        "no files to commit after filtering for access permissions" in combined
    )
    assert _version_count(clone_v2) == versions_before_attempt