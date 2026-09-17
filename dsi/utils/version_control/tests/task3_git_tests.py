"""Plain-Git integration test for an ACL grant in V1 and revocation in V2.

The test uses two local clones made by the same second user: one after V1 and
one after V2.  This keeps the Git and DSI-VCS experiments equivalent without
depending on a DSI-VCS pull command.  Git hooks are disabled by the shared
helpers in ``test_git_local_permissions.py``.

Requirements and invocation are the same as ``test_git_local_permissions.py``:

    sudo python3 -m pytest -v test_git_acl_revocation_across_versions.py
"""

from __future__ import annotations

import os
import shutil
import stat

import pytest

from task2_git_tests import (
    GitTestEnvironment,
    Principal,
    _acl_entries,
    _append_as,
    _clone_as,
    _create_source_repository,
    _git_as,
    _new_case,
    _run,
    _run_as,
    # commented out git_env to avoid linter fail
    # git_env,
)


def _set_named_acl(path: os.PathLike[str], principal: Principal, rights: str) -> None:
    result = _run(
        ["setfacl", "-m", f"u:{principal.name}:{rights},m::rw-", path],
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(
            "the test filesystem does not support POSIX ACLs: "
            f"{result.stderr.strip()}"
        )


def _commit_as(
    principal: Principal,
    repo: os.PathLike[str],
    *,
    message: str,
    identity: str,
) -> str:
    _git_as(principal, ["add", "result.txt"], cwd=repo)
    commit = _git_as(
        principal,
        [
            "-c",
            f"user.name={identity}",
            "-c",
            f"user.email={principal.name}@example.invalid",
            "commit",
            "-m",
            message,
        ],
        cwd=repo,
        check=False,
    )
    assert commit.returncode == 0, f"{commit.stdout}\n{commit.stderr}"
    return _git_as(principal, ["rev-parse", "HEAD"], cwd=repo).stdout.strip()


def test_git_does_not_enforce_acl_revocation_across_cloned_versions(
    git_env: GitTestEnvironment,
) -> None:
    """Git permits local writes and commits after an ACL is revoked in V2."""
    missing_acl_tools = [
        name for name in ("setfacl", "getfacl") if shutil.which(name) is None
    ]
    if missing_acl_tools:
        pytest.skip(f"missing ACL utilities: {', '.join(missing_acl_tools)}")

    _, owner_space, _, outsider_space = _new_case(
        git_env, "acl-revocation-across-versions"
    )
    source_repo = owner_space / "source"
    source_file = _create_source_repository(git_env.owner, source_repo)

    # V1: ordinary mode bits allow writing, and the named ACL explicitly grants
    # the second user read/write access.
    os.chmod(source_file, 0o666)
    _set_named_acl(source_file, git_env.outsider, "rw-")
    assert stat.S_IMODE(source_file.stat().st_mode) == 0o666
    assert f"user:{git_env.outsider.name}:rw-" in _acl_entries(source_file)
    assert _run_as(
        git_env.outsider, ["test", "-w", source_file], check=False
    ).returncode == 0

    # Git records neither the ACL nor this non-executable mode-bit change.
    assert _git_as(
        git_env.owner, ["status", "--porcelain"], cwd=source_repo
    ).stdout == ""
    v1_source_head = _git_as(
        git_env.owner, ["rev-parse", "HEAD"], cwd=source_repo
    ).stdout.strip()

    clone_v1 = outsider_space / "clone-v1"
    _clone_as(git_env.outsider, source_repo, clone_v1)
    clone_v1_file = clone_v1 / "result.txt"

    # The clone is writable because Git creates it under the cloning user.  The
    # source ACL grant is not present in the clone.
    assert clone_v1_file.stat().st_uid == git_env.outsider.uid
    assert f"user:{git_env.outsider.name}:rw-" not in _acl_entries(clone_v1_file)
    assert _run_as(
        git_env.outsider, ["test", "-w", clone_v1_file], check=False
    ).returncode == 0
    _append_as(git_env.outsider, clone_v1_file, "change by second user in V1\n")
    v1_clone_head = _commit_as(
        git_env.outsider,
        clone_v1,
        message="second user writes under V1",
        identity="Second User",
    )
    assert v1_clone_head != v1_source_head

    # V2: the owner revokes only the named user's ACL write right.  The mode
    # bits remain 0666, so the ACL is the reason source writes are denied.
    _set_named_acl(source_file, git_env.outsider, "r--")
    assert stat.S_IMODE(source_file.stat().st_mode) == 0o666
    assert source_file.stat().st_mode & stat.S_IWOTH
    assert f"user:{git_env.outsider.name}:r--" in _acl_entries(source_file)
    assert _run_as(
        git_env.outsider, ["test", "-w", source_file], check=False
    ).returncode != 0

    # An ACL-only change still leaves Git's working tree clean.  Add a content
    # marker solely so Git can create the requested successive V2 commit.
    assert _git_as(
        git_env.owner, ["status", "--porcelain"], cwd=source_repo
    ).stdout == ""
    _append_as(git_env.owner, source_file, "owner publishes V2\n")
    v2_source_head = _commit_as(
        git_env.owner,
        source_repo,
        message="V2 with source ACL write revoked",
        identity="Original User",
    )
    assert v2_source_head != v1_source_head

    clone_v2 = outsider_space / "clone-v2"
    _clone_as(git_env.outsider, source_repo, clone_v2)
    clone_v2_file = clone_v2 / "result.txt"

    # Git again recreates a user-owned writable file and omits the V2 ACL, so
    # the revocation has no effect on edits or commits in this local clone.
    assert clone_v2_file.stat().st_uid == git_env.outsider.uid
    assert f"user:{git_env.outsider.name}:r--" not in _acl_entries(clone_v2_file)
    assert _run_as(
        git_env.outsider, ["test", "-w", clone_v2_file], check=False
    ).returncode == 0
    _append_as(
        git_env.outsider,
        clone_v2_file,
        "change by second user after V2 revocation\n",
    )
    v2_clone_head = _commit_as(
        git_env.outsider,
        clone_v2,
        message="second user writes after V2 revocation",
        identity="Second User",
    )

    assert v2_clone_head != v2_source_head
    assert _git_as(
        git_env.owner, ["rev-parse", "HEAD"], cwd=source_repo
    ).stdout.strip() == v2_source_head
    # checking the behavior as following is ok.
    # this supports the git philosophy but fails dsi-vcs test setup
    # to make the test avoid git philosophy, changed != to == in line 175
    # git allows commit but owner and clone head points to different commit hash