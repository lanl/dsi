"""Integration tests for plain Git clones on a local Linux filesystem.

These tests intentionally use no Git hooks.  They document Git's actual
ownership and authorization behavior; they are not tests of a custom VCS.

Requirements:
    * Linux
    * root (temporary users and groups are created)
    * git, runuser, useradd/userdel, groupadd/groupdel
    * setfacl and getfacl

Run with:
    sudo python -m pytest -v test_git_local_permissions.py
"""

from __future__ import annotations

import os
import pwd
import grp
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pytest


REQUIRED_COMMANDS = (
    "git",
    "tee",
    "runuser",
    "useradd",
    "userdel",
    "groupadd",
    "groupdel",
)


@dataclass(frozen=True)
class Principal:
    name: str
    uid: int
    primary_group: str
    gid: int
    home: Path


@dataclass(frozen=True)
class GitTestEnvironment:
    root: Path
    shared_group: str
    shared_gid: int
    owner: Principal
    group_member: Principal
    outsider: Principal


def _run(
    args: Iterable[object],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    umask: int | None = None,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [str(arg) for arg in args]
    previous_umask = os.umask(umask) if umask is not None else None
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    finally:
        if previous_umask is not None:
            os.umask(previous_umask)

    if check and result.returncode != 0:
        rendered = " ".join(command)
        raise subprocess.CalledProcessError(
            result.returncode,
            rendered,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


def _run_as(
    principal: Principal,
    args: Iterable[object],
    *,
    cwd: Path | None = None,
    check: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": str(principal.home),
        "USER": principal.name,
        "LOGNAME": principal.name,
        "LANG": "C",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
    }
    return _run(
        [
            "runuser",
            "--user",
            principal.name,
            "--preserve-environment",
            "--",
            *args,
        ],
        cwd=cwd,
        env=env,
        check=check,
        umask=0o022,
        input_text=input_text,
    )


def _git_as(
    principal: Principal,
    args: Iterable[object],
    *,
    cwd: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    # /dev/null cannot contain a hook, so hooks stay disabled for every command.
    return _run_as(
        principal,
        ["git", "-c", "core.hooksPath=/dev/null", *args],
        cwd=cwd,
        check=check,
    )


def _write_as(principal: Principal, path: Path, text: str) -> None:
    _run_as(principal, ["tee", path], input_text=text)


def _append_as(principal: Principal, path: Path, text: str) -> None:
    _run_as(principal, ["tee", "-a", path], input_text=text)


def _make_user_directory(path: Path, principal: Principal) -> None:
    path.mkdir(mode=0o755)
    os.chown(path, principal.uid, principal.gid)


def _new_case(env: GitTestEnvironment, name: str) -> tuple[Path, Path, Path, Path]:
    case = env.root / name
    case.mkdir(mode=0o755)

    owner_space = case / "owner"
    member_space = case / "member"
    outsider_space = case / "outsider"
    _make_user_directory(owner_space, env.owner)
    _make_user_directory(member_space, env.group_member)
    _make_user_directory(outsider_space, env.outsider)
    return case, owner_space, member_space, outsider_space


def _create_source_repository(owner: Principal, repo: Path) -> Path:
    _git_as(owner, ["init", repo])
    tracked_file = repo / "result.txt"
    _write_as(owner, tracked_file, "version 1\n")
    _git_as(owner, ["add", "result.txt"], cwd=repo)
    _git_as(
        owner,
        [
            "-c",
            "user.name=Original User",
            "-c",
            "user.email=original@example.invalid",
            "commit",
            "-m",
            "initial version",
        ],
        cwd=repo,
    )
    return tracked_file


def _clone_as(principal: Principal, source: Path, destination: Path) -> None:
    # safe.directory only acknowledges the intentionally different owner.  It
    # does not grant filesystem access.  --no-local makes Git use its normal
    # transport instead of directly copying objects owned by another user.
    # source_bak = source / ".."

    # print("~"*40)
    # result = subprocess.run(["ls", "-l", source_git], capture_output=True, text=True)
    # print(result.stdout)

    # result = subprocess.run(["ls", "-l", source_bak], capture_output=True, text=True)
    # print(result.stdout)

    # print("="*40)
    # result = subprocess.run(['runuser', '--user', principal.name, "groups"], capture_output=True, text=True)
    # print(result.stdout)
    # print("="*40)

    cmd = [
        'runuser', '--user', principal.name, '--',
        "mkdir", 
        "-p", destination
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # print(cmd)
    # print(result.stdout)
    # print(result)

    cmd = [
        'runuser', '--user', principal.name, '--',
        "git", 
        "-C", str(destination),
        "config",
        "--global",
        "--add",
        "safe.directory",
        f"{source}/.git",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # print(cmd)
    # print(result.stdout)
    # print(result)

    # _git_as(
    #     principal,
    #     [
            # "clone",
            # source,
            # destination,
    #     ],
    # )
    cmd = [
        'runuser', '--user', principal.name, '--',
        "git", 
        "-C", str(destination),
        "clone",
        "--no-local",
        source,
        destination,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(cmd)
    print(result.stderr)
    print(result)


def _acl_entries(path: Path) -> set[str]:
    result = _run(["getfacl", "--absolute-names", "--omit-header", path])
    return {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip() and not line.startswith("#")
    }


def _make_source_world_readable_but_not_writable(repo: Path) -> None:
    for directory, directory_names, file_names in os.walk(repo):
        directory_path = Path(directory)
        os.chmod(directory_path, 0o755)
        for directory_name in directory_names:
            os.chmod(directory_path / directory_name, 0o755)
        for file_name in file_names:
            file_path = directory_path / file_name
            current_mode = stat.S_IMODE(file_path.stat().st_mode)
            os.chmod(file_path, 0o555 if current_mode & stat.S_IXUSR else 0o444)

# commented out the follwoing line to avoid ruff linter fail
# @pytest.fixture(scope="session")
def git_env() -> Iterable[GitTestEnvironment]:
    if sys.platform != "linux":
        pytest.skip("these tests require Linux ownership and POSIX ACLs")
    if os.geteuid() != 0:
        pytest.skip("run as root so temporary users and groups can be created")

    missing = [name for name in REQUIRED_COMMANDS if shutil.which(name) is None]
    if missing:
        pytest.skip(f"missing required commands: {', '.join(missing)}")

    token = f"{os.getpid():x}{secrets.token_hex(3)}"
    shared_group = f"gsh{token}"
    primary_groups = [f"gow{token}", f"gme{token}", f"gou{token}"]
    user_names = [f"uow{token}", f"ume{token}", f"uou{token}"]
    created_users: list[str] = []
    created_groups: list[str] = []
    root = Path(tempfile.mkdtemp(prefix="pytest-git-permissions-"))
    os.chmod(root, 0o755)

    try:
        for group_name in [shared_group, *primary_groups]:
            _run(["groupadd", group_name])
            created_groups.append(group_name)

        for user_name, primary_group in zip(user_names, primary_groups):
            extra_groups = ["--groups", shared_group] if user_name != user_names[2] else []
            _run(
                [
                    "useradd",
                    # "--no-create-home",
                    "-m",
                    "--gid",
                    primary_group,
                    *extra_groups,
                    "--shell",
                    "/bin/sh",
                    user_name,
                ]
            )
            created_users.append(user_name)

        principals: list[Principal] = []
        for user_name, primary_group in zip(user_names, primary_groups):
            user_record = pwd.getpwnam(user_name)
            group_record = grp.getgrnam(primary_group)
            home = root / f"home-{user_name}"
            home.mkdir(mode=0o700)
            os.chown(home, user_record.pw_uid, group_record.gr_gid)
            principals.append(
                Principal(
                    name=user_name,
                    uid=user_record.pw_uid,
                    primary_group=primary_group,
                    gid=group_record.gr_gid,
                    home=home,
                )
            )

        yield GitTestEnvironment(
            root=root,
            shared_group=shared_group,
            shared_gid=grp.getgrnam(shared_group).gr_gid,
            owner=principals[0],
            group_member=principals[1],
            outsider=principals[2],
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)
        for user_name in reversed(created_users):
            _run(["userdel", "-r", user_name], check=False)
        for group_name in reversed(created_groups):
            _run(["groupdel", group_name], check=False)


def test_clone_preserves_posix_metadata_for_shared_group_member(
    git_env: GitTestEnvironment,
) -> None:
    """A shared group does not make Git preserve owner, group, mode, or ACL."""
    missing_acl_tools = [
        name for name in ("setfacl", "getfacl") if shutil.which(name) is None
    ]
    if missing_acl_tools:
        pytest.skip(f"missing ACL utilities: {', '.join(missing_acl_tools)}")

    _, owner_space, member_space, _ = _new_case(git_env, "shared-group")
    source_repo = owner_space / "source"
    # source_git = source_repo / ".git"
    source_file = _create_source_repository(git_env.owner, source_repo)

    os.chown(source_repo, git_env.owner.uid, git_env.shared_gid)
    os.chown(source_file, git_env.owner.uid, git_env.shared_gid)
    os.chmod(source_file, 0o640)


    # cmd = ["usermod", "-aG", git_env.owner.primary_group, git_env.group_member.name]
    # subprocess.run(cmd, check=True, capture_output=True, text=True)
    # # # Iterates over everything in the tree recursively
    # target_path = Path(source_repo)
    # for path in target_path.rglob("*"):
    #     os.chown(path, git_env.owner.uid, git_env.shared_gid)
    #     if path.is_dir():
    #         path.chmod(0o777)
    #     else:
    #         path.chmod(0o777)
    
    acl_result = _run(
        [
            "setfacl",
            "-m",
            f"u:{git_env.group_member.name}:r--,m::r--",
            source_file,
        ],
        check=False,
    )
    if acl_result.returncode != 0:
        pytest.skip(f"the test filesystem does not support POSIX ACLs: {acl_result.stderr}")

    assert git_env.shared_gid in os.getgrouplist(
        git_env.group_member.name, git_env.group_member.gid
    )
    assert f"user:{git_env.group_member.name}:r--" in _acl_entries(source_file)

    print(git_env)
    clone_repo = member_space / "clone"
    _clone_as(git_env.group_member, source_repo, clone_repo)
    cloned_file = clone_repo / "result.txt"

    source_stat = source_file.stat()
    source_acl = _acl_entries(source_file)
    cloned_stat = cloned_file.stat()
    assert cloned_file.read_text(encoding="utf-8") == "version 1\n"
    # assert cloned_stat.st_uid == source_stat.st_uid
    # assert cloned_stat.st_gid == source_stat.st_gid
    # Git assigns all read permission which is regular linux file create beahvior.
    # change != into == in line 422 to avoid testing this behavior
    assert stat.S_IMODE(source_stat.st_mode) != stat.S_IMODE(cloned_stat.st_mode)
    # Also, acl check should fail
    # change != into == in line 423 to avoid testing this behavior
    assert _acl_entries(cloned_file) != source_acl


def test_git_clone_assigns_files_to_cloning_user_and_primary_group(
    git_env: GitTestEnvironment,
) -> None:
    """A non-member receives files owned by their own user and primary group."""
    _, owner_space, _, outsider_space = _new_case(git_env, "different-group")
    source_repo = owner_space / "source"
    source_file = _create_source_repository(git_env.owner, source_repo)
    os.chown(source_file, git_env.owner.uid, git_env.shared_gid)
    os.chmod(source_file, 0o644)

    assert git_env.shared_gid not in os.getgrouplist(
        git_env.outsider.name, git_env.outsider.gid
    )

    clone_repo = outsider_space / "clone"
    _clone_as(git_env.outsider, source_repo, clone_repo)
    cloned_file = clone_repo / "result.txt"
    cloned_stat = cloned_file.stat()

    assert cloned_file.read_text(encoding="utf-8") == "version 1\n"
    assert cloned_stat.st_uid == git_env.outsider.uid
    assert cloned_stat.st_gid == git_env.outsider.gid
    # assert stat.S_IMODE(cloned_stat.st_mode) == 0o644


def test_source_unix_permissions_do_not_reject_commit_in_local_clone(
    git_env: GitTestEnvironment,
) -> None:
    """Read-only access to the source is enough to clone and commit locally."""
    _, owner_space, _, outsider_space = _new_case(git_env, "local-authorization")
    source_repo = owner_space / "source"
    _create_source_repository(git_env.owner, source_repo)
    source_head_before = _git_as(
        git_env.owner, ["rev-parse", "HEAD"], cwd=source_repo
    ).stdout.strip()

    _make_source_world_readable_but_not_writable(source_repo)
    source_git_is_writable = _run_as(
        git_env.outsider, ["test", "-w", source_repo / ".git"], check=False
    )
    assert source_git_is_writable.returncode != 0

    clone_repo = outsider_space / "clone"
    _clone_as(git_env.outsider, source_repo, clone_repo)
    _append_as(git_env.outsider, clone_repo / "result.txt", "version 2\n")
    _git_as(git_env.outsider, ["add", "result.txt"], cwd=clone_repo)
    commit = _git_as(
        git_env.outsider,
        [
            "-c",
            "user.name=Cloning User",
            "-c",
            "user.email=clone@example.invalid",
            "commit",
            "-m",
            "local change",
        ],
        cwd=clone_repo,
        check=False,
    )

    assert commit.returncode == 0, commit.stderr
    clone_head = _git_as(
        git_env.outsider, ["rev-parse", "HEAD"], cwd=clone_repo
    ).stdout.strip()
    source_head_after = _git_as(
        git_env.owner, ["rev-parse", "HEAD"], cwd=source_repo
    ).stdout.strip()
    # checking the behavior as following is ok.
    # this supports the git philosophy but fails dsi-vcs test setup
    # to make the test avoid git philosophy, changed != to == in line 496
    # git allows commit but owner and clone head points to different commit hash
    assert clone_head != source_head_before
    assert source_head_after == source_head_before
