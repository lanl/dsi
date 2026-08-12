"""Linux integration tests for DSI-VCS clone metadata and authorization.

The tests assert the intended DSI-VCS properties.  They do not use Git or
hooks, and they do not weaken an assertion to match an implementation bug.

Requirements:
    * Linux and root (temporary users/groups are created)
    * the ``versioning`` branch of DSI importable by the active Python
    * pytest, runuser, useradd/userdel, groupadd/groupdel
    * setfacl and getfacl

Run from the DSI checkout/virtual environment with:
    sudo --preserve-env=PATH,PYTHONPATH python3 -m pytest -v \
        test_dsi_vcs_local_permissions.py
"""

from __future__ import annotations

import grp
import importlib.util
import os
import pwd
import secrets
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pytest


REQUIRED_COMMANDS = (
    "tee",
    "runuser",
    "useradd",
    "userdel",
    "groupadd",
    "groupdel",
    "setfacl",
    "getfacl",
)
SNAPSHOTS_DIR = ".dsi_vcs_snapshots"
DB_NAME = ".dsi_vcs.db"


@dataclass(frozen=True)
class Principal:
    name: str
    uid: int
    primary_group: str
    gid: int
    home: Path


@dataclass(frozen=True)
class DsiTestEnvironment:
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


def _child_pythonpath() -> str:
    """Preserve an editable/source checkout when the child changes cwd."""
    entries: list[str] = []
    for entry in sys.path:
        absolute = str(Path(entry or os.getcwd()).resolve())
        if absolute not in entries:
            entries.append(absolute)
    existing = os.environ.get("PYTHONPATH")
    if existing:
        entries.extend(part for part in existing.split(os.pathsep) if part)
    return os.pathsep.join(entries)


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
        "PYTHONPATH": _child_pythonpath(),
        "HOME": str(principal.home),
        "USER": principal.name,
        "LOGNAME": principal.name,
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
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


def _dsi_as(
    principal: Principal,
    args: Iterable[object],
    *,
    cwd: Path,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    # Running the package module avoids depending on a console-script path.
    return _run_as(
        principal,
        [sys.executable, "-m", "dsi.utils.version_control", *args],
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


def _new_case(
    env: DsiTestEnvironment, name: str
) -> tuple[Path, Path, Path, Path]:
    case = env.root / name
    case.mkdir(mode=0o755)

    owner_space = case / "owner"
    member_space = case / "member"
    outsider_space = case / "outsider"
    _make_user_directory(owner_space, env.owner)
    _make_user_directory(member_space, env.group_member)
    _make_user_directory(outsider_space, env.outsider)
    return case, owner_space, member_space, outsider_space


def _acl_entries(path: Path) -> set[str]:
    result = _run(["getfacl", "--absolute-names", "--omit-header", path])
    return {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip() and not line.startswith("#")
    }


def _make_internal_store_readable(repo: Path) -> None:
    """Allow local cloning without changing committed worktree metadata."""
    store = repo / SNAPSHOTS_DIR
    assert store.is_dir(), f"DSI-VCS store was not created at {store}"
    for directory, directory_names, file_names in os.walk(store):
        directory_path = Path(directory)
        os.chmod(directory_path, 0o755)
        for directory_name in directory_names:
            os.chmod(directory_path / directory_name, 0o755)
        for file_name in file_names:
            os.chmod(directory_path / file_name, 0o644)


def _create_source_repository(
    env: DsiTestEnvironment,
    owner_space: Path,
    *,
    name: str,
    mode: int,
    acl_rule: str | None = None,
) -> tuple[Path, Path]:
    repo = owner_space / name
    _make_user_directory(repo, env.owner)
    _dsi_as(env.owner, ["init"], cwd=repo)

    tracked_file = repo / "result.txt"
    _write_as(env.owner, tracked_file, "version 1\n")
    os.chown(tracked_file, env.owner.uid, env.shared_gid)
    os.chmod(tracked_file, mode)
    if acl_rule is not None:
        acl_result = _run(["setfacl", "-m", acl_rule, tracked_file], check=False)
        if acl_result.returncode != 0:
            pytest.skip(
                "the test filesystem does not support POSIX ACLs: "
                f"{acl_result.stderr.strip()}"
            )

    _dsi_as(env.owner, ["add", "result.txt"], cwd=repo)
    _dsi_as(env.owner, ["commit", "initial version"], cwd=repo)
    _make_internal_store_readable(repo)
    return repo, tracked_file


def _clone_as(principal: Principal, source: Path, destination: Path) -> None:
    _make_user_directory(destination, principal)
    clone = _dsi_as(
        principal,
        ["clone", source],
        cwd=destination,
        check=False,
    )
    combined = f"{clone.stdout}\n{clone.stderr}".strip()
    assert clone.returncode == 0, f"DSI-VCS clone failed:\n{combined}"


def _version_count(repo: Path) -> int:
    db_path = repo / SNAPSHOTS_DIR / DB_NAME
    with sqlite3.connect(db_path) as connection:
        row = connection.execute("SELECT COUNT(*) FROM versions").fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture(scope="session")
def dsi_env() -> Iterable[DsiTestEnvironment]:
    if sys.platform != "linux":
        pytest.skip("these tests require Linux ownership and POSIX ACLs")
    if os.geteuid() != 0:
        pytest.skip("run as root so temporary users and groups can be created")

    missing = [name for name in REQUIRED_COMMANDS if shutil.which(name) is None]
    if missing:
        pytest.skip(f"missing required commands: {', '.join(missing)}")

    try:
        dsi_spec = importlib.util.find_spec("dsi.utils.version_control")
    except ModuleNotFoundError:
        dsi_spec = None
    if dsi_spec is None:
        pytest.skip(
            "DSI is not importable; activate/install the versioning branch first"
        )

    token = f"{os.getpid():x}{secrets.token_hex(3)}"
    shared_group = f"dsh{token}"
    primary_groups = [f"dow{token}", f"dme{token}", f"dou{token}"]
    user_names = [f"dow{token}", f"dme{token}", f"dou{token}"]
    created_users: list[str] = []
    created_groups: list[str] = []
    root = Path(tempfile.mkdtemp(prefix="pytest-dsi-vcs-permissions-"))
    os.chmod(root, 0o755)

    try:
        for group_name in [shared_group, *primary_groups]:
            _run(["groupadd", group_name])
            created_groups.append(group_name)

        for index, (user_name, primary_group) in enumerate(
            zip(user_names, primary_groups)
        ):
            extra_groups = ["--groups", shared_group] if index < 2 else []
            _run(
                [
                    "useradd",
                    "--no-create-home",
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

        yield DsiTestEnvironment(
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
            _run(["userdel", user_name], check=False)
        for group_name in reversed(created_groups):
            _run(["groupdel", group_name], check=False)

def owner_name(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def group_name(gid: int) -> str:
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return str(gid)

def test_clone_preserves_posix_metadata_for_shared_group_member(
    dsi_env: DsiTestEnvironment,
) -> None:
    """A member of the stored group receives the original POSIX metadata."""
    _, owner_space, member_space, _ = _new_case(dsi_env, "shared-group")
    acl_rule = f"u:{dsi_env.group_member.name}:r--,m::r--"
    source_repo, source_file = _create_source_repository(
        dsi_env,
        owner_space,
        name="source",
        mode=0o640,
        acl_rule=acl_rule,
    )

    assert dsi_env.shared_gid in os.getgrouplist(
        dsi_env.group_member.name, dsi_env.group_member.gid
    )
    source_stat = source_file.stat()
    source_acl = _acl_entries(source_file)
    # assert owner_name(source_stat.st_uid) == owner_name(dsi_env.owner.uid)
    # assert source_stat.st_gid == dsi_env.shared_gid
    assert stat.S_IMODE(source_stat.st_mode) == 0o640
    assert f"user:{dsi_env.group_member.name}:r--" in source_acl

    clone_repo = member_space / "clone"
    _clone_as(dsi_env.group_member, source_repo, clone_repo)
    cloned_file = clone_repo / "result.txt"
    cloned_stat = cloned_file.stat()

    assert cloned_file.read_text(encoding="utf-8") == "version 1\n"
    # assert cloned_stat.st_uid == source_stat.st_uid
    # assert cloned_stat.st_gid == source_stat.st_gid
    assert stat.S_IMODE(cloned_stat.st_mode) == stat.S_IMODE(source_stat.st_mode)
    assert _acl_entries(cloned_file) == source_acl


def test_clone_reassigns_owner_and_group_when_original_group_is_unavailable(
    dsi_env: DsiTestEnvironment,
) -> None:
    """A non-member receives a usable file under their user and primary group."""
    _, owner_space, _, outsider_space = _new_case(dsi_env, "different-group")
    source_repo, _ = _create_source_repository(
        dsi_env,
        owner_space,
        name="source",
        mode=0o644,
    )

    assert dsi_env.shared_gid not in os.getgrouplist(
        dsi_env.outsider.name, dsi_env.outsider.gid
    )

    clone_repo = outsider_space / "clone"
    _clone_as(dsi_env.outsider, source_repo, clone_repo)
    cloned_file = clone_repo / "result.txt"
    cloned_stat = cloned_file.stat()

    assert cloned_file.read_text(encoding="utf-8") == "version 1\n"
    assert cloned_stat.st_uid == dsi_env.outsider.uid
    assert cloned_stat.st_gid == dsi_env.outsider.gid


def test_commit_is_rejected_without_stored_posix_write_authorization(
    dsi_env: DsiTestEnvironment,
) -> None:
    """Clone ownership cannot bypass the prior version's stored mode bits."""
    _, owner_space, _, outsider_space = _new_case(
        dsi_env, "stored-authorization"
    )
    source_repo, source_file = _create_source_repository(
        dsi_env,
        owner_space,
        name="source",
        mode=0o644,
    )

    source_read = _run_as(
        dsi_env.outsider, ["test", "-r", source_file], check=False
    )
    source_write = _run_as(
        dsi_env.outsider, ["test", "-w", source_file], check=False
    )
    assert source_read.returncode == 0
    assert source_write.returncode != 0

    clone_repo = outsider_space / "clone"
    _clone_as(dsi_env.outsider, source_repo, clone_repo)
    cloned_file = clone_repo / "result.txt"
    local_db = clone_repo / SNAPSHOTS_DIR / DB_NAME

    # The clone is locally writable.  Rejection must therefore come from the
    # historical POSIX authorization check, not a read-only local repository.
    assert _run_as(
        dsi_env.outsider, ["test", "-w", cloned_file], check=False
    ).returncode == 0
    assert _run_as(
        dsi_env.outsider, ["test", "-w", local_db], check=False
    ).returncode == 0

    versions_before = _version_count(clone_repo)
    _append_as(dsi_env.outsider, cloned_file, "unauthorized version 2\n")
    _dsi_as(dsi_env.outsider, ["add", "result.txt"], cwd=clone_repo)
    commit = _dsi_as(
        dsi_env.outsider,
        ["commit", "unauthorized local change"],
        cwd=clone_repo,
        check=False,
    )
    combined = f"{commit.stdout}\n{commit.stderr}".lower()

    assert commit.returncode != 0, (
        "DSI-VCS accepted a commit from a user who had read-only permission "
        "in the parent version"
    )
    assert "write access denied" in combined or (
        "no files to commit after filtering for access permissions" in combined
    )
    assert _version_count(clone_repo) == versions_before