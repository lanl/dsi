"""
Setup:
    pip install pytest

Run with:
    pytest -v -s test_dsi_vcs_commands.py

If `dsi-vcs` is not on PATH, or fails a basic `-h` smoke check (e.g. due
to issue #1 above), the whole suite is skipped with an explanatory
reason rather than failing every test with an import traceback.
"""

import re
import shutil
import subprocess

import pytest

COMMIT_HASH_RE = re.compile(r"Committed (\S+) at")
FILE_COUNT_RE = re.compile(r"(\d+) file\(s\),")


def run_dsi_vcs(args, cwd):
    """Run `dsi-vcs <args>` in `cwd` and return the CompletedProcess (never raises)."""
    return subprocess.run(
        ["dsi-vcs", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def report(label, result):
    """Print a human-readable pass/fail line for a dsi-vcs command result."""
    status = "PASS" if result.returncode == 0 else "FAIL"
    print(f"\n[{status}] dsi-vcs {' '.join(result.args[1:])}  (exit={result.returncode})")
    if result.stdout.strip():
        print(f"       stdout: {result.stdout.strip()}")
    if result.stderr.strip():
        print(f"       stderr: {result.stderr.strip()}")


def extract_commit_hash(stdout):
    match = COMMIT_HASH_RE.search(stdout)
    assert match, f"could not find a commit hash in commit output:\n{stdout}"
    return match.group(1)


def extract_file_count(stdout):
    match = FILE_COUNT_RE.search(stdout)
    assert match, f"could not find a file count in commit output:\n{stdout}"
    return int(match.group(1))


def _dsi_vcs_available():
    """True only if `dsi-vcs` is on PATH and runs without an import-time crash."""
    exe = shutil.which("dsi-vcs")
    if exe is None:
        return False
    try:
        result = subprocess.run([exe, "-h"], capture_output=True, text=True, timeout=15)
    except Exception:
        return False
    return result.returncode == 0


DSI_VCS_AVAILABLE = _dsi_vcs_available()
pytestmark = pytest.mark.skipif(
    not DSI_VCS_AVAILABLE,
    reason=(
        "dsi-vcs CLI not found or failed to import (see module docstring for "
        "required system packages and two upstream import-bug patches)."
    ),
)


@pytest.fixture(scope="class")
def paths(tmp_path_factory):
    """Shared origin/clone directories for the whole test class."""
    base = tmp_path_factory.mktemp("dsi_vcs_workflow")
    return {
        "origin": base / "origin",
        "clone": base / "clone",
    }


@pytest.fixture(scope="class")
def state():
    """Mutable state (e.g. commit hashes) shared across ordered tests."""
    return {}


class TestDsiVcsWorkflow:
    """
    Runs a sequence of dsi-vcs commands and asserts each one succeeds.
    Tests are numbered because later steps depend on state built up by
    earlier ones (a repo, a commit, a clone) - pytest runs tests within
    a class top-to-bottom by default, so don't reorder with plugins
    like pytest-randomly.
    """

    def test_01_dsi_vcs_init(self, paths):
        origin = paths["origin"]
        origin.mkdir(parents=True)

        result = run_dsi_vcs(["init"], cwd=origin)
        report("init", result)
        assert result.returncode == 0, result.stderr
        assert (origin / ".dsi_vcs_snapshots").is_dir()

    def test_02_dsi_vcs_add(self, paths):
        origin = paths["origin"]
        (origin / "hello.txt").write_text("hello world\n")

        result = run_dsi_vcs(["add", "hello.txt"], cwd=origin)
        report("add", result)
        assert result.returncode == 0, result.stderr
        assert "hello.txt [add]" in result.stdout

    def test_03_dsi_vcs_commit(self, paths, state):
        origin = paths["origin"]

        result = run_dsi_vcs(["commit", "initial commit"], cwd=origin)
        report("commit", result)
        assert result.returncode == 0, result.stderr
        state["initial_commit_hash"] = extract_commit_hash(result.stdout)

    def test_04_dsi_vcs_clone(self, paths):
        origin, clone = paths["origin"], paths["clone"]
        clone.mkdir(parents=True)

        # dsi-vcs clone is run FROM the (empty) target directory, pointed
        # at the source repo path - there is no separate destination arg.
        result = run_dsi_vcs(["clone", str(origin)], cwd=clone)
        report("clone", result)
        assert result.returncode == 0, result.stderr
        assert (clone / "hello.txt").exists()
        assert (clone / "hello.txt").read_text() == "hello world\n"

    def test_05_dsi_vcs_diff(self, paths):
        origin = paths["origin"]
        (origin / "hello.txt").write_text("hello world\nsecond line\n")

        # Zero-arg diff: working tree vs. latest commit (equivalent in
        # spirit to plain `git diff`). Two-explicit-hash diff is a known
        # broken code path upstream - see module docstring, issue #3.
        result = run_dsi_vcs(["diff"], cwd=origin)
        report("diff", result)
        assert result.returncode == 0, result.stderr
        assert "MODIFIED" in result.stdout
        assert "hello.txt" in result.stdout

    def test_06_dsi_vcs_restore(self, paths, state):
        origin = paths["origin"]
        commit_hash = state["initial_commit_hash"]

        result = run_dsi_vcs(["restore", commit_hash], cwd=origin)
        report("restore", result)
        assert result.returncode == 0, result.stderr
        assert (origin / "hello.txt").read_text() == "hello world\n"

    def test_07_dsi_vcs_remove(self, paths):
        """
        Remove an already-staged file from the staging area (unstage it)
        without deleting the file itself.

        NOTE: this currently fails against the `versioning` branch due to
        a real upstream bug - see module docstring, issue #2. The
        assertions below reflect the documented contract, not the buggy
        behavior.
        """
        origin = paths["origin"]
        staged_file = origin / "staged_file.txt"
        staged_file.write_text("this file will be unstaged\n")

        add_result = run_dsi_vcs(["add", "staged_file.txt"], cwd=origin)
        assert add_result.returncode == 0, add_result.stderr
        assert "staged_file.txt [add]" in add_result.stdout

        result = run_dsi_vcs(["remove", "staged_file.txt"], cwd=origin)
        report("remove (unstage)", result)
        assert result.returncode == 0, result.stderr

        # File must remain on disk - "remove" only affects staging.
        assert staged_file.exists()
        assert "staged_file.txt [add]" not in result.stdout, (
            "staged_file.txt is still listed as staged after 'remove' "
            "(see module docstring, known issue #2)"
        )

    def test_08_dsi_vcs_delete(self, paths):
        """
        Stage a previously committed file for deletion and record that in
        the next commit.

        NOTE: unlike git's `git rm`, dsi-vcs's `delete` only removes the
        path from tracking - it does not remove the file from disk (see
        module docstring). This test asserts on the tracked file count
        reported by `commit`, and explicitly confirms the file is still
        present on disk, rather than asserting the file was deleted.
        """
        origin = paths["origin"]
        doomed_file = origin / "doomed.txt"
        doomed_file.write_text("this will be untracked, not deleted\n")

        add_result = run_dsi_vcs(["add", "doomed.txt"], cwd=origin)
        assert add_result.returncode == 0, add_result.stderr
        commit_add = run_dsi_vcs(["commit", "add doomed file"], cwd=origin)
        assert commit_add.returncode == 0, commit_add.stderr
        count_before = extract_file_count(commit_add.stdout)

        result = run_dsi_vcs(["delete", "doomed.txt"], cwd=origin)
        report("delete (stage deletion)", result)
        assert result.returncode == 0, result.stderr
        assert "doomed.txt [delete]" in result.stdout

        commit_delete = run_dsi_vcs(["commit", "delete doomed file"], cwd=origin)
        assert commit_delete.returncode == 0, commit_delete.stderr
        count_after = extract_file_count(commit_delete.stdout)

        assert count_after == count_before - 1
        # Documented dsi-vcs behavior: the file itself is left in place.
        assert doomed_file.exists()