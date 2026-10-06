"""
pytest suite that exercises common git commands end-to-end and reports
pass/fail for each one.

Covers: git init, clone, add, remove, delete, commit, diff, restore.

Interpretation notes:
- "remove" -> unstage an already-staged file with `git restore --staged`
  (the file stays on disk and in the working tree, it just leaves the
  staging area / index).
- "delete" -> delete a file that was already committed in an earlier
  commit, then record that deletion in the *next* commit
  (`git rm <file>` followed by `git commit`).

Requirements:
    pip install pytest
    git must be installed and on PATH

Run with:
    pytest -v test_git_commands.py

Each test prints a [PASS]/[FAIL] line for the git command it exercises
(visible with -s / -v), and also fails the test itself (via assert) if
the command's exit code was non-zero, so pytest's own summary reports
success/failure too.
"""

import subprocess

import pytest


def run_git(args, cwd):
    """Run `git <args>` in `cwd` and return the CompletedProcess (never raises)."""
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def report(label, result):
    """Print a human-readable pass/fail line for a git command result."""
    status = "PASS" if result.returncode == 0 else "FAIL"
    print(f"\n[{status}] git {' '.join(result.args[1:])}  (exit={result.returncode})")
    if result.stdout.strip():
        print(f"       stdout: {result.stdout.strip()}")
    if result.stderr.strip():
        print(f"       stderr: {result.stderr.strip()}")


@pytest.fixture(scope="class")
def paths(tmp_path_factory):
    """Shared origin/clone directories for the whole test class."""
    base = tmp_path_factory.mktemp("git_workflow")
    return {
        "origin": base / "origin",
        "clone": base / "clone",
    }


class TestGitWorkflow:
    """
    Runs a sequence of git commands and asserts each one succeeds.
    Tests are numbered because later steps depend on state built up by
    earlier ones (a repo, a commit, a clone) — pytest runs tests within
    a class top-to-bottom by default, so don't reorder with plugins
    like pytest-randomly.
    """

    def test_01_git_init(self, paths):
        origin = paths["origin"]
        origin.mkdir()

        result = run_git(["init"], cwd=origin)
        report("init", result)
        assert result.returncode == 0, result.stderr

        # Local identity so commits succeed in fresh/CI environments.
        for cfg in (
            ["config", "user.email", "test@example.com"],
            ["config", "user.name", "Test User"],
        ):
            cfg_result = run_git(cfg, cwd=origin)
            assert cfg_result.returncode == 0, cfg_result.stderr

    def test_02_git_add(self, paths):
        origin = paths["origin"]
        (origin / "hello.txt").write_text("hello world\n")

        result = run_git(["add", "hello.txt"], cwd=origin)
        report("add", result)
        assert result.returncode == 0, result.stderr

        status = run_git(["status", "--porcelain"], cwd=origin)
        assert "A  hello.txt" in status.stdout

    def test_03_git_commit(self, paths):
        origin = paths["origin"]

        result = run_git(["commit", "-m", "initial commit"], cwd=origin)
        report("commit", result)
        assert result.returncode == 0, result.stderr

    def test_04_git_clone(self, paths):
        origin, clone = paths["origin"], paths["clone"]

        result = run_git(["clone", str(origin), str(clone)], cwd=origin.parent)
        report("clone", result)
        assert result.returncode == 0, result.stderr
        assert (clone / "hello.txt").exists()

    def test_05_git_diff(self, paths):
        origin = paths["origin"]
        (origin / "hello.txt").write_text("hello world\nsecond line\n")

        result = run_git(["diff"], cwd=origin)
        report("diff", result)
        assert result.returncode == 0, result.stderr
        assert "second line" in result.stdout

    def test_06_git_restore(self, paths):
        origin = paths["origin"]
        file_path = origin / "hello.txt"

        result = run_git(["restore", "hello.txt"], cwd=origin)
        report("restore", result)
        assert result.returncode == 0, result.stderr
        assert file_path.read_text() == "hello world\n"

    def test_07_git_remove(self, paths):
        """Remove an already-staged file from the staging area (unstage it)."""
        origin = paths["origin"]
        staged_file = origin / "staged_file.txt"
        staged_file.write_text("this file will be unstaged\n")

        add_result = run_git(["add", "staged_file.txt"], cwd=origin)
        assert add_result.returncode == 0, add_result.stderr

        status_before = run_git(["status", "--porcelain"], cwd=origin)
        assert "A  staged_file.txt" in status_before.stdout

        result = run_git(["restore", "--staged", "staged_file.txt"], cwd=origin)
        report("restore --staged (remove from staging area)", result)
        assert result.returncode == 0, result.stderr

        # File stays on disk and in the working tree, just no longer staged.
        assert staged_file.exists()
        status_after = run_git(["status", "--porcelain"], cwd=origin)
        assert "?? staged_file.txt" in status_after.stdout

    def test_08_git_delete(self, paths):
        """Delete a previously committed file and record the deletion in the next commit."""
        origin = paths["origin"]
        committed_file = origin / "committed_file.txt"
        committed_file.write_text("this file will be deleted later\n")

        # Setup: commit the file first, so there's a prior commit to delete from.
        add_result = run_git(["add", "committed_file.txt"], cwd=origin)
        assert add_result.returncode == 0, add_result.stderr
        commit_result = run_git(["commit", "-m", "add file to be deleted"], cwd=origin)
        assert commit_result.returncode == 0, commit_result.stderr

        # Delete the tracked file and stage the deletion.
        result = run_git(["rm", "committed_file.txt"], cwd=origin)
        report("rm (delete)", result)
        assert result.returncode == 0, result.stderr
        assert not committed_file.exists()

        # Record the deletion in the next commit.
        commit_delete = run_git(["commit", "-m", "delete committed file"], cwd=origin)
        assert commit_delete.returncode == 0, commit_delete.stderr

        ls_files = run_git(["ls-files"], cwd=origin)
        assert "committed_file.txt" not in ls_files.stdout.split()