import argparse
import datetime
import sys
import os

from .dsi_vcs import Version
from . import __version__

def main():
    parser = argparse.ArgumentParser(
        description=f"dsi-vcs (v{__version__}) — content defined version control with file metadata"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Initialize a new repo")
    # p_init.add_argument("root_folder")

    p_add = sub.add_parser("add", help="add file or directory to staging")
    p_add.add_argument('files', nargs='+', type=str)

    p_delete = sub.add_parser("delete", help="stage file or directory deletion")
    p_delete.add_argument('files', nargs='+', type=str)

    p_remove = sub.add_parser("remove", help="remove file or directory from staging")
    p_remove.add_argument('files', nargs='+', type=str)

    p_commit = sub.add_parser("commit", help="Snapshot and commit staged paths")
    p_commit.add_argument("message", nargs="?", default="Committed at " + datetime.datetime.now().isoformat())

    p_branch = sub.add_parser("branch", help="Create a branch")
    p_branch.add_argument("branch_name")
    p_branch.add_argument("start_point", nargs="?", default=None)

    p_merge = sub.add_parser("merge", help="Merge a branch into the current HEAD")
    p_merge.add_argument("branch_name")
    p_merge.add_argument("target_commit", nargs="?", default=None)

    sub.add_parser("list-branch", help="List all branches")

    p_switch = sub.add_parser("switch", help="Switch to a branch")
    p_switch.add_argument("branch_name")

    p_log = sub.add_parser("log", help="List all commits")
    p_log.add_argument("branch_name", nargs="?", default=None)
    p_log.add_argument("v_limit", nargs="?", default=10, type=int)

    p_diff = sub.add_parser("diff", help="Diff two commits")
    p_diff.add_argument("c1", nargs="?", default=None)
    p_diff.add_argument("c2", nargs="?", default=None)

    p_restore = sub.add_parser("restore", help="Restore a version")
    p_restore.add_argument("version")

    p_clone = sub.add_parser("clone", help="Clone a remote repo")
    p_clone.add_argument("repo_url")
    p_clone.add_argument("dest_path", nargs="?", default=None)

    sub.add_parser("status", help="Show current branch, commit, and staged files")
    
    args = parser.parse_args(args=None if sys.argv[1:] else ["-h"])

    # parser.print_help()
    # parser.format_help()
    if   args.command == "init":
        vcs = Version(os.getcwd())
    elif args.command == "add":
        vcs = Version(os.getcwd())
        vcs.cmd_add(args.files)
    elif args.command == "delete":
        vcs = Version(os.getcwd())
        vcs.cmd_delete(args.files)
    elif args.command == "remove":
        vcs = Version(os.getcwd())
        vcs.cmd_remove(args.files)
    elif args.command == "commit":
        vcs = Version(os.getcwd())
        vcs.cmd_commit(args.message)
    elif args.command == "branch":
        vcs = Version(os.getcwd())
        vcs.cmd_branch(args.branch_name, args.start_point)
    elif args.command == "merge":
        vcs = Version(os.getcwd())
        vcs.cmd_merge(args.branch_name, args.target_commit)
    elif args.command == "list-branch":
        vcs = Version(os.getcwd())
        vcs.cmd_list_branch()
    elif args.command == "switch":
        vcs = Version(os.getcwd())
        vcs.cmd_switch(args.branch_name)
    elif args.command == "diff":
        vcs = Version(os.getcwd())
        vcs.cmd_diff(args.c1, args.c2)
    elif args.command == "log":
        vcs = Version(os.getcwd())
        vcs.cmd_log(args.branch_name, args.v_limit)
    elif args.command == "restore":
        vcs = Version(os.getcwd())
        vcs.cmd_restore(args.version)
    elif args.command == "clone":
        vcs = Version(os.getcwd())
        vcs.cmd_clone(args.repo_url, args.dest_path)
    elif args.command == "status":
        vcs = Version(os.getcwd())
        vcs.cmd_status()

if __name__ == "__main__":
    # print("\n=== dsi-vcs: rsync-based file version control ===\n")
    # print(os.getcwd())
    main()
