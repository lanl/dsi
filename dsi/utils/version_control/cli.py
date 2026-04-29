import argparse
import datetime
import sys
import os

from .dsi_vcs import Version

def main():
    parser = argparse.ArgumentParser(
        description="dsi-vcs — rsync-based file version control with full Linux metadata"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Initialize a new repo")
    # p_init.add_argument("root_folder")

    p_add = sub.add_parser("add", help="add file or directory to staging")
    p_add.add_argument('files', nargs='+', type=str)

    p_delete = sub.add_parser("delete", help="stage file or directory deletion")
    p_delete.add_argument('files', nargs='+', type=str)

    p_remove = sub.add_parser("remove", help="remove file or directory from staging")
    p_remove.add_argument('files', nargs='+', type=str)

    p_commit = sub.add_parser("commit", help="Snapshot and commit staged paths")
    p_commit.add_argument("message", nargs="?", default="Committed at " + datetime.datetime.now().isoformat())

    p_log = sub.add_parser("log", help="List all commits")

    p_diff = sub.add_parser("diff", help="Diff two commits")
    p_diff.add_argument("c1", nargs="?", default=None)
    p_diff.add_argument("c2", nargs="?", default=None)

    p_restore = sub.add_parser("restore", help="Restore a version")
    p_restore.add_argument("version")

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
    elif args.command == "diff":
        vcs = Version(os.getcwd())
        vcs.cmd_diff(args.c1, args.c2)
    elif args.command == "log":
        vcs = Version(os.getcwd())
        vcs.cmd_log()
    elif args.command == "restore":
        vcs = Version(os.getcwd())
        vcs.cmd_restore(args.version)

if __name__ == "__main__":
    # print("\n=== dsi-vcs: rsync-based file version control ===\n")
    # print(os.getcwd())
    main()
