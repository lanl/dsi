import os
import stat
import hashlib
import sqlite3
import json
import subprocess
import pwd
import grp
import sys
from typing import Optional

# ─────────────────────────── METADATA HELPERS ────────────────────────────────

def file_type_str(mode: int) -> str:
    if stat.S_ISREG(mode):  return "file"
    if stat.S_ISDIR(mode):  return "dir"
    if stat.S_ISLNK(mode):  return "symlink"
    if stat.S_ISBLK(mode):  return "block"
    if stat.S_ISCHR(mode):  return "char"
    if stat.S_ISFIFO(mode): return "fifo"
    if stat.S_ISSOCK(mode): return "socket"
    return "unknown"


def permission_str(mode: int) -> str:
    """Convert mode bits → rwxrwxrwx string (no file-type prefix)."""
    chars = []
    for who in (stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR,
                stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP,
                stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH):
        chars.append("+" if mode & who else "-")
    # Replace x with s/t for setuid/setgid/sticky
    if mode & stat.S_ISUID:
        chars[2] = "s" if chars[2] == "+" else "S"
    if mode & stat.S_ISGID:
        chars[5] = "s" if chars[5] == "+" else "S"
    if mode & stat.S_ISVTX:
        chars[8] = "t" if chars[8] == "+" else "T"
    return "".join(chars)


def owner_name(uid: int) -> str:
    try:   return pwd.getpwuid(uid).pw_name
    except KeyError: return str(uid)


def group_name(gid: int) -> str:
    try:   return grp.getgrgid(gid).gr_name
    except KeyError: return str(gid)


def get_acl(path: str) -> Optional[str]:
    """Return raw getfacl output, or None if unavailable."""
    try:
        if sys.platform == "darwin":
            result = subprocess.run(
                # ["getfacl", "--omit-header", "--absolute-names", path],
                ["ls", "-le", path],
                capture_output=True, text=True, timeout=5
            )
            lines = result.stdout.splitlines()

            acl_entries = []

            for line in lines:
                line = line.strip()
                if line.startswith(tuple(str(i) + ":" for i in range(10))):
                    # Example line: "0: user:azad allow read,write"
                    parts = line.split("user:")
                    if len(parts) > 1:
                        acl_part = parts[1].strip()
                        acl_entries.append(acl_part)

            return ";".join(acl_entries) if acl_entries else None
        else:
            result = subprocess.run(
                ["getfacl", "--omit-header", "--absolute-names", path],
                capture_output=True, text=True, timeout=5
            )
            return result.stdout.strip() or None
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    

def set_acl(path: str, acl_string: str) -> Optional[str]:
    """Return raw getfacl output, or None if unavailable."""
    acl_list = acl_string.split(";")
    for acl_entry in acl_list:
        try:
            if len(acl_entry) == 0:
                continue
            acls = acl_entry.split()
            acl_perm = "user:" + acls[0] + " allow " + acls[2]
            print(acl_perm)
            result = subprocess.run(
                ["chmod", "+a", acl_perm, path],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode != 0:
                print(f"Failed to set ACL entry '{acl_entry}' on {path}: {result.stderr}")
        except Exception as e:
            print(f"Error setting ACL entry '{acl_entry}' on {path}: {e}")


def get_xattrs(path: str) -> Optional[str]:
    """Return JSON dict of extended attributes, or None."""
    try:
        import xattr  # pyxattr
        attrs = {}
        for key in xattr.listxattr(path, symlink=True):
            try:
                val = xattr.getxattr(path, key, symlink=True)
                attrs[key] = val.decode("utf-8", errors="replace")
            except OSError:
                attrs[key] = "<unreadable>"
        return json.dumps(attrs) if attrs else None
    except ImportError:
        # Fallback: use getfattr
        try:
            result = subprocess.run(
                ["getfattr", "-d", "-m", "-", "--absolute-names", path],
                capture_output=True, text=True, timeout=5
            )
            return result.stdout.strip() or None
        except FileNotFoundError:
            return None
    except OSError:
        return None


def get_security_context(path: str) -> Optional[str]:
    """Return SELinux/AppArmor security context if available."""
    try:
        result = subprocess.run(
            ["ls", "-Z", path],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            parts = result.stdout.strip().split()
            # ls -Z format: context file  (or just file if no context)
            if len(parts) >= 2 and ":" in parts[0]:
                return parts[0]
        return None
    except Exception:
        return None


def md5_hash(path: str, chunk_size: int = 1 << 20) -> Optional[str]:
    """Stream-hash a file in 1 MB chunks. Returns None on error."""
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, OSError):
        return None


def collect_metadata(abs_path: str, root_folder: str) -> dict:
    """
    Gather all available metadata for a single path.

    Returns a dict with two logical groups:
      • lstat  — raw os.lstat() result stored as a nested dict (→ JSON column)
      • all other keys — each stored in its own column
    """
    rel_path = os.path.relpath(abs_path, root_folder)

    try:
        s = os.lstat(abs_path)
    except OSError as e:
        return {"relative_path": rel_path, "error": str(e)}

    mode  = s.st_mode
    ftype = file_type_str(mode)

    # ── lstat dict (raw kernel values only) ──────────────────────────────────
    lstat_dict = {
        "st_mode":    mode,
        "st_ino":     s.st_ino,
        "st_dev":     s.st_dev,
        "st_nlink":   s.st_nlink,
        "st_uid":     s.st_uid,
        "st_gid":     s.st_gid,
        "st_size":    s.st_size,
        "st_atime":   s.st_atime,
        "st_mtime":   s.st_mtime,
        "st_ctime":   s.st_ctime,
        "st_blocks":  getattr(s, "st_blocks", None),
        "st_blksize": getattr(s, "st_blksize", None),
    }

    # ── all other (derived / external) attributes — own columns ──────────────
    entry = {
        "relative_path": rel_path,
        "absolute_path": abs_path,
        "file_name":     os.path.basename(abs_path),
        "file_type":     ftype,

        # serialised lstat dict → stored as TEXT (JSON) in a single column
        "lstat": json.dumps(lstat_dict),

        # human-readable permission strings
        "permissions_octal": oct(stat.S_IMODE(mode)),
        "permissions_str":   permission_str(mode),
        "owner_name":        owner_name(s.st_uid),
        "group_name":        group_name(s.st_gid),

        # special-bit flags
        "setuid": int(bool(mode & stat.S_ISUID)),
        "setgid": int(bool(mode & stat.S_ISGID)),
        "sticky": int(bool(mode & stat.S_ISVTX)),

        # external / subprocess-derived attributes
        "acl_text":         get_acl(abs_path),
        "xattrs":           get_xattrs(abs_path),
        "security_context": get_security_context(abs_path),
        "symlink_target":   os.readlink(abs_path) if ftype == "symlink" else None,

        # content hash — regular files only
        "md5_hash": md5_hash(abs_path) if ftype == "file" else None,

        # convenience shortcut used by walk_folder aggregation (not stored)
        "_st_size": s.st_size,
    }
    print(f"ACL text for {rel_path}: {entry['acl_text']}")
    return entry


# def walk_folder(root_folder: str) -> list[dict]:
#     """Recursively collect metadata for every entry under root_folder."""
#     entries = []
#     skip_dirs = {DB_NAME, SNAPSHOTS_DIR}

#     for dirpath, dirnames, filenames in os.walk(root_folder, followlinks=False):
#         # Prune internal bookkeeping dirs
#         dirnames[:] = [d for d in dirnames if d not in skip_dirs]

#         # Include the directory entry itself (except root)
#         if dirpath != root_folder:
#             entries.append(collect_metadata(dirpath, root_folder))

#         for fname in filenames:
#             if fname == DB_NAME:
#                 continue
#             abs_path = os.path.join(dirpath, fname)
#             entries.append(collect_metadata(abs_path, root_folder))

#     return entries
