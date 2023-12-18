from collections import defaultdict
from dataclasses import dataclass
from stat import S_IRWXU, S_IRWXG, S_IRWXO
from os import stat, getuid, getgid, chown, chmod


@dataclass(eq=True, frozen=True)
class Permission:
    """A simple dataclass to represent POSIX file permissions"""

    uid: int
    gid: int
    settings: str

    def __iter__(self):
        """enables conversion to tuple, list, etc."""
        for v in (self.uid, self.gid, self.settings):
            yield v

    def __str__(self):
        return f"{self.uid}-{self.gid}-{self.settings}"


class PermissionsManager:
    """
    A class to handle and register the mapping from columns
    to their permissions. Uses flyweights so that each unique
    permission is shared and only stored in memory once.
    """

    def __init__(self, allow_multiple_permissions=False, squash_permissions=False):
        self.perms_collection = {}
        self.column_perms = {}
        self.allow_multiple_permissions = allow_multiple_permissions
        self.squash_permissions = squash_permissions

    def get_perm(self, uid, gid, settings) -> Permission:
        """If a perm already exists, return it. Else create it."""
        if (uid, gid, settings) in self.perms_collection:
            return self.perms_collection[(uid, gid, settings)]
        perm = (
            Permission(uid, gid, settings)
            if not self.squash_permissions
            else Permission(*self.get_process_permissions())
        )
        self.perms_collection[(uid, gid, settings)] = perm
        return perm

    def register_columns(self, keys: list[str], perm: Permission) -> None:
        """Links a list of column names to a given permission"""
        if tuple(perm) not in self.perms_collection:
            raise PermissionNotFoundError(perm)
        for key in keys:
            self.column_perms[key] = perm

    def register_columns_with_file(self, keys: list[str], fp: str) -> None:
        """Gets a file's Permission and links it to the given columns"""
        uid, gid, settings = (
            self.get_process_permissions()
            if fp is None or self.squash_permissions
            else self.get_file_permissions(fp)
        )
        perm = self.get_perm(uid, gid, settings)
        self.register_columns(keys, perm)

    def get_permission_columnlist_mapping(self) -> dict[Permission, list[str]]:
        """
        Returns a mapping from unique Permission to list of columns.
        """
        mapping = defaultdict(list)
        for col, perm in self.column_perms.items():
            mapping[perm].append(col)
        return mapping

    def get_column_perms(self, key: str) -> Permission:
        """Get the Permission of a given column"""
        try:
            return self.column_perms[key]
        except KeyError:
            raise ColumnNotRegisteredError(key)

    def get_file_permissions(self, fpath: str) -> tuple[int, int, str]:
        """Given a filepath, returns (uid, gid, settings)"""
        st = stat(fpath)  # includes info on filetype, perms, etc.
        uid = st.st_uid
        gid = st.st_gid
        perm_mask = S_IRWXU | S_IRWXG | S_IRWXO  # user | group | other
        settings = oct(st.st_mode & perm_mask)  # select perm bits from st_mode
        return (uid, gid, settings)

    def set_file_permissions(self, file_mapping: dict[str, list[str]]) -> None:
        """
        Given a mapping from filename to list of columns, set each file
        to its column's permissions. (All columns of a file share perms)
        """
        for filename, cols in file_mapping.items():
            f_perm = self.get_column_perms(cols[0])  # cols share same perms
            uid, gid, settings = tuple(f_perm)
            chown(filename, uid, gid)  # type: ignore
            chmod(filename, int(settings, base=8))  # type: ignore

    def get_process_permissions(self) -> tuple[int, int, str]:
        """
        In the event of data not coming from a file,
        default to (uid, egid, 660)
        """
        uid = getuid()
        egid = getgid()
        return (uid, egid, "0o660")

    def has_multiple_permissions(self) -> bool:
        """
        Returns whether or not the collection has multiple permissions.
        """
        return len(self.perms_collection.keys()) > 1


class PermissionNotFoundError(Exception):
    def __init__(self, perm: Permission) -> None:
        self.msg = (
            f"Permission {perm} not found. Make sure to use get_perm instead of "
            + "manually instantiating a Permission to register."
        )
        super().__init__(self.msg)


class ColumnNotRegisteredError(Exception):
    def __init__(self, key: str) -> None:
        self.msg = f"Permission for column {key} not registered. Be sure to `register_columns`."
        super().__init__(self.msg)
