from dataclasses import dataclass
from stat import S_IRWXU, S_IRWXG, S_IRWXO
from os import stat


@dataclass(eq=True, frozen=True)
class Permission:
    """ A simple dataclass to represent POSIX file permissions """
    uid: int
    gid: int
    settings: str

    def __iter__(self):
        """ enables conversion to tuple, list, etc. """
        for v in (self.uid, self.gid, self.settings):
            yield v


class PermissionsManager:
    """
    A class to handle and register the mapping from columns
    to their permissions. Uses flyweights so that each unique
    permission is shared and only stored in memory once.
    """

    def __init__(self):
        self.perms_collection = {}
        self.column_perms = {}

    def get_perm(self, uid, gid, settings) -> Permission:
        """ If a perm already exists, return it. Else create it. """
        if (uid, gid, settings) in self.perms_collection:
            return self.perms_collection[(uid, gid, settings)]
        perm = Permission(uid, gid, settings)
        self.perms_collection[(uid, gid, settings)] = perm
        return perm

    def register_columns(self, keys: list[str], perm: Permission) -> None:
        """ Links a list of column names to a given permission """
        if tuple(perm) not in self.perms_collection:
            raise PermissionNotFoundError(perm)
        for key in keys:
            self.column_perms[key] = perm

    def register_columns_with_file(self, keys: list[str], fp: str) -> None:
        uid, gid, settings = (None, None, None) if fp is None \
            else self.get_file_permissions(fp)
        perm = self.get_perm(uid, gid, settings)
        self.register_columns(keys, perm)

    def get_column_perms(self, key: str) -> Permission:
        """ Get the Permission of a given column """
        try:
            return self.column_perms[key]
        except KeyError:
            raise ColumnNotRegisteredError(key)

    def get_file_permissions(self, fpath: str) -> tuple[int, int, str]:
        """ Given a filepath, returns (uid, gid, settings) """
        st = stat(fpath)  # includes info on filetype, perms, etc.
        uid = st.st_uid
        gid = st.st_gid
        perm_mask = S_IRWXU | S_IRWXG | S_IRWXO  # user | group | other
        settings = oct(st.st_mode & perm_mask)  # select perm bits from st_mode
        return (uid, gid, settings)


class PermissionNotFoundError(Exception):
    def __init__(self, perm: Permission) -> None:
        self.msg = f"Permission {perm} not found. Make sure to use get_perm instead of " + \
            "manually instantiating a Permission to register."
        super().__init__(self.msg)


class ColumnNotRegisteredError(Exception):
    def __init__(self, key: str) -> None:
        self.msg = f"Permission for column {key} not registered. Be sure to `register_columns`."
        super().__init__(self.msg)
