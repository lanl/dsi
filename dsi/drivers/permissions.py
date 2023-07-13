from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Permission:
    uid: int
    gid: int
    settings: str


class PermissionsManager:
    def __init__(self):
        self.perms_collection = {}

    def get(self, uid: int, gid: int, settings: str) -> Permission:
        if (uid, gid, settings) in self.perms_collection:
            return self.perms_collection[(uid, gid, settings)]
        perm = Permission(uid, gid, settings)
        self.perms_collection[(uid, gid, settings)] = perm
        return perm
