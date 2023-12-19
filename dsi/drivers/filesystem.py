from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from dsi.permissions.permissions import PermissionsManager


class Driver(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, filename) -> None:
        pass

    @property
    @abstractmethod
    def git_commit_sha(self):
        pass

    @abstractmethod
    def put_artifacts(self, artifacts, **kwargs) -> None:
        pass

    @abstractmethod
    def get_artifacts(self, query):
        pass

    @abstractmethod
    def inspect_artifacts(self):
        pass


class Filesystem(Driver):
    git_commit_sha: str = "5d79e08d4a6c1570ceb47cdd61d2259505c05de9"
    # Declare named types
    DOUBLE = "DOUBLE"
    STRING = "VARCHAR"
    FLOAT = "FLOAT"
    INT = "INT"

    # Declare store types
    GUFI_STORE = "gufi"
    SQLITE_STORE = "sqlite"
    PARQUET_STORE = "parquet"

    # Declare comparison types
    GT = ">"
    LT = "<"
    EQ = "="

    def __init__(self, filename: str, perms_manager: PermissionsManager) -> None:
        self.filename = filename
        self.perms_manager = perms_manager

    def put_artifacts(self, artifacts, **kwargs) -> None:
        pass

    def get_artifacts(self, query):
        pass

    def inspect_artifacts(self):
        pass

    def write_files(
        self,
        collection,
        write_func: Callable[[dict[str, list], str], None],
        f_basename: str,
        f_ext: str,
    ) -> None:
        """
        Write out a given collection to multiple files, one per
        unique permission. File are written with `write_func`
        and those files are set to their corresponding permission.
        """
        f_map = self.get_output_file_mapping(f_basename, f_ext)
        for f, cols in f_map.items():  # Write one file for each unique permission
            col_to_data = {col: collection[col] for col in cols}
            write_func(col_to_data, f)
        self.perms_manager.set_file_permissions(f_map)

    def get_output_file_mapping(
        self, base_filename: str, file_ext: str
    ) -> dict[str, list[str]]:
        """
        Given a base filename and extention, returns a mapping from filename
        to a list of corresponding columns. Each filename encodes permissions.
        """
        perms_to_cols = self.perms_manager.get_permission_columnlist_mapping()
        return {
            (base_filename + "-" + str(perm) + file_ext): cols
            for perm, cols in perms_to_cols.items()
        }
