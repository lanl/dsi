from abc import ABCMeta, abstractmethod


class Backend(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, filename) -> None:
        pass

    @property
    @abstractmethod
    def git_commit_sha(self):
        pass

    @abstractmethod
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass

    @abstractmethod
    def get_artifacts(self, query):
        pass

    @abstractmethod
    def inspect_artifacts(self):
        pass


class Filesystem(Backend):
    git_commit_sha = '5d79e08d4a6c1570ceb47cdd61d2259505c05de9'
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

    def __init__(self, filename) -> None:
        pass

    def put_artifacts(self, artifacts, kwargs) -> None:
        pass

    def get_artifacts(self, query):
        pass

    def inspect_artifacts(self):
        pass
