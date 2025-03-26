from abc import ABCMeta, abstractmethod


class Backend(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, filename) -> None:
        pass

    @property
    @abstractmethod
    def git_commit_sha(self):
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE ingest_artifacts()
    @abstractmethod
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass
    
    @abstractmethod
    def ingest_artifacts(self, artifacts, kwargs) -> None:
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE query_artifacts()
    @abstractmethod
    def get_artifacts(self, query, kwargs):
        pass

    @abstractmethod
    def query_artifacts(self, query, kwargs):
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE notebook()
    @abstractmethod
    def inspect_artifacts(self, kwargs):
        pass

    @abstractmethod
    def notebook(self, kwargs):
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE process_artifacts()
    @abstractmethod
    def read_to_artifacts(self, kwargs):
        pass

    @abstractmethod
    def process_artifacts(self, kwargs):
        pass

    @abstractmethod
    def find(self, query_object, kwargs):
        pass

    @abstractmethod
    def find_table(self, query_object, kwargs):
        pass

    @abstractmethod
    def find_column(self, query_object, kwargs):
        pass

    @abstractmethod
    def find_cell(self, query_object, kwargs):
        pass

    @abstractmethod
    def close(self):
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

    # DEPRECATING IN FUTURE DSI RELEASE. USE ingest_artifacts()
    def put_artifacts(self, artifacts, kwargs) -> None:
        pass

    def ingest_artifacts(self, artifacts, kwargs) -> None:
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE query_artifacts()
    def get_artifacts(self, query, kwargs):
        pass

    def query_artifacts(self, query, kwargs):
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE notebook()
    def inspect_artifacts(self, kwargs):
        pass

    def notebook(self, kwargs):
        pass

    # DEPRECATING IN FUTURE DSI RELEASE. USE process_artifacts()
    def read_to_artifacts(self, kwargs):
        pass

    def process_artifacts(self, kwargs):
        pass

    def find(self, query_object, kwargs):
        pass

    def find_table(self, query_object, kwargs):
        pass

    def find_column(self, query_object, kwargs):
        pass

    def find_cell(self, query_object, kwargs):
        pass

    def close(self):
        pass