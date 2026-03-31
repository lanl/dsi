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
    def ingest_artifacts(self, artifacts, kwargs) -> None:
        pass

    @abstractmethod
    def query_artifacts(self, query, kwargs):
        pass

    @abstractmethod
    def get_table(self, table_name, kwargs):
        pass

    @abstractmethod
    def notebook(self, kwargs):
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

    @abstractmethod
    def find_relation(self, column_name, relation, kwargs):
        pass

    @abstractmethod
    def list(self, kwargs):
        pass

    @abstractmethod
    def num_tables(self, kwargs):
        pass

    @abstractmethod
    def display(self, table_name, kwargs):
        pass

    @abstractmethod
    def summary(self, table_name, kwargs):
        pass

    @abstractmethod
    def overwrite_table(self, table_name, collection, kwargs):
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

    # Declare comparison types
    GT = ">"
    LT = "<"
    EQ = "="

    def __init__(self, filename) -> None:
        pass

    def ingest_artifacts(self, artifacts, kwargs) -> None:
        pass

    def query_artifacts(self, query, kwargs):
        pass

    def get_table(self, table_name, kwargs):
        pass

    def notebook(self, kwargs):
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

    def find_relation(self, column_name, relation, kwargs):
        pass

    def list(self, kwargs):
        pass

    def num_tables(self, kwargs):
        pass

    def display(self, table_name, kwargs):
        pass

    def summary(self, table_name, kwargs):
        pass

    def overwrite_table(self, table_name, collection, kwargs):
        pass