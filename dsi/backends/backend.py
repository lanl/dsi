from abc import ABC, abstractmethod

class Backend(ABC):
    @abstractmethod
    def __init__(self, data_source, **kwargs) -> None:
        pass

    @abstractmethod
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        pass

    @abstractmethod
    def query_artifacts(self, query, **kwargs):
        pass

    @abstractmethod
    def notebook(self, **kwargs):
        pass

    @abstractmethod
    def find(self, query_object, **kwargs):
        pass

    @abstractmethod
    def find_table(self, query_object, **kwargs):
        pass

    @abstractmethod
    def find_column(self, query_object, **kwargs):
        pass

    @abstractmethod
    def find_cell(self, query_object, **kwargs):
        pass

    @abstractmethod
    def find_relation(self, column_name, relation, **kwargs):
        pass

    @abstractmethod
    def list(self, **kwargs):
        pass

    @abstractmethod
    def display(self, table_name, **kwargs):
        pass

    @abstractmethod
    def summary(self, table_name, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass