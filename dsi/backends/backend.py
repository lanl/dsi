from abc import ABCMeta, abstractmethod


class Backend(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self) -> None:
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