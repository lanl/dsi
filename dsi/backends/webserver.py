from dsi.backends import Backend

class Webserver(Backend):
    # Declare named types
    DOUBLE = "DOUBLE"
    STRING = "VARCHAR"
    FLOAT = "FLOAT"
    INT = "INT"

    def __init__(self) -> None:
        # Need to define webserver generic-all input
        pass

    def ingest_artifacts(self, artifacts, kwargs) -> None:
        pass

    def query_artifacts(self, query, kwargs):
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