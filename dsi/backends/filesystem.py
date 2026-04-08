from dsi.backends import Backend

class Filesystem(Backend):
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