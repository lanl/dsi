from abc import ABC, abstractmethod
from dsi.backends import Backend
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

class Filesystem(Backend, ABC):
    @abstractmethod
    def __init__(self, filename, **kwargs) -> None:
        pass

    # Can raise NotImplementedError for a read-only backend
    @abstractmethod
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        pass
    
    @abstractmethod
    def query_artifacts(self, query, **kwargs):
        pass

    @abstractmethod
    def get_table(self, table_name, **kwargs):
        pass

    @abstractmethod
    def process_artifacts(self, **kwargs):
        pass

    @abstractmethod
    def get_schema(self):
        pass
    
    ## DEPRECATING SOON
    @abstractmethod
    def find(self, query_object, **kwargs):
        pass
    
    ## DEPRECATING SOON
    @abstractmethod
    def find_table(self, query_object, **kwargs):
        pass

    ## DEPRECATING SOON
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
    def num_tables(self, **kwargs):
        pass

    @abstractmethod
    def display(self, table_name, **kwargs):
        pass

    @abstractmethod
    def summary(self, table_name, **kwargs):
        pass

    # Can raise NotImplementedError for a read-only backend
    @abstractmethod
    def overwrite_table(self, table_name, collection, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass

    def check_sql(self, sql: str, dialect: str) -> tuple[bool, str]:
        if dialect.lower() not in ["sqlite", "duckdb"]:
            raise ValueError("Only checking SQL queries for Sqlite and DuckDB")
        try:
            statements = [
                statement
                for statement in sqlglot.parse(sql, read=dialect.lower())
                if statement is not None
            ]
        except (ParseError, TokenError):
            return False

        if len(statements) == 0:
            return False
        error_msg = None
        if len(statements) != 1:
            error_msg = "Can only enter one SQL query at a time."

        statement = statements[0].unnest()

        # Recognize common SQL statements before checking permissions.
        # Extend this tuple if you need to recognize additional commands.
        recognized_statement_types = (
            exp.Query,
            exp.DML,
            exp.DDL,
            exp.Alter,
            exp.Drop,
            exp.Pragma,
            exp.Set,
            exp.Transaction,
            exp.Commit,
            exp.Rollback,
        )

        if not isinstance(statement, recognized_statement_types):
            return False

        if error_msg is not None:
            return False, error_msg

        command = statement.key.upper()

        select_types = (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Pragma, exp.Update, exp.Alter)
        if not isinstance(statement, select_types):
            return True, command

        return True, command