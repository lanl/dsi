from collections import OrderedDict
from abc import ABCMeta, abstractmethod


class Plugin(metaclass=ABCMeta):
    """Plugin abstract class for DSI core product.

    A Plugin connects a data producer to a compatible middleware
    data structure.
    """

    @abstractmethod
    def __init__(self, path):
        """Initialize Plugin setup.

        Read a Plugin file. Return a Plugin object.
        """

    @property
    @abstractmethod
    def git_commit_sha(self):
        pass

    @abstractmethod
    def add_to_output(self, path):
        """Initialize Plugin setup.

        Read a Plugin file. Return a Plugin object.
        """


class StructuredMetadata(Plugin):
    """ plugin superclass that provides handy methods for structured data """
    git_commit_sha: str = '5d79e08d4a6c1570ceb47cdd61d2259505c05de9'

    def __init__(self):
        """
        Initializes a StructuredDataPlugin with an output collector
        and an initially unset column count.
        """
        self.output_collector = OrderedDict()
        self.column_cnt = None  # schema not set until pack_header
        self.validation_model = None  # optional pydantic Model

    def set_schema(self, column_names: list, validation_model=None) -> None:
        """
        Initializes columns in the output_collector and column_cnt.
        Useful in a plugin's pack_header method.
        """
        for name in column_names:
            self.output_collector[name] = []
        self.column_cnt = len(column_names)
        self.validation_model = validation_model

    def add_to_output(self, row: list) -> None:
        """
        Adds a row of data to the output_collector and guarantees good structure.
        Useful in a plugin's add_rows method.
        """
        if not self.schema_is_set():
            raise RuntimeError("pack_header must be done before add_row")
        if self.validation_model is not None:
            row_dict = {k: v for k, v in zip(
                self.output_collector.keys(), row)}
            self.validation_model.model_validate(row_dict)
        elif len(row) != self.column_cnt:
            raise RuntimeError("Incorrect length of row was given")

        for key, row_elem in zip(self.output_collector.keys(), row):
            self.output_collector[key].append(row_elem)

    def schema_is_set(self) -> bool:
        """ Helper method to see if the schema has been set """
        return self.column_cnt is not None
