"""Abstract base class for the handling plugins."""

from abc import ABC, abstractmethod

class PluginDriver(ABC):
    """Plugin driver for DSI core product.

    A driver object must implement an __init__ method that
    prepares a user-defined plugin to convert arbitrary input
    to Python built-in data structures.
    """

    @abstractmethod
    def __init__(self, path):
        """Initialize Plugin setup.

        Read a Plugin file. Return a Plugin object or throw error.
        """

    @abstractmethod
    def parse(self):
        """Convert input to Python built-in data structure.

        Initial Plugin design supports file post-processing and
        regular schema data only.
        """

    @abstractmethod
    def add_row(self, collection):
        """Add to the plugin's collection.

        Initial Plugin design supports adding rows to an existing
        object, and throws an error if columns don't match previous.
        """
