"""Abstract base class for the handling plugins."""

from abc import ABC, abstractmethod

class Plugin(ABC):
    """Plugin abstract class for DSI core product.

    A Plugin connects a data producer to a compatible middleware
    data structure.
    """

    @abstractmethod
    def __init__(self, path):
        """Initialize Plugin setup.

        Read a Plugin file. Return a Plugin object.
        """

    @abstractmethod
    def pack_header(self):
        """Establish column names for a structured metadata Plugin.

        Initial Plugin design supports file post-processing and
        regular schema data only.
        """

    @abstractmethod
    def add_row(self, collection):
        """Add a sample to the plugin's row collection.

        Initial Plugin design supports adding rows to an existing
        object, and well fail tests if column count differ from 
        previous.
        """
