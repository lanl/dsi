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
        regular schema data only. pack_header should only be executed once,
        along with the first call of add_row. This allows supplemental methods
        to alter the column structure up the last possible moment, when metadata
        are prepared for movement to the middleware.
        """

    @abstractmethod
    def add_row(self, collection):
        """Add a sample to the plugin's row collection.

        A call to add_row should fail tests if column count differs
        from previous.
        """
