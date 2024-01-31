from abc import ABCMeta, abstractmethod

class Plugin(metaclass=ABCMeta):
    """Plugin abstract class for DSI core product.

    A Plugin connects a data reader or writer to a compatible middleware
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