#!/usr/bin/python

from abc import ABC, abstractmethod
from typing import Any


class Plugin(ABC):
    """
    Abstract class to implement plugins.
    A Plugin provides functionality to ingest some modality of data
    """

    def __init__(self):
        """
        Empty Parent Constructor
        """
        pass

    @abstractmethod
    def read_data(self, **kwargs) -> Any:
        """
        Abstract method for reading data from the file 
        system and giving a usable representation to the core.

        `return`: Any, some object readable by the core
        """
        pass
