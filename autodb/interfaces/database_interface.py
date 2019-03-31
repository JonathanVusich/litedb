from typing import List
from abc import ABC, abstractmethod


class DatabaseInterface(ABC):
    """Provides an easy interfaces for a user to store and retrieve objects by value."""

    @abstractmethod
    def insert(self, complex_object: object) -> bool:
        """Allows a user to insert arbitrary data into the index storage."""
        pass

    @abstractmethod
    def retrieve(self, class_type: object, **kwargs) -> List[object]:
        """Allows a user to retrieve data of a certain class type by specifying search attributes."""
        pass
