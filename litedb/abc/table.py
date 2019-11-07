from abc import ABC, abstractmethod


class Table(ABC):

    @abstractmethod
    def __len__(self):
        """Returns the number of items in this table."""
        raise NotImplemented

    @abstractmethod
    def __iter__(self):
        """Returns a generator of all the items in this table."""
        raise NotImplemented

    @abstractmethod
    def __repr__(self):
        """Should return the number of items in this table."""
        raise NotImplemented

    @property
    @abstractmethod
    def indexes(self):
        """Retrieves all of the valid indexes for this type."""
        raise NotImplemented

    @abstractmethod
    def retrieve(self, **kwargs):
        """Filters search results."""
        raise NotImplemented

    @abstractmethod
    def delete(self, **kwargs):
        """Deletes all items in this table based on the parameters."""
        raise NotImplemented

    @abstractmethod
    def clear(self):
        """Removes all items from this table."""
        raise NotImplemented
