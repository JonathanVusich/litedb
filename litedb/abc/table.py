from abc import ABC, abstractmethod


class Table(ABC):

    @abstractmethod
    def __repr__(self):
        raise NotImplemented

    @abstractmethod
    def __len__(self):
        raise NotImplemented

    @abstractmethod
    def retrieve(self, **kwargs):
        """Filters search results."""
        raise NotImplemented

    @abstractmethod
    def retrieve_all(self):
        """Returns a generator over all of the items in the table."""
        raise NotImplemented

    @property
    @abstractmethod
    def indexes(self):
        """Retrieves all of the valid indexes for this type."""
        raise NotImplemented

    @abstractmethod
    def delete(self, **kwargs):
        """Deletes all items in this table based on the parameters."""
        raise NotImplemented

    @abstractmethod
    def clear(self):
        """Removes all items from this table."""
        raise NotImplemented
