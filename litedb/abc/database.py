from abc import ABC, abstractmethod

from .table import Table


class Database(ABC):
    """
    This is a base class that provides the basic functionality
    necessary for an AutoDB implementation.
    """

    @abstractmethod
    def __len__(self):
        """
        Should return the sum of all the objects
        stored in this database.
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def __iter__(self):
        """
        Should allow iteration of the class types contained
        within this database.
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def __repr__(self):
        """
        Should print out a summary of all of the object
        tables.
        :return:
        """
        raise NotImplemented

    @property
    @abstractmethod
    def tables(self):
        """
        Should return a view of all of the tables in this database.
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def insert(self, item):
        """
        Inserts and indexes arbitrary Python objects.
        :param item:
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def select(self, cls) -> Table:
        """
        Select items of type `cls`.
        :param cls:
        :return:
        """
        raise NotImplemented
