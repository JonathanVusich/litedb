from abc import ABC, abstractmethod


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
    def __repr__(self):
        """
        Should print out a summary of all of the object
        tables.
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
    def retrieve(self, class_type=None, **kwargs):
        """
        Retrieves objects from the database based on the given
        class_type and the given queries.
        :param class_type:
        :param kwargs:
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def delete(self, class_type=None, **kwargs):
        """
        Deletes objects from the database based on the given
        class_type and the given queries.
        :param class_type:
        :param kwargs:
        :return:
        """
        raise NotImplemented
