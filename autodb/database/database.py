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
    def retrieve(self, cls, **kwargs):
        """
        Retrieves objects from the database based on the given
        class_type and the given queries.
        :param cls:
        :param kwargs:
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def retrieve_all(self):
        """
        Retrieve all objects from the database sequentially.
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def retrieve_valid_indexes(self, cls):
        """
        Retrieve the valid indexes for the objects of type 'cls'.
        :param cls:
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def delete(self, cls, **kwargs):
        """
        Deletes objects from the database based on the given
        class_type and the given queries.
        :param cls:
        :param kwargs:
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def clear(self):
        """
        Delete all objects from the database.
        :return:
        """
        raise NotImplemented
