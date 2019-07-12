from abc import ABC, abstractmethod


class Table(ABC):

    @abstractmethod
    def __repr__(self):
        raise NotImplemented

    @abstractmethod
    def __len__(self):
        raise NotImplemented

    @abstractmethod
    def insert(self, item):
        raise NotImplemented

    @abstractmethod
    def retrieve(self, **kwargs):
        raise NotImplemented

    @abstractmethod
    def retrieve_all(self):
        raise NotImplemented

    @abstractmethod
    def retrieve_valid_indexes(self):
        raise NotImplemented

    @abstractmethod
    def delete(self, **kwargs):
        raise NotImplemented

    @abstractmethod
    def clear(self):
        raise NotImplemented
