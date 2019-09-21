from abc import ABC, abstractmethod
from typing import Optional, Set


class IndexManager(ABC):

    @abstractmethod
    def index_item(self, item: object, index: int) -> None:
        """
        Inserts/creates index tables based on the given item and index.
        :param item
        :param index
        """
        raise NotImplemented

    @abstractmethod
    def unindex_item(self, item: object, index: int) -> None:
        """
        Removes indexes based on the given item and index.
        :param item:
        :param index:
        :return:
        """
        raise NotImplemented

    @abstractmethod
    def retrieve(self, **kwargs) -> Optional[Set[int]]:
        """
        Retrieves item indexes based on the given search parameters.
        :param kwargs:
        :return:
        """
        raise NotImplemented
