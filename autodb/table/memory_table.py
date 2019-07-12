import pickle
from typing import List, Optional, Generator, Set

from autodb.abc.table import Table
from ..index.memory_index import MemoryIndex


class MemoryTable(Table):
    """
    This class stores elements of a certain class type within a regular Python list.
    Object attributes are indexed and stored within indexes that can then be used to retrieve
    objects from the list.
    """

    def __init__(self) -> None:
        self.size = 0
        self.table: List[object] = []
        self.pickle_table: List[Optional[bytes]] = []
        self.unused_indexes: Set[int] = set()
        self.index_manager = MemoryIndex()

    def __repr__(self):
        return f"Table(size={self.size})"

    def __len__(self):
        return self.size

    def insert(self, item: object) -> None:
        byte_repr = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
        if len(self.unused_indexes) > 0:
            index = self.unused_indexes.pop()
            self.table[index] = item
            self.pickle_table[index] = byte_repr
        else:
            index = len(self.table)
            self.table.append(item)
            self.pickle_table.append(byte_repr)
        self.size += 1
        self.index_manager.index_item(item, index)

    def retrieve(self, **kwargs) -> [Generator[object, None, None]]:
        if len(kwargs) == 0:
            raise ValueError
        indexes = self.index_manager.retrieve(**kwargs)
        if indexes:
            return (pickle.loads(self.pickle_table[index]) for index in indexes)
        else:
            return ([])

    def retrieve_all(self):
        return (pickle.loads(item) for index, item in enumerate(self.pickle_table) if index not in self.unused_indexes)

    def retrieve_valid_indexes(self):
        return list(self.index_manager.index_map.keys())

    def delete(self, **kwargs):
        if len(kwargs) == 0:
            raise ValueError
        indexes_to_delete = self.index_manager.retrieve(**kwargs)
        self._delete(indexes_to_delete)

    def _delete(self, indexes) -> None:
        if indexes:
            for index in indexes:
                item = self.table[index]
                self.index_manager.unindex_item(item, index)
                self.table[index] = None
                self.pickle_table[index] = None
            self.unused_indexes.update(indexes)
            self.size -= len(indexes)

    def clear(self):
        indexes_to_delete = (index for index in range(len(self.table)) if index not in self.unused_indexes)
        self._delete(indexes_to_delete)
