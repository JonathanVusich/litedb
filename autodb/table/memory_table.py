import pickle
from typing import List, Set, Dict, Optional, Generator

from ..errors import InvalidRange
from ..index import Index
from ..utils.object_utils import retrieve_possible_object_indexes
from .table import Table


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
        self.unused_indexes: List[int] = []
        self.index_map: Dict[str, Index] = {}
        self.index_blacklist: Set[str] = set()

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
        self._index_item(item, index)

    def retrieve(self, **kwargs) -> [Generator[object, None, None]]:
        if len(kwargs) == 0:
            return (pickle.loads(item) for item in self.pickle_table if item is not None)
        indexes = self._retrieve(**kwargs)
        if indexes:
            return (pickle.loads(self.pickle_table[index]) for index in indexes)
        else:
            return ([])

    def delete(self, **kwargs):
        indexes_to_delete = self._retrieve(**kwargs)
        if indexes_to_delete:
            for index in indexes_to_delete:
                self._delete(index)

    def _index_item(self, item: object, index: int) -> None:
        """Inserts/creates index tables based on the given object."""
        indexes = retrieve_possible_object_indexes(item)
        for var_name, value in indexes.items():
            if var_name in self.index_blacklist:
                continue
            if var_name not in self.index_map:
                self.index_map.update({var_name: Index(type(value))})
            try:
                self.index_map[var_name].add(value, index)
            except TypeError:
                self.index_map.pop(var_name)
                self.index_blacklist.add(var_name)

    def _unindex_item(self, item: object, index: int) -> None:
        """Removes indexes for the given object."""
        indexes = retrieve_possible_object_indexes(item)
        for var_name, value in indexes.items():
            if var_name in self.index_blacklist:
                continue
            self.index_map[var_name].destroy(value, index)

    def _retrieve(self, **kwargs) -> Optional[Set[int]]:
        indexes: Set[int] = set()
        for x, key in enumerate(kwargs.keys()):
            if key in self.index_blacklist or key not in self.index_map:
                raise IndexError(f"{key} is not a valid index!")
            index = self.index_map[key]
            if len(index) == 0:
                continue
            value = kwargs[key]
            if isinstance(value, tuple):
                if len(value) != 2:
                    raise InvalidRange
                low, high = value
                if low is not None and not isinstance(low, index.index_type):
                    raise ValueError(f"The low value of \"{key}\" must be of type {index.index_type}")
                if high is not None and not isinstance(high, index.index_type):
                    raise ValueError(f"The high value of \"{key}\" must be of type {index.index_type}")
                if x == 0:
                    indexes.update(index.retrieve_range(low, high))
                else:
                    indexes.intersection_update(index.retrieve_range(low, high))
            else:
                if value is not None and not isinstance(value, index.index_type):
                    raise ValueError(f"\"{key}\" must be of type {index.index_type}")
                results = index.retrieve(value)
                if x == 0:
                    indexes.update(results)
                else:
                    indexes.intersection_update(results)
        if len(indexes) > 0:
            return indexes

    def _delete(self, object_index: int) -> None:
        self._unindex_item(self.table[object_index], object_index)
        self.table[object_index] = None
        self.pickle_table[object_index] = None
        self.unused_indexes.append(object_index)
        self.size -= 1
