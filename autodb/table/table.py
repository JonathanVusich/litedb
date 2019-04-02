from typing import List, Set, Dict, Tuple
from ..index import Index
from ..utils.object_utils import retrieve_object_indexes


class Table:
    """
    This class stores elements of a certain class type within a regular Python list.
    Object attributes are indexed and stored within indexes that can then be used to retrieve
    objects from the list.
    """

    def __init__(self, first_object: object) -> None:
        self.size = 0
        self.table_type = type(first_object)
        self.table: List[object] = []
        self.unused_indexes: Set[int] = {}
        self.index_map: Dict[str, Index] = {}
        self.insert(first_object)

    def __repr__(self):
        return f"Table(table_type={self.table_type})"

    def __len__(self):
        return self.size

    def insert(self, item: object) -> None:
        if not isinstance(item, self.table_type):
            raise ValueError(f"Cannot insert item of type {type(item)} into table of type {self.table_type}!")
        if len(self.unused_indexes) > 0:
            index = self.unused_indexes.pop()
            self.table[index] = item
        else:
            index = len(self.table)
            self.table.append(item)
        self.size += 1
        self._insert_indexes(item, index)

    def _insert_indexes(self, item: object, index: int) -> None:
        """Inserts/creates index tables based on the given object."""
        indexes = retrieve_object_indexes(item)
        for var_name, value in indexes.items():
            if var_name not in self.index_map:
                self.index_map.update({var_name: Index(type(value))})
            self.index_map[var_name].add(value, index)

    def _remove_indexes(self, item: object) -> None:
        """Removes indexes for the given object."""
        indexes = retrieve_object_indexes(item)
        for var_name, value in indexes.items():
            self.index_map[var_name].destroy()

    def _retrieve(self, **kwargs) -> Tuple[object]:
        pass

    def _delete(self, object_index: int) -> None:
        self._remove_indexes(self.table[object_index])
        self.table[object_index] = None
        self.unused_indexes.add(object_index)
        self.size -= 1
