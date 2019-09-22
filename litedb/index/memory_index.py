from typing import Optional, Set

from litedb.abc import IndexManager
from .index import Index
from ..errors import InvalidRange
from ..utils.index import retrieve_possible_object_indexes

NoneType = type(None)


class MemoryIndex(IndexManager):
    """This is a index manager that handles indexes for all of the different types
    in the database."""

    def __init__(self):
        self.index_map = {}
        self.index_blacklist = set()

    def index_item(self, item: object, index: int) -> None:
        """Inserts/creates index tables based on the given object."""
        indexes = retrieve_possible_object_indexes(item)
        for var_name, value in indexes.items():
            if var_name in self.index_blacklist:
                continue
            if var_name not in self.index_map:
                # if the first item value is None, create the index without assigning type
                value_type = type(value)
                if value_type is NoneType:
                    self.index_map.update({var_name: Index()})
                else:
                    self.index_map.update({var_name: Index(type(value))})
            try:
                self.index_map[var_name].add(value, index)
            except TypeError:
                self.index_map.pop(var_name)
                self.index_blacklist.add(var_name)

    def unindex_item(self, item: object, index: int) -> None:
        """Removes indexes for the given object."""
        indexes = retrieve_possible_object_indexes(item)
        for var_name, value in indexes.items():
            if var_name not in self.index_blacklist:
                self.index_map[var_name].destroy(value, index)

    def retrieve(self, **kwargs) -> Optional[Set[int]]:
        """Retrieves indexes that match the given parameters."""
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
                    results = index.retrieve_range(low, high)
                    if results is not None:
                        indexes.update(results)
                else:
                    results = index.retrieve_range(low, high)
                    if results is not None:
                        indexes.intersection_update(results)
            else:
                if value is not None and not isinstance(value, index.index_type):
                    raise ValueError(f"\"{key}\" must be of type {index.index_type}")
                results = index.retrieve(value)
                if results is not None:
                    if x == 0:
                        indexes.update(results)
                    else:
                        indexes.intersection_update(results)
        if len(indexes) > 0:
            return indexes
