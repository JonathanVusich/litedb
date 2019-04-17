from .index import Index
from ..errors import InvalidRange
from ..utils.object_utils import retrieve_possible_object_indexes
from ..utils.serialization_utils import load_table_index, dump_table_index
from typing import Set, Optional


class IndexManager:

    def __init__(self, index_path: str) -> None:
        self.index_path = index_path
        self.index_blacklist: Set[str] = set()
        self.index_map = {}
        self.load()

    def load(self) -> None:
        index_info = load_table_index(self.index_path)
        if index_info is not None:
            self.index_map = index_info["map"]
            self.index_blacklist = index_info["blacklist"]

    def persist(self) -> None:
        index_info = {}
        index_info.update({"map": self.index_map})
        index_info.update({"blacklist": self.index_blacklist})
        dump_table_index(self.index_path, index_info)

    def index_item(self, item: object, index: int) -> None:
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

    def unindex_item(self, item: object, index: int) -> None:
        """Removes indexes for the given object."""
        indexes = retrieve_possible_object_indexes(item)
        for var_name, value in indexes.items():
            self.index_map[var_name].destroy(value, index)

    def retrieve(self, **kwargs) -> Optional[Set[int]]:
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
