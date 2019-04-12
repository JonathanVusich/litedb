from .index import Index
from ..utils.object_utils import retrieve_possible_object_indexes
from ..utils.serialization_utils import load_table_index, dump_table_index
from typing import Set


class IndexManager:

    def __init__(self, index_path: str) -> None:
        self.index_path = index_path
        self.index_blacklist: Set[str] = set()
        self.index_attributes = {}
        self.index_map = {}
        self.load()

    def load(self):
        index_info = load_table_index(self.index_path)
        if index_info is not None:
            self.index_map = index_info["map"]
            self.index_blacklist = index_info["blacklist"]

    def persist(self):
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
                self._add_index_to_attr_map(var_name, index)
            except TypeError:
                self.index_map.pop(var_name)
                self.index_blacklist.add(var_name)

    def _add_index_to_attr_map(self, attribute: object, index: int) -> None:
        try:
            self.index_attributes[attribute].add(index)
        except KeyError:
            self.index_attributes[attribute] = {index}

    def unindex_item(self, index: int) -> None:
        """Removes indexes for the given object."""
        for var_name, index_set in self.index_attributes.items():
            index_set.remove(index)
            self.index_map[var_name].destroy(var_name, index)
