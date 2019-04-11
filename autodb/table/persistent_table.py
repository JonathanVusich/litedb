import pickle
from typing import List, Dict, Set, Optional, Generator, Tuple
from collections import deque
from sortedcontainers import SortedList

from .table import Table
from ..errors import InvalidRange, PathError
from ..index import Index
from ..utils.io_utils import create_info_path, create_index_path, dir_empty
from ..utils.object_utils import retrieve_possible_object_indexes
from ..utils.serialization_utils import load_table_index, load_table_info
from ..shard import ShardManager


class PersistentTable(Table):
    """
    This class stores elements of a certain class type within a regular Python list.
    Object attributes are indexed and stored within indexes that can then be used to retrieve
    objects from the list.
    """

    def __init__(self, directory: str = None, path_info: Tuple[str, str, str, Dict[int, str]] = None,
                 table_type=None) -> None:
        """
        This class can be instantiated as either a fresh new table or as an existing one from a file structure.
        To create a new table, an empty table directory and table type must be specified. Otherwise path info for
        the existing table must be supplied.
        :param directory:
        :param path_info:
        :param table_type:
        """
        if path_info is None and directory is not None and table_type is not None:
            if not dir_empty(directory):
                raise PathError("Tables should always be instantiated to an empty directory!")
            self.directory = directory
            self.index_path = create_index_path(self.directory)
            self.info_path = create_info_path(self.directory)
            shard_paths: Dict[int: str] = {}
            self.shard_manager = ShardManager(self.directory, shard_paths)
            self.table_type = None
            self.size = 0
            self.unused_indexes: SortedList[int] = SortedList()
            self.index_blacklist: Set[str] = set()
            self.index_map: Dict[str, Index] = {}
        elif path_info is not None and directory is None and table_type is None:
            self.directory = path_info[0]
            self.index_path = path_info[1]
            self.info_path = path_info[2]
            shard_paths = path_info[3]
            self.shard_manager = ShardManager(self.directory, shard_paths)
            table_info = load_table_info(path_info[2])
            self.table_type = table_info["table_type"]
            self.size = table_info["size"]
            self.unused_indexes: List[int] = table_info["unused_indexes"]
            self.index_blacklist: Set[str] = table_info["index_blacklist"]
            self.index_map: Dict[str, Index] = load_table_index(self.index_path)
        else:
            raise AttributeError
        self.shard_priority = deque()

    def __repr__(self):
        return f"Table(size={self.size})"

    def __len__(self):
        return self.size

    @classmethod
    def from_file(cls, path_info: Tuple[str, str, str, Dict[int, str]]):
        return cls(path_info=path_info)

    @classmethod
    def new(cls, directory: str, table_type):
        return cls(directory=directory, table_type=table_type)

    def serialize_table(self):
        table_info = {"table_type": self.table_type, "size": self.size, "unused_indexes": self.unused_indexes,
                      "index_blacklist": self.index_blacklist}
        pickle.dump(table_info, self.info_path)
        pickle.dump(self.index_path, self.index_map)

    def insert(self, item: object) -> None:
        if len(self.unused_indexes) > 0:
            index = self.unused_indexes.pop()
        else:
            index = self.size
            self.shard_manager.insert(item, index)
        self.size += 1
        self._index_item(item, index)

    def retrieve(self, **kwargs) -> [Generator[object, None, None]]:
        if len(kwargs) == 0:
            return self.shard_manager.retrieve_all()
        indexes = self._retrieve(**kwargs)
        if indexes:
            return self.shard_manager.retrieve(indexes)
        else:
            return ([])

    def delete(self, **kwargs):
        indexes_to_delete = self._retrieve(**kwargs)
        # Sort indexes for shards
        indexes_to_delete = sorted(list(indexes_to_delete))
        if indexes_to_delete:
            items_to_delete = self.shard_manager.retrieve(indexes_to_delete)
            for item, index in zip(items_to_delete, indexes_to_delete):
                self._unindex_item(item, index)
            self.unused_indexes.extend(indexes_to_delete)
            self.shard_manager.delete(indexes_to_delete)

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
        self._unindex_item(self.shard_manager.retrieve((object_index,)), object_index)
        self.shard_manager.delete((object_index,))
        self.unused_indexes.append(object_index)
        self.size -= 1


